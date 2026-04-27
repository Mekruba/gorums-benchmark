package server

import (
	// "bytes"
	"context"
	"crypto/ed25519"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// ── PBFT protocol ─────────────────────────────────────────────────────────────

type queuedRequest struct {
	ts int64
}

type PBFTServer struct {
	id          uint32
	view        uint32
	primary     bool
	clusterSize int

	mu sync.Mutex // protects view and primary

	pendingMu sync.Mutex
	pending   map[int64]chan<- *pb.Reply
	delivered map[int64]*pb.Reply

	deliverCh chan *logEntry // ordered delivery queue

	msgLog   *MessageLog
	reqQueue chan queuedRequest

	received  atomic.Uint64
	committed atomic.Uint64

	// needed by checkpoint.go and viewchange.go for timer-fired multicasts
	outboundCfg gorums.Configuration

	keys     NodeKeys
	peerKeys map[uint32]ed25519.PublicKey // nodeID → public key
}

func NewPBFTServer(id uint32, clusterSize int) *PBFTServer {
	p := &PBFTServer{
		id:          id,
		primary:     id == 1,
		clusterSize: clusterSize,
		pending:     make(map[int64]chan<- *pb.Reply),
		delivered:   make(map[int64]*pb.Reply),
		msgLog:      NewMessageLog(),
		reqQueue:    make(chan queuedRequest, 2000),
		deliverCh:   make(chan *logEntry, 2000),
	}
	if p.primary {
		go p.runPrimary()
	}
	go p.runDeliver()
	return p
}

// deliver enqueues the entry for ordered delivery.
// Called from Commit handler — may be concurrent.
func (p *PBFTServer) deliver(e *logEntry) {
	p.deliverCh <- e
}

// runDeliver drains the delivery channel in order, ensuring
// seq N is fully processed before seq N+1.
func (p *PBFTServer) runDeliver() {
	var nextSeq uint64 = 1
	pending := make(map[uint64]*logEntry)

	for e := range p.deliverCh {
		pending[e.seq] = e
		for {
			// Skip past any sequences that were GC'd
			lwm := p.msgLog.LowWaterMark()
			if nextSeq <= lwm {
				// These were GC'd — skip them, they were committed
				// by quorum on other nodes
				for seq := range pending {
					if seq <= lwm {
						delete(pending, seq)
					}
				}
				nextSeq = lwm + 1
			}

			entry, ok := pending[nextSeq]
			if !ok {
				break
			}
			delete(pending, nextSeq)
			p.executeEntry(entry)
			nextSeq++
		}
	}
}

// executeEntry runs the actual delivery logic for a single committed entry.
func (p *PBFTServer) executeEntry(e *logEntry) {
	e.executed = true
	p.committed.Add(1)

	p.pendingMu.Lock()
	ch, ok := p.pending[e.ts]
	if ok {
		delete(p.pending, e.ts)
	}
	p.pendingMu.Unlock()

	if !ok {
		p.pendingMu.Lock()
		p.delivered[e.ts] = pb.Reply_builder{
			View:      p.view,
			ReplicaId: p.id,
			Timestamp: e.ts,
			Result:    "ok",
		}.Build()
		p.pendingMu.Unlock()
	} else {
		slog.Info("committed", "node", p.id, "seq", e.seq,
			"total_committed", p.committed.Load(),
			"total_received", p.received.Load())
		ch <- pb.Reply_builder{
			View:      p.view,
			ReplicaId: p.id,
			Timestamp: e.ts,
			Result:    "ok",
		}.Build()
	}

	if p.msgLog.ShouldCheckpoint(e.seq) {
		p.sendCheckpoint(e.seq)
	}
}

// ── PBFT primary loop and RPC handlers  ───────

func (p *PBFTServer) runPrimary() {
	var sequence uint64

	for req := range p.reqQueue {
		sequence++
		seq := sequence

		if seq > p.msgLog.HighWaterMark() {
			slog.Debug("primary stalled at high water mark",
				"node", p.id, "seq", seq, "hwm", p.msgLog.HighWaterMark())
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			p.msgLog.WaitForWaterMark(ctx, seq)
			cancel()
		}

		p.tryRecordPrePrepare(seq, req.ts)

		// Build a fresh protocol context — independent of any client context.
		// Protocol phases must complete even if the originating client disconnects.
		cfg := p.outbound()
		cfgCtx := cfg.Context(context.Background())

		digest := requestDigest(req.ts)
		sigDigest := prePrepareDigest(p.view, seq, digest)
		sig := sign(getPrivKey(p.id), sigDigest)
		if err :=
			pb.PrePrepare(cfgCtx, pb.PrePrepareMsg_builder{
				View:      p.view,
				Sequence:  seq,
				Timestamp: req.ts,
				Digest:    digest,
				Signature: sig,
			}.Build(), gorums.IgnoreErrors()); err != nil {
			slog.Warn("PrePrepare send error", "node", p.id, "seq", seq, "err", err)
		}

		slog.Debug("preprepare sent", "node", p.id, "seq", seq, "ts", req.ts)
	}
}

func (p *PBFTServer) ClientRequest(ctx gorums.ServerCtx, request *pb.Request) (*pb.Reply, error) {
	ts := request.GetTimestamp()
	slog.Debug("CLIENT-REQUEST", "node", p.id, "ts", ts)

	p.received.Add(1)

	// Check if the protocol already completed before we got here.
	p.pendingMu.Lock()
	if reply, ok := p.delivered[ts]; ok {
		delete(p.delivered, ts)
		p.pendingMu.Unlock()
		return reply, nil
	}
	replyCh := make(chan *pb.Reply, 1)
	p.pending[ts] = replyCh
	p.pendingMu.Unlock()

	// Only the primary enqueues — no cfgCtx needed anymore.
	if p.primary {
		select {
		case p.reqQueue <- queuedRequest{ts: ts}:
		case <-ctx.Done():
			p.cleanupPending(ts)
			p.logTimeout(ts)
			return nil, context.Canceled
		}
	}

	ctx.Release()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		timer := time.NewTimer(1000 * time.Millisecond)
		defer timer.Stop()
		select {
		case reply := <-replyCh:
			return reply, nil
		case <-timer.C:
			p.cleanupPending(ts)
			p.logTimeout(ts)
			return nil, context.Canceled
		}
	}
}

func (p *PBFTServer) logTimeout(ts int64) {
	if e := p.msgLog.FindByTs(ts); e != nil {
		slog.Warn("ClientRequest timeout",
			"node", p.id, "ts", ts, "seq", e.seq,
			"prepares", len(e.prepares), "commits", len(e.commits),
			"committed", e.commited, "executed", e.executed,
		)
	} else {
		slog.Warn("ClientRequest timeout — no log entry", "node", p.id, "ts", ts)
	}
}

func (p *PBFTServer) cleanupPending(ts int64) {
	p.pendingMu.Lock()
	delete(p.pending, ts)
	p.pendingMu.Unlock()
}

func (p *PBFTServer) Ping(ctx gorums.ServerCtx, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (p *PBFTServer) Benchmark(ctx gorums.ServerCtx, _ *emptypb.Empty) {
	ctx.Release()
	slog.Info("resetting state", "node", p.id)

	// Wait for all in-flight requests to finish
	deadline := time.After(5 * time.Second)
	for {
		received := p.received.Load()
		committed := p.committed.Load()
		if committed >= received {
			break
		}
		select {
		case <-deadline:
			slog.Warn("benchmark reset: drain timeout",
				"node", p.id,
				"received", received,
				"committed", committed,
			)
			goto reset
		case <-time.After(50 * time.Millisecond):
		}
	}

reset:
	p.msgLog.Reset()

	p.pendingMu.Lock()
	p.pending = make(map[int64]chan<- *pb.Reply)
	p.delivered = make(map[int64]*pb.Reply)
	p.view = 0
	p.pendingMu.Unlock()

	for len(p.reqQueue) > 0 {
		<-p.reqQueue
	}
	for len(p.deliverCh) > 0 {
		<-p.deliverCh
	}

	p.received.Store(0)
	p.committed.Store(0)
}

// ── RPC Handlers ─────────────────────────────────────────────────────────────

// Backup Replicas receive PrePrepare from Primary
func (p *PBFTServer) PrePrepare(ctx gorums.ServerCtx, request *pb.PrePrepareMsg) {
	seq := request.GetSequence()
	primaryID := uint32(p.view%uint32(p.clusterSize)) + 1
	if !verifyMsg(primaryID, prePrepareDigest(request.GetView(), seq, request.GetDigest()), request.GetSignature()) {
		slog.Warn("PrePrepare signature invalid", "node", p.id, "seq", seq)
		return
	}
	if !p.msgLog.WithinWaterMarks(seq) {
		slog.Warn("PrePrepare outside water marks", "node", p.id, "seq", seq)
		return
	}

	slog.Debug("PRE-PREPARE", "node", p.id, "seq", seq)

	if p.tryRecordPrePrepare(seq, request.GetTimestamp()) {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			return
		}
		sig := sign(getPrivKey(p.id), prepareDigest(request.GetView(), seq, p.id, request.GetDigest()))

		ctx.Release()
		slog.Debug("prepare sent", "node", p.id, "seq", seq)
		pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
			View: request.GetView(), Sequence: seq, ReplicaId: p.id,
			Digest:    request.GetDigest(),
			Signature: sig,
		}.Build(), gorums.IgnoreErrors())

		// prepares may have arrived before PrePrepare
		if p.recheckPrepared(seq) {
			sig = sign(getPrivKey(p.id), commitDigest(request.GetView(), seq, p.id, request.GetDigest()))
			pb.Commit(cfgCtx, pb.CommitMsg_builder{
				View: request.GetView(), Sequence: seq, ReplicaId: p.id,
				Digest:    request.GetDigest(),
				Signature: sig,
			}.Build(), gorums.IgnoreErrors())

			if p.recheckCommitted(seq) {
				p.deliver(p.msgLog.GetOrCreate(seq))
			}
		}
	}
}

func (p *PBFTServer) Prepare(ctx gorums.ServerCtx, request *pb.PrepareMsg) {
	seq := request.GetSequence()
	if !verifyMsg(request.GetReplicaId(), prepareDigest(request.GetView(), seq, request.GetReplicaId(), request.GetDigest()), request.GetSignature()) {
		slog.Warn("Prepare signature invalid", "node", p.id, "seq", seq, "from", request.GetReplicaId())
		return
	}
	if !p.msgLog.WithinWaterMarks(seq) {
		slog.Warn("Prepare outside water marks", "node", p.id, "seq", seq)
		return
	}
	slog.Debug("prepare received", "node", p.id, "seq", seq, "from", request.GetReplicaId())

	if p.tryPrepare(seq, request.GetView(), request.GetReplicaId(), request.GetDigest()) {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			return
		}
		sig := sign(getPrivKey(p.id), commitDigest(request.GetView(), seq, p.id, request.GetDigest()))
		ctx.Release()

		slog.Debug("commit sent", "node", p.id, "seq", seq)
		pb.Commit(cfgCtx, pb.CommitMsg_builder{
			View:      request.GetView(),
			Sequence:  seq,
			ReplicaId: p.id,
			Digest:    request.GetDigest(),
			Signature: sig,
		}.Build(), gorums.IgnoreErrors())
	}
}

func (p *PBFTServer) Commit(ctx gorums.ServerCtx, request *pb.CommitMsg) {
	seq := request.GetSequence()
	if !verifyMsg(request.GetReplicaId(), commitDigest(request.GetView(), seq, request.GetReplicaId(), request.GetDigest()), request.GetSignature()) {
		slog.Warn("Commit signature invalid", "node", p.id, "seq", seq, "from", request.GetReplicaId())
		return
	}
	if !p.msgLog.WithinWaterMarks(seq) {
		slog.Warn("Commit outside water marks", "node", p.id, "seq", seq)
		return
	}
	slog.Debug("commit received", "node", p.id, "seq", seq, "from", request.GetReplicaId())

	if p.tryCommit(seq, request.GetView(), request.GetReplicaId(), request.GetDigest()) {
		p.deliver(p.msgLog.GetOrCreate(seq))
	}
}

// ── PBFT Server Helpers ───────────────────────────────────────────────────────

// tryRecordPrePrepare returns true if this is the first time we see a PrePrepare for this seq.
func (p *PBFTServer) tryRecordPrePrepare(seq uint64, ts int64) bool {
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.sentPrepare {
		return false
	}
	entry.ts = ts
	entry.sentPrepare = true
	digest := requestDigest(ts)
	entry.prePrepare = &prePrepareRecord{
		view:     p.view,
		sequence: seq,
		digest:   digest,
	}
	// self-count own prepare
	if entry.prepares == nil {
		entry.prepares = make(map[uint32]*prepareRecord)
	}
	entry.prepares[p.id] = &prepareRecord{
		view: p.view, sequence: seq, digest: digest, replicaID: p.id,
	}
	return true
}

func (p *PBFTServer) tryPrepare(seq uint64, view uint32, replicaID uint32, digest []byte) bool {
	f := (p.clusterSize - 1) / 3
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.prepares == nil {
		entry.prepares = make(map[uint32]*prepareRecord)
	}
	// deduplicate by sender
	if _, exists := entry.prepares[replicaID]; exists {
		return false
	}
	entry.prepares[replicaID] = &prepareRecord{
		view: view, sequence: seq, digest: digest, replicaID: replicaID,
	}
	return entry.checkPrepared(f)
}

func (p *PBFTServer) tryCommit(seq uint64, view uint32, replicaID uint32, digest []byte) bool {
	f := (p.clusterSize - 1) / 3
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.commits == nil {
		entry.commits = make(map[uint32]*commitRecord)
	}
	if _, exists := entry.commits[replicaID]; exists {
		return false
	}
	entry.commits[replicaID] = &commitRecord{
		view: view, sequence: seq, digest: digest, replicaID: replicaID,
	}
	return entry.checkCommitted(f)
}

// recheckPrepared checks if accumulated prepares now satisfy threshold.
// Called after prePrepare is set to handle out-of-order message delivery.
func (p *PBFTServer) recheckPrepared(seq uint64) bool {
	f := (p.clusterSize - 1) / 3
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.checkPrepared(f)
}

func (p *PBFTServer) recheckCommitted(seq uint64) bool {
	f := (p.clusterSize - 1) / 3
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.checkCommitted(f)
}

func (p *PBFTServer) SetOutboundConfig(cfg gorums.Configuration) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.outboundCfg = cfg
}

func (p *PBFTServer) outbound() gorums.Configuration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.outboundCfg
}

func (p *PBFTServer) f() int {
	return (p.clusterSize - 1) / 3
}
