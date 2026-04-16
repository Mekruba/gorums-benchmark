package server

import (
	"context"
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
	ts     int64
	cfgCtx *gorums.ConfigContext
}

type PBFTServer struct {
	id          uint32
	view        uint32
	primary     bool
	clusterSize int

	pendingMu sync.Mutex
	pending   map[int64]chan<- *pb.Reply
	delivered map[int64]*pb.Reply // replies that arrived before ClientRequest registered

	msgLog *MessageLog

	// reqQueue serializes sequencing — one goroutine assigns sequence numbers
	// and sends PrePrepare, preventing any latency spikes from lock contention.
	reqQueue chan queuedRequest

	received  atomic.Uint64
	committed atomic.Uint64
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
	}
	if p.primary {
		go p.runPrimary()
	}
	return p
}

func (p *PBFTServer) deliver(e *logEntry) {
	e.executed = true
	p.committed.Add(1)

	// The ClientRequest handler may not have registered its replyCh yet
	// if the protocol completed before the handler ran on this node.
	// Poll briefly to give it time to register.
	var ch chan<- *pb.Reply
	var ok bool
	for range 50 {
		p.pendingMu.Lock()
		ch, ok = p.pending[e.ts]
		if ok {
			delete(p.pending, e.ts)
		}
		p.pendingMu.Unlock()
		if ok {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	if !ok {
		// Store the reply so ClientRequest can pick it up when it registers
		p.pendingMu.Lock()
		p.delivered[e.ts] = pb.Reply_builder{
			View:      p.view,
			ReplicaId: p.id,
			Timestamp: e.ts,
			Result:    "ok",
		}.Build()
		p.pendingMu.Unlock()
		return
	}

	ch <- pb.Reply_builder{
		View:      p.view,
		ReplicaId: p.id,
		Timestamp: e.ts,
		Result:    "ok",
	}.Build()
}

// ── PBFT primary loop and RPC handlers  ───────

func (p *PBFTServer) runPrimary() {
	var sequence uint64
	for req := range p.reqQueue {
		sequence++
		seq := sequence

		entry := p.msgLog.GetOrCreate(seq)
		entry.mu.Lock()
		entry.ts = req.ts
		entry.sentPrepare = true
		entry.prepareCount = 1
		entry.mu.Unlock()

		if err := pb.PrePrepare(req.cfgCtx, pb.PrePrepareMsg_builder{
			View:      p.view,
			Sequence:  sequence,
			Timestamp: req.ts,
		}.Build(), gorums.IgnoreErrors()); err != nil {
			slog.Warn("PrePrepare send error", "node", p.id, "seq", sequence, "err", err)
		}
		slog.Debug("preprepare sent", "node", p.id, "seq", sequence, "ts", req.ts)
	}
}

func (p *PBFTServer) ClientRequest(ctx gorums.ServerCtx, request *pb.Request) (*pb.Reply, error) {
	ts := request.GetTimestamp()
    slog.Debug("CLIENT-REQUEST", "node", p.id, "ts", ts)

	p.received.Add(1)

	// Check if the protocol already completed before we got here
	p.pendingMu.Lock()
	if reply, ok := p.delivered[ts]; ok {
		delete(p.delivered, ts)
		p.pendingMu.Unlock()
		return reply, nil
	}

	replyCh := make(chan *pb.Reply, 1)
	p.pending[ts] = replyCh
	p.pendingMu.Unlock()

	if p.primary {
		cfgCtx := ctx.ConfigContext()
		select {
		case p.reqQueue <- queuedRequest{ts: ts, cfgCtx: cfgCtx}:
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
		timer := time.NewTimer(200 * time.Millisecond)
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
			"prepares", e.prepareCount, "commits", e.commitCount,
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
	p.pendingMu.Unlock()

	for len(p.reqQueue) > 0 {
		<-p.reqQueue
	}

	p.received.Store(0)
	p.committed.Store(0)
}

// ── RPC Handlers ─────────────────────────────────────────────────────────────

func (p *PBFTServer) PrePrepare(ctx gorums.ServerCtx, request *pb.PrePrepareMsg) {
	seq := request.GetSequence()
	slog.Debug("PRE-PREPARE", "node", p.id, "seq", seq)

	if p.tryRecordPrePrepare(seq, request.GetTimestamp()) {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			return
		}
		ctx.Release()

		slog.Debug("prepare sent", "node", p.id, "seq", seq)
		pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
			View: request.GetView(), Sequence: seq, ReplicaId: p.id,
		}.Build(), gorums.IgnoreErrors())
	}
}

func (p *PBFTServer) Prepare(ctx gorums.ServerCtx, request *pb.PrepareMsg) {
	seq := request.GetSequence()
	slog.Debug("prepare received", "node", p.id, "seq", seq, "from", request.GetReplicaId())

	if p.tryPrepare(seq) {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			return
		}
		ctx.Release()

		slog.Debug("commit sent", "node", p.id, "seq", seq)
		pb.Commit(cfgCtx, pb.CommitMsg_builder{
			View: request.GetView(), Sequence: seq, ReplicaId: p.id,
		}.Build(), gorums.IgnoreErrors())
	}
}

func (p *PBFTServer) Commit(ctx gorums.ServerCtx, request *pb.CommitMsg) {
	seq := request.GetSequence()
	slog.Debug("commit received", "node", p.id, "seq", seq, "from", request.GetReplicaId())

	if p.tryCommit(seq) {
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
	entry.prepareCount++
	return true
}

func (p *PBFTServer) tryPrepare(seq uint64) bool {
	f := (p.clusterSize - 1) / 3
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.prepareCount++
	return entry.checkPrepared(f)
}

func (p *PBFTServer) tryCommit(seq uint64) bool {
	f := (p.clusterSize - 1) / 3
	entry := p.msgLog.GetOrCreate(seq)
	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.commitCount++
	return entry.checkCommitted(f)
}
