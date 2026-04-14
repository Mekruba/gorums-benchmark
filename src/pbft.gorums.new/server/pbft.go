package server

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"

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
	// p.msgLog.delete(e.seq)

	p.pendingMu.Lock()
	ch, ok := p.pending[e.ts]
	if ok {
		delete(p.pending, e.ts)
	}
	p.pendingMu.Unlock()

	// if !ok {
	// 	return
	// }
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

// ── PBFT primary loop and RPC handlers (kept in server.go for locality) ───────

func (p *PBFTServer) runPrimary() {
	var sequence uint64
	for req := range p.reqQueue {
		sequence++
		seq := sequence

		p.msgLog.Update(seq, func(e *logEntry) {
			e.ts = req.ts
			e.sentPrepare = true
			e.prepareCount = 1
		})

		if err := pb.PrePrepare(req.cfgCtx, pb.PrePrepareMsg_builder{
			View:      p.view,
			Sequence:  seq,
			Timestamp: req.ts,
		}.Build()); err != nil {
			slog.Warn("PrePrepare send error", "node", p.id, "seq", seq, "err", err)
		}

		slog.Info("preprepare sent", "node", p.id, "seq", seq, "ts", req.ts)
	}
}

func (p *PBFTServer) ClientRequest(ctx gorums.ServerCtx, request *pb.Request) (*pb.Reply, error) {
	ts := request.GetTimestamp()
	slog.Debug("CLIENT-REQUEST", "node", p.id, "ts", ts)
	cfgCtx := ctx.ConfigContext()

	p.received.Add(1)

	replyCh := make(chan *pb.Reply, 1)
	p.pendingMu.Lock()
	p.pending[ts] = replyCh
	p.pendingMu.Unlock()

	if p.primary {
		select {
		case p.reqQueue <- queuedRequest{ts: ts, cfgCtx: cfgCtx}:
		case <-ctx.Done():
			p.cleanupPending(ts)
			return nil, context.Canceled
		}
	}

	ctx.Release()

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		select {
		case reply := <-replyCh:
			return reply, nil
		default:
		}
		p.cleanupPending(ts)
		if e := p.msgLog.FindByTs(ts); e != nil {
			slog.Warn("ctx.Done while waiting for reply",
				"node", p.id, "ts", ts, "seq", e.seq,
				"prepares", e.prepareCount, "commits", e.commitCount,
				"committed", e.committed, "executed", e.executed,
			)
		} else {
			slog.Warn("ctx.Done — no log entry found for timestamp", "node", p.id, "ts", ts)
		}
		return nil, context.Canceled
	}
}

func (p *PBFTServer) cleanupPending(ts int64) {
	p.pendingMu.Lock()
	delete(p.pending, ts)
	p.pendingMu.Unlock()
}

// Benchmark resets server state between benchmark steps.
func (p *PBFTServer) Benchmark(ctx gorums.ServerCtx, _ *emptypb.Empty) {
	ctx.Release()
	slog.Info("resetting state", "node", p.id)

	p.msgLog.Reset()

	p.pendingMu.Lock()
	p.pending = make(map[int64]chan<- *pb.Reply)
	p.pendingMu.Unlock()

	for len(p.reqQueue) > 0 {
		<-p.reqQueue
	}

	p.received.Store(0)
	p.committed.Store(0)
}

// PrePrepare is received by all nodes including the primary via loopback.
// The primary ignores it because sentPrepare=true is set in runPrimary.
func (p *PBFTServer) PrePrepare(ctx gorums.ServerCtx, request *pb.PrePrepareMsg) {
	seq := request.GetSequence()
	ts := request.GetTimestamp()
	slog.Debug("PRE-PREPARE", "node", p.id, "seq", seq)

	var alreadySent bool
	// Atomic check and update
	p.msgLog.Update(seq, func(e *logEntry) {
		if e.sentPrepare {
			alreadySent = true
			return
		}
		e.ts = ts
		e.sentPrepare = true
		e.prepareCount++
	})

	if alreadySent {
		return
	}

	cfgCtx := ctx.ConfigContext()
	if cfgCtx == nil {
		return
	}
	ctx.Release()

	if err := pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
		View:      request.GetView(),
		Sequence:  seq,
		ReplicaId: p.id,
	}.Build()); err != nil {
		slog.Warn("Prepare send error", "node", p.id, "seq", seq, "err", err)
	}
}

// Prepare is received by all nodes.
func (p *PBFTServer) Prepare(ctx gorums.ServerCtx, request *pb.PrepareMsg) {
	seq := request.GetSequence()
	sender := request.GetReplicaId()
	slog.Debug("PREPARE", "node", p.id, "seq", seq)
	slog.Info("prepare received", "node", p.id, "seq", seq, "from", sender)

	f := (p.clusterSize - 1) / 3
	var shouldCommit bool
	p.msgLog.Update(seq, func(e *logEntry) {
		e.prepareCount++
		if e.prepareCount >= 2*f && !e.sentCommit {
			e.sentCommit = true
			e.commitCount++
			shouldCommit = true
		}
	})

	if shouldCommit {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			return
		}
		ctx.Release()
		pb.Commit(cfgCtx, pb.CommitMsg_builder{
			View: request.GetView(), Sequence: seq, ReplicaId: p.id,
		}.Build())
	}
}

// Commit is received by all nodes.
func (p *PBFTServer) Commit(ctx gorums.ServerCtx, request *pb.CommitMsg) {
	seq := request.GetSequence()
	sender := request.GetReplicaId()
	slog.Info("commit received", "node", p.id, "seq", seq, "from", sender)
	f := (p.clusterSize - 1) / 3
	ctx.Release()

	var readyToDeliver *logEntry
	p.msgLog.Update(seq, func(e *logEntry) {
		e.commitCount++
		currentCommits := e.commitCount

		slog.Info("COMMIT-RECV", "node", p.id, "seq", seq, "from", sender, "total", currentCommits)
		if e.commitCount >= 2*f+1 && !e.executed && e.ts != 0 {
			e.committed = true
			readyToDeliver = e // Capture to deliver outside the lock
		}
	})

	if readyToDeliver != nil {
		slog.Info("THRESHOLD-REACHED", "node", p.id, "seq", seq, "ts", readyToDeliver.ts)
		p.deliver(readyToDeliver)
	}
}
