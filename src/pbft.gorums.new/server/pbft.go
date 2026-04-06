package server

import (
	"log/slog"
	"sync"
	"sync/atomic"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
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
