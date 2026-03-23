package server

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// NodeInfo describes a cluster member.
type NodeInfo struct {
	ID   uint32
	Addr string
}

// NodeAddr implements gorums.NodeAddr.
type NodeAddr struct{ Addr_ string }

func (n NodeAddr) Addr() string { return n.Addr_ }

func InitLogger(id uint32, verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

type Server struct {
	id    uint32
	nodes []NodeInfo
	sys   *gorums.System
}

func New(id uint32, addr string, peers map[int]string) *Server {
	nodes := make([]NodeInfo, 0, len(peers))
	for nodeID, p := range peers {
		nodes = append(nodes, NodeInfo{ID: uint32(nodeID), Addr: p})
	}
	return &Server{id: id, nodes: nodes}
}

func (s *Server) Start(_ bool) {
	peerMap := make(map[uint32]NodeAddr)
	for _, n := range s.nodes {
		peerMap[n.ID] = NodeAddr{Addr_: n.Addr}
	}
	peerList := gorums.WithNodes(peerMap)

	var addr string
	for _, n := range s.nodes {
		if n.ID == s.id {
			addr = n.Addr
			break
		}
	}

	sys, err := gorums.NewSystem(addr,
		gorums.WithConfig(s.id, peerList),
		gorums.WithReceiveBufferSize(1024),
		gorums.WithSendBufferSize(512),
		peerList,
		gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		log.Fatal(err)
	}
	s.sys = sys

	mgr := pb.NewManager(gorums.WithDialOptions(
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	))

	pbft := NewPBFTServer(s.id, len(s.nodes))
	sys.RegisterService(mgr, func(gs *gorums.Server) {
		pb.RegisterPBFTServer(gs, pbft)
	})

	go func() {
		if err := sys.Serve(); err != nil {
			log.Println("serve error:", err)
		}
	}()

	slog.Info("ready", "node", s.id, "addr", addr)
	time.Sleep(2 * time.Second)
}

func (s *Server) Stop() {
	if s.sys != nil {
		s.sys.Stop()
	}
}

func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	return s, nil
}

func RunServer(id uint32, nodes []NodeInfo, verbose bool) {
	InitLogger(id, verbose)
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutting down", "node", id)
	s.Stop()
}

// ── message log ───────────────────────────────────────────────────────────────

type messageLog struct {
	mu      sync.Mutex
	entries map[uint64]*logEntry
}

func newMessageLog() *messageLog {
	return &messageLog{entries: make(map[uint64]*logEntry)}
}

func (ml *messageLog) getOrCreate(seq uint64) *logEntry {
	if e, ok := ml.entries[seq]; ok {
		return e
	}
	e := &logEntry{seq: seq}
	ml.entries[seq] = e
	return e
}

func (ml *messageLog) delete(seq uint64) {
	delete(ml.entries, seq)
}

func (ml *messageLog) reset() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.entries = make(map[uint64]*logEntry)
}

func (ml *messageLog) findByTs(ts int64) *logEntry {
	for _, e := range ml.entries {
		if e.ts == ts {
			return e
		}
	}
	return nil
}

// ── PBFT protocol ─────────────────────────────────────────────────────────────

type logEntry struct {
	seq          uint64
	ts           int64
	prepareCount int
	commitCount  int
	sentPrepare  bool
	sentCommit   bool
	committed    bool
	executed     bool
}

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
	pending   map[int64]chan *pb.Reply

	msgLog *messageLog

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
		pending:     make(map[int64]chan *pb.Reply),
		msgLog:      newMessageLog(),
		reqQueue:    make(chan queuedRequest, 2000),
	}
	if p.primary {
		go p.runPrimary()
	}
	return p
}

// runPrimary processes requests serially from reqQueue. A single goroutine
// assigns sequence numbers in order — no mutex needed for sequencing.
// Setting sentPrepare=true and incrementing prepareCount here means the
// primary's own looped-back PrePrepare multicast is ignored by the handler.
func (p *PBFTServer) runPrimary() {
	var sequence uint64
	for req := range p.reqQueue {
		sequence++
		seq := sequence

		p.msgLog.mu.Lock()
		e := p.msgLog.getOrCreate(seq)
		e.ts = req.ts
		p.msgLog.mu.Unlock()

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
			p.pendingMu.Lock()
			delete(p.pending, ts)
			p.pendingMu.Unlock()
			slog.Warn("ctx.Done while enqueuing",
				"node", p.id, "ts", ts,
				"queue_len", len(p.reqQueue),
				"cause", context.Cause(ctx),
			)
			return nil, context.Canceled
		}
	}

	ctx.Release()
	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		// deliver may have sent to replyCh at the same instant ctx was cancelled.
		// Drain the buffered channel before giving up.
		select {
		case reply := <-replyCh:
			return reply, nil
		default:
		}
		p.pendingMu.Lock()
		delete(p.pending, ts)
		p.pendingMu.Unlock()
		p.msgLog.mu.Lock()
		e := p.msgLog.findByTs(ts)
		if e != nil {
			slog.Warn("ctx.Done while waiting for reply",
				"node", p.id, "ts", ts,
				"is_primary", p.primary,
				"seq", e.seq,
				"prepareCount", e.prepareCount,
				"commitCount", e.commitCount,
				"sentPrepare", e.sentPrepare,
				"sentCommit", e.sentCommit,
				"committed", e.committed,
				"executed", e.executed,
				"cause", context.Cause(ctx),
			)
		} else {
			slog.Warn("ctx.Done while waiting for reply — no log entry",
				"node", p.id, "ts", ts,
				"is_primary", p.primary,
				"cause", context.Cause(ctx),
			)
		}
		p.msgLog.mu.Unlock()
		return nil, context.Canceled
	}
}

// PrePrepare is received by all nodes including the primary via loopback.
// The primary ignores it because sentPrepare=true is set in runPrimary.
func (p *PBFTServer) PrePrepare(ctx gorums.ServerCtx, request *pb.PrePrepareMsg) {
	seq := request.GetSequence()
	ts := request.GetTimestamp()
	slog.Debug("PRE-PREPARE", "node", p.id, "seq", seq)

	p.msgLog.mu.Lock()
	e := p.msgLog.getOrCreate(seq)
	if e.sentPrepare {
		p.msgLog.mu.Unlock()
		return
	}
	e.ts = ts
	e.sentPrepare = true
	e.prepareCount++
	// if e.committed && !e.executed {
	// 	p.deliver(e)
	// }
	p.msgLog.mu.Unlock()
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
	slog.Debug("PREPARE", "node", p.id, "seq", seq)

	p.msgLog.mu.Lock()
	e := p.msgLog.getOrCreate(seq)
	slog.Info("prepare received", "node", p.id, "seq", seq, "from", request.GetReplicaId())

	e.prepareCount++
	f := (p.clusterSize - 1) / 3
	shouldCommit := e.prepareCount >= 2*f && !e.sentCommit
	if shouldCommit {
		e.sentCommit = true
		e.commitCount++
	}
	p.msgLog.mu.Unlock()

	if shouldCommit {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			slog.Error("No config found when sending Commit", "node", p.id, "seq", e.seq)
			return
		}
		ctx.Release()
		if err := pb.Commit(cfgCtx, pb.CommitMsg_builder{
			View:      request.GetView(),
			Sequence:  seq,
			ReplicaId: p.id,
		}.Build()); err != nil {
			slog.Warn("Commit send error", "node", p.id, "seq", seq, "err", err)
		}
	}
}

// Commit is received by all nodes.
func (p *PBFTServer) Commit(ctx gorums.ServerCtx, request *pb.CommitMsg) {
	seq := request.GetSequence()
	slog.Debug("COMMIT", "node", p.id, "seq", seq)
	ctx.Release()

	p.msgLog.mu.Lock()
	defer p.msgLog.mu.Unlock()

	e := p.msgLog.getOrCreate(seq)
	slog.Info("commit received", "node", p.id, "seq", seq, "from", request.GetReplicaId(), "commitCount", e.commitCount)
	e.commitCount++
	f := (p.clusterSize - 1) / 3
	if e.commitCount < 2*f+1 || e.executed {
		return
	}
	e.committed = true
	if e.ts != 0 {
		p.deliver(e)
	}
}

// Benchmark resets server state between benchmark steps.
func (p *PBFTServer) Benchmark(ctx gorums.ServerCtx, _ *emptypb.Empty) {
	ctx.Release()
	slog.Info("resetting state", "node", p.id)

	p.msgLog.reset()

	p.pendingMu.Lock()
	p.pending = make(map[int64]chan *pb.Reply)
	p.pendingMu.Unlock()

	for len(p.reqQueue) > 0 {
		<-p.reqQueue
	}

	p.received.Store(0)
	p.committed.Store(0)
}
