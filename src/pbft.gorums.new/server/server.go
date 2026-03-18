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

func InitLogger(verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	})))
}

// Server wraps a running gorums system and implements the BenchmarkServer
// interface expected by the bench framework (Start/Stop).
type Server struct {
	id    uint32
	nodes []NodeInfo
	sys   *gorums.System
}

// New creates a Server ready to be started.
func New(id uint32, addr string, peers map[int]string) *Server {
	nodes := make([]NodeInfo, 0, len(peers))
	for nodeID, p := range peers {
		nodes = append(nodes, NodeInfo{ID: uint32(nodeID), Addr: p})
	}
	return &Server{id: id, nodes: nodes}
}

// Start implements BenchmarkServer.
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
		gorums.WithReceiveBufferSize(128),
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

	_, err = sys.NewOutboundConfig(peerList,
		gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		log.Fatal(err)
	}

	slog.Info("ready", "node", s.id, "addr", addr)
	time.Sleep(2 * time.Second)
}

// Stop implements BenchmarkServer.
func (s *Server) Stop() {
	if s.sys != nil {
		s.sys.Stop()
	}
}

// StartServer starts a PBFT replica in-process and returns immediately.
func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	return s, nil
}

// RunServer starts a replica and blocks until SIGINT/SIGTERM.
func RunServer(id uint32, nodes []NodeInfo, verbose bool) {
	InitLogger(verbose)
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutting down", "node", id)
	s.Stop()
}

// ── message log ───────────────────────────────────────────────────────────────

// messageLog holds protocol state with its own mutex, independent of the
// server mutex. This allows log operations to not block pending map access.
type messageLog struct {
	mu      sync.Mutex
	entries map[uint64]*logEntry
}

func newMessageLog() *messageLog {
	return &messageLog{
		entries: make(map[uint64]*logEntry),
	}
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
	sequence    uint64
	primary     bool
	clusterSize int

	// pendingMu protects pending and sequence.
	pendingMu sync.Mutex
	pending   map[int64]chan *pb.Reply

	// msgLog has its own mutex — log operations don't block pending access.
	msgLog *messageLog

	reqQueue  chan queuedRequest
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

func (p *PBFTServer) runPrimary() {
	for req := range p.reqQueue {
		p.pendingMu.Lock()
		p.sequence++
		seq := p.sequence
		p.pendingMu.Unlock()

		p.msgLog.mu.Lock()
		e := p.msgLog.getOrCreate(seq)
		e.ts = req.ts
		p.msgLog.mu.Unlock()

		err := pb.PrePrepare(req.cfgCtx, pb.PrePrepareMsg_builder{
			View:      p.view,
			Sequence:  seq,
			Timestamp: req.ts,
		}.Build())

		if err != nil {
			slog.Warn("PrePrepare send error", "node", p.id, "seq", seq, "err", err)
		}
	}
}

func (p *PBFTServer) deliver(e *logEntry) {
	e.executed = true
	p.committed.Add(1)
	p.msgLog.delete(e.seq)

	p.pendingMu.Lock()
	ch, ok := p.pending[e.ts]
	if ok {
		delete(p.pending, e.ts)
	}
	p.pendingMu.Unlock()

	if !ok {
		return
	}
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
	ctx.Release()

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
				"node", p.id,
				"ts", ts,
				"queue_len", len(p.reqQueue),
				"queue_cap", cap(p.reqQueue),
				"cause", context.Cause(ctx),
			)
			return nil, context.Canceled
		}
	}

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		p.pendingMu.Lock()
		delete(p.pending, ts)
		p.pendingMu.Unlock()
		slog.Warn("ctx.Done while waiting for reply",
			"node", p.id,
			"ts", ts,
			"is_primary", p.primary,
			"queue_len", len(p.reqQueue),
			"pending_count", func() int {
				p.pendingMu.Lock()
				defer p.pendingMu.Unlock()
				return len(p.pending)
			}(),
			"cause", context.Cause(ctx),
		)
		return nil, context.Canceled
	}
}

func (p *PBFTServer) PrePrepare(ctx gorums.ServerCtx, request *pb.PrePrepareMsg) {
	seq := request.GetSequence()
	ts := request.GetTimestamp()
	slog.Debug("PRE-PREPARE", "node", p.id, "seq", seq)

	p.msgLog.mu.Lock()
	e := p.msgLog.getOrCreate(seq)
	if e.sentPrepare {
		p.msgLog.mu.Unlock()
		ctx.Release()
		return
	}
	e.ts = ts
	e.sentPrepare = true
	e.prepareCount++
	if e.committed && !e.executed {
		p.deliver(e)
	}
	p.msgLog.mu.Unlock()

	cfgCtx := ctx.ConfigContext()
	ctx.Release()
	if cfgCtx == nil {
		return
	}

	err := pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
		View:      request.GetView(),
		Sequence:  seq,
		ReplicaId: p.id,
	}.Build(), gorums.IgnoreErrors())

	if err != nil {
		slog.Warn("Prepare send error", "node", p.id, "seq", seq, "err", err)
	}
}

func (p *PBFTServer) Prepare(ctx gorums.ServerCtx, request *pb.PrepareMsg) {
	seq := request.GetSequence()
	slog.Debug("PREPARE", "node", p.id, "seq", seq)

	p.msgLog.mu.Lock()
	e := p.msgLog.getOrCreate(seq)
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
		ctx.Release()
		if cfgCtx == nil {
			return
		}
		err := pb.Commit(cfgCtx, pb.CommitMsg_builder{
			View:      request.GetView(),
			Sequence:  seq,
			ReplicaId: p.id,
		}.Build(), gorums.IgnoreErrors())

		if err != nil {
			slog.Warn("Commit send error", "node", p.id, "seq", seq, "err", err)
		}
	}
}

func (p *PBFTServer) Commit(ctx gorums.ServerCtx, request *pb.CommitMsg) {
	seq := request.GetSequence()
	slog.Debug("COMMIT", "node", p.id, "seq", seq)
	ctx.Release()

	p.msgLog.mu.Lock()
	defer p.msgLog.mu.Unlock()

	e := p.msgLog.getOrCreate(seq)
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

func (p *PBFTServer) Benchmark(ctx gorums.ServerCtx, _ *emptypb.Empty) {
	ctx.Release()
	slog.Info("resetting state", "node", p.id)

	p.msgLog.reset()

	p.pendingMu.Lock()
	// Don't close channels — just abandon them. Goroutines waiting on them
	// will eventually time out via their own ctx.
	p.pending = make(map[int64]chan *pb.Reply)
	p.sequence = 0
	p.pendingMu.Unlock()

	for len(p.reqQueue) > 0 {
		<-p.reqQueue
	}

	p.received.Store(0)
	p.committed.Store(0)
}
