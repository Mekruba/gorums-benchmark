package server

import (
	"context"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// New creates a Server ready to be started. The peers slice uses index as ID,
// matching the bench framework convention of srvAddresses[id].
func New(id uint32, addr string, peers map[int]string) *Server {
	nodes := make([]NodeInfo, 0, len(peers))
	for nodeID, p := range peers {
		nodes = append(nodes, NodeInfo{ID: uint32(nodeID), Addr: p})
	}
	return &Server{id: id, nodes: nodes}
}

// Start implements BenchmarkServer. local is ignored — always starts in-process.
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
		gorums.WithReceiveBufferSize(64),
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

// Stop implements BenchmarkServer.
func (s *Server) Stop() {
	if s.sys != nil {
		s.sys.Stop()
	}
}

// StartServer starts a PBFT replica in-process and returns immediately.
// Used by the bench framework's CreateServer.
func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	return s, nil
}

// RunServer starts a replica and blocks until SIGINT/SIGTERM.
// Used by the CLI binary.
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

type PBFTServer struct {
	id          uint32
	view        uint32
	sequence    uint64
	primary     bool
	clusterSize int
	mu          sync.Mutex

	log     map[uint64]*logEntry
	pending map[int64]chan *pb.Reply
}

func NewPBFTServer(id uint32, clusterSize int) *PBFTServer {
	return &PBFTServer{
		id:          id,
		primary:     id == 1,
		clusterSize: clusterSize,
		log:         make(map[uint64]*logEntry),
		pending:     make(map[int64]chan *pb.Reply),
	}
}

func (p *PBFTServer) getOrCreate(seq uint64) *logEntry {
	if e, ok := p.log[seq]; ok {
		return e
	}
	e := &logEntry{seq: seq}
	p.log[seq] = e
	return e
}

func (p *PBFTServer) deliver(e *logEntry) {
	ch, ok := p.pending[e.ts]
	if !ok {
		return
	}
	e.executed = true
	delete(p.pending, e.ts)
	delete(p.log, e.seq)
	slog.Info("committed", "node", p.id, "seq", e.seq)
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
	ctx.Release()
	cfgCtx := ctx.ConfigContext()

	replyCh := make(chan *pb.Reply, 1)

	p.mu.Lock()
	p.pending[ts] = replyCh
	p.mu.Unlock()

	var seq uint64
	if p.primary {
		p.mu.Lock()
		p.sequence++
		seq = p.sequence
		e := p.getOrCreate(seq)
		e.ts = ts
		p.mu.Unlock()

		slog.Debug("PRIMARY: sending PrePrepare", "node", p.id, "seq", seq)
		_ = pb.PrePrepare(cfgCtx, pb.PrePrepareMsg_builder{
			View:      p.view,
			Sequence:  seq,
			Timestamp: ts,
		}.Build(), gorums.IgnoreErrors())
	}

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-ctx.Done():
		p.mu.Lock()
		delete(p.pending, ts)
		if p.primary && seq != 0 {
			if e, ok := p.log[seq]; ok && !e.committed {
				delete(p.log, seq)
			}
		}
		p.mu.Unlock()
		return nil, context.Canceled
	}
}

func (p *PBFTServer) PrePrepare(ctx gorums.ServerCtx, request *pb.PrePrepareMsg) {
	seq := request.GetSequence()
	ts := request.GetTimestamp()
	slog.Debug("PRE-PREPARE", "node", p.id, "seq", seq)

	p.mu.Lock()
	e := p.getOrCreate(seq)
	if e.sentPrepare {
		p.mu.Unlock()
		return
	}
	e.ts = ts
	e.sentPrepare = true
	e.prepareCount++
	if e.committed && !e.executed {
		p.deliver(e)
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	cfgCtx := ctx.ConfigContext()
	ctx.Release()
	if cfgCtx == nil {
		return
	}
	_ = pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
		View:      request.GetView(),
		Sequence:  seq,
		ReplicaId: p.id,
	}.Build(), gorums.IgnoreErrors())
}

func (p *PBFTServer) Prepare(ctx gorums.ServerCtx, request *pb.PrepareMsg) {
	seq := request.GetSequence()
	slog.Debug("PREPARE", "node", p.id, "seq", seq)

	p.mu.Lock()
	e := p.getOrCreate(seq)
	e.prepareCount++
	f := (p.clusterSize - 1) / 3
	shouldCommit := e.prepareCount >= 2*f && !e.sentCommit
	if shouldCommit {
		e.sentCommit = true
		e.commitCount++
	}
	p.mu.Unlock()

	if shouldCommit {
		cfgCtx := ctx.ConfigContext()
		ctx.Release()
		if cfgCtx == nil {
			return
		}
		_ = pb.Commit(cfgCtx, pb.CommitMsg_builder{
			View:      request.GetView(),
			Sequence:  seq,
			ReplicaId: p.id,
		}.Build(), gorums.IgnoreErrors())
	}
}

func (p *PBFTServer) Commit(ctx gorums.ServerCtx, request *pb.CommitMsg) {
	seq := request.GetSequence()
	slog.Debug("COMMIT", "node", p.id, "seq", seq)

	p.mu.Lock()
	defer p.mu.Unlock()

	e := p.getOrCreate(seq)
	e.commitCount++
	f := (p.clusterSize - 1) / 3
	if e.commitCount < 2*f+1 || e.executed {
		return
	}
	e.committed = true
	if e.ts == 0 {
		return
	}
	p.deliver(e)
}
