package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
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
	var output io.Writer = os.Stderr

	if verbose {
		level = slog.LevelDebug
		filename := fmt.Sprintf("node-%d.log", id)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to create log file: %v\n", err)
		} else {
			output = io.MultiWriter(os.Stderr, file)
		}
	}

	handler := slog.NewTextHandler(output, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
}

// ── Server ────────────────────────────────────────────────────────────────────

type Server struct {
	id    uint32
	nodes []NodeInfo
	sys   *gorums.System
}

// New creates a Server from the map[int]string format used by main.go.
// peers maps node ID → "host:port".
func New(id uint32, peers map[int]string) *Server {
	nodes := make([]NodeInfo, 0, len(peers))
	for nodeID, addr := range peers {
		nodes = append(nodes, NodeInfo{ID: uint32(nodeID), Addr: addr})
	}
	return &Server{id: id, nodes: nodes}
}

// NewFromNodeInfo creates a Server from a []NodeInfo slice (used in local/test mode).
func NewFromNodeInfo(id uint32, nodes []NodeInfo) *Server {
	return &Server{id: id, nodes: nodes}
}

// Start brings the gorums system up. The local parameter is kept for interface
// compatibility but no longer changes behaviour — callers that need to block
// after Start should do so themselves (see RunServer / main.go docker mode).
func (s *Server) Start(_ bool) {

	peerMap := make(map[uint32]NodeAddr)
	for _, n := range s.nodes {
		peerMap[n.ID] = NodeAddr{Addr_: n.Addr}
	}
	peerList := gorums.WithNodes(peerMap)
	dialOpts := gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))

	var addr string
	for _, n := range s.nodes {
		if n.ID == s.id {
			addr = n.Addr
			break
		}
	}
	if addr == "" {
		log.Fatalf("server: node ID %d not found in peer list", s.id)
	}

	sys, err := gorums.NewSystem(addr,
		gorums.WithConfig(s.id, peerList),
		gorums.WithReceiveBufferSize(1024),
		gorums.WithPeerSendBufferSize(512),
		peerList,
		dialOpts,
	)
	if err != nil {
		log.Fatal(err)
	}
	s.sys = sys

	pbft := NewPBFTServer(s.id, len(s.nodes))
	sys.RegisterService(nil, func(gs *gorums.Server) {
		pb.RegisterPBFTServer(gs, pbft)
	})

	go func() {
		if err := sys.Serve(); err != nil {
			log.Println("serve error:", err)
		}
	}()

	slog.Info("ready", "node", s.id, "addr", addr)
	// Brief pause so all nodes finish binding before clients connect.
	time.Sleep(2 * time.Second)
}

func (s *Server) Stop() {
	if s.sys != nil {
		s.sys.Stop()
	}
}

// ── Convenience constructors used by benchmark framework and tests ─────────────

// StartServer starts a server and returns it. Used by the benchmark framework
// in local mode (PbftGorumsNewBenchmark.CreateServer).
func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := NewFromNodeInfo(id, nodes)
	s.Start(true)
	return s, nil
}

// RunServer starts a server and blocks until SIGINT/SIGTERM. Used by the
// standalone pbft.gorums.new/main.go CLI.
func RunServer(id uint32, nodes []NodeInfo, verbose bool) {
	InitLogger(id, verbose)
	s := NewFromNodeInfo(id, nodes)
	s.Start(true)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	slog.Info("shutting down", "node", id)
	s.Stop()
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
