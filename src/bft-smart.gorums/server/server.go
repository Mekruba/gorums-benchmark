package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type NodeInfo struct {
	ID   uint32
	Addr string
}

type NodeAddr struct{ Addr_ string }

func (n NodeAddr) Addr() string { return n.Addr_ }

type Server struct {
	id          uint32
	nodes       []NodeInfo
	standby     bool
	sys         *gorums.System
	watchCancel context.CancelFunc
}

func New(id uint32, peers map[int]string, standby bool) *Server {
	nodes := make([]NodeInfo, 0, len(peers))
	for nid, addr := range peers {
		nodes = append(nodes, NodeInfo{ID: uint32(nid), Addr: addr})
	}
	return &Server{id: id, nodes: nodes, standby: standby}
}

func NewFromNodeInfo(id uint32, nodes []NodeInfo, standby bool) *Server {
	return &Server{id: id, nodes: nodes, standby: standby}
}

func (s *Server) Start(_ bool) {
	InitKeys(len(s.nodes))

	peerMap := make(map[uint32]NodeAddr)
	for _, n := range s.nodes {
		peerMap[n.ID] = NodeAddr{Addr_: n.Addr}
	}
	peers := gorums.WithNodes(peerMap)
	dialOpt := gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))

	addr := s.addrOf(s.id)
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatalf("bad addr %q: %v", addr, err)
	}

	sys, err := gorums.NewSystem(":"+port,
		gorums.WithServerOptions(
			gorums.WithConfig(s.id, peers),
			gorums.WithBufferSizes(1024, 1024),
		),
		gorums.WithOutboundNodes(peers),
		dialOpt,
	)
	if err != nil {
		log.Fatal(err)
	}
	s.sys = sys

	impl := NewBFTSmartServer(s.id, len(s.nodes))
	sys.RegisterService(nil, func(gs *gorums.Server) {
		pb.RegisterBFTSmartServer(gs, impl)
	})

	go func() {
		if err := sys.Serve(); err != nil {
			log.Println("serve error:", err)
		}
	}()

	time.Sleep(2 * time.Second)
	outbound := sys.OutboundConfig()
	s.waitForPeers(outbound, len(s.nodes))
	impl.SetOutboundConfig(outbound)

	slog.Info("outbound config", "node", s.id, "size", outbound.Size(), "peers", outbound.NodeIDs())

	watchCtx, cancel := context.WithCancel(context.Background())
	s.watchCancel = cancel
	go s.watchMembership(watchCtx, impl)

	if s.standby {
		go func() {
			slog.Info("standby: starting state transfer", "node", s.id)
			if err := impl.SendStateTransfer(); err != nil {
				log.Fatalf("state transfer failed: %v", err)
			}
			time.Sleep(3 * time.Second)
			impl.sendStop()
		}()
	}

	slog.Info("ready", "node", s.id, "addr", addr)
}

func (s *Server) waitForPeers(cfg gorums.Configuration, expected int) {
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := pb.Ping(cfg.Context(ctx), &emptypb.Empty{}).Threshold(expected - 1)
		cancel()
		if err == nil {
			slog.Info("all peers connected", "node", s.id)
			return
		}
		slog.Info("waiting for peers", "node", s.id, "err", err)
		time.Sleep(5 * time.Second)
	}
}

// watchMembership monitors the outbound config for dead nodes and the inbound
// config for newly joining nodes. Both are handled in a single goroutine to
// avoid races between two goroutines calling SetOutboundConfig concurrently.
func (s *Server) watchMembership(ctx context.Context, impl *BFTSmartServer) {
	// track which node IDs we have already sent ViewUpdate JOIN for
	knownJoined := make(map[uint32]bool)
	for _, n := range s.nodes {
		knownJoined[n.ID] = true
	}

	outbound := s.sys.OutboundConfig()
	ch := outbound.Watch(ctx, 2*time.Second, func(c gorums.Configuration) gorums.Configuration {
		return c.SortBy(gorums.LastNodeError)
	})

	for cfg := range ch {
		// ── outbound: remove dead nodes ───────────────────────────────────
		var dead []uint32
		for _, n := range cfg.Nodes() {
			if n.LastErr() != nil {
				dead = append(dead, n.ID())
			}
		}
		if len(dead) > 0 {
			cfg = cfg.Remove(dead...)
			slog.Info("removed dead nodes", "node", s.id, "dead", dead, "alive", cfg.NodeIDs())
			impl.SetOutboundConfig(cfg)

			impl.mu.Lock()
			isLeader := impl.leader
			f := impl.f()
			impl.mu.Unlock()
			if isLeader && cfg.Size() < 2*f+1 {
				slog.Warn("leader cannot reach quorum, triggering sync",
					"node", s.id, "alive", cfg.Size(), "needed", 2*f+1)
				go impl.sendStop()
			}
		} else {
			impl.SetOutboundConfig(cfg)
		}

		// ── inbound: detect newly joined nodes (standbys) ─────────────────
		// sys.Config() returns the inbound peer set — nodes whose streams
		// are established to this server. Non-original nodes are standbys.
		inbound := s.sys.Config()
		slog.Info("inbound peers", "node", s.id,
			"inbound_size", inbound.Size(), "inbound_ids", inbound.NodeIDs())

		impl.mu.Lock()
		isLeader := impl.leader
		view := impl.view
		cid := impl.consensusID.Load()
		impl.mu.Unlock()

		for _, n := range inbound.Nodes() {
			id := n.ID()
			if knownJoined[id] {
				continue
			}
			knownJoined[id] = true

			slog.Info("new node visible in inbound", "node", s.id,
				"new_node", id, "is_leader", isLeader)

			if !isLeader {
				continue
			}

			// leader adds the new node to outbound and notifies everyone
			current := impl.outbound()
			updated := current.Add(id)
			impl.SetOutboundConfig(updated)
			impl.mu.Lock()
			impl.clusterSize++
			impl.mu.Unlock()

			slog.Info("sending ViewUpdate JOIN", "node", s.id,
				"new_node", id, "new_view", view+1, "cluster_size", impl.clusterSize)

			sig := sign(getPrivKey(s.id), viewUpdateDigest(view+1, id, cid))
			pb.ViewUpdate(updated.Context(context.Background()),
				pb.ViewUpdateMsg_builder{
					NewView:     view + 1,
					LeaderId:    s.id,
					TargetId:    id,
					ConsensusId: cid,
					Action:      pb.MembershipAction_JOIN,
					Signature:   sig,
				}.Build(), gorums.IgnoreErrors())
		}
	}
}

func (s *Server) Stop() {
	if s.watchCancel != nil {
		s.watchCancel()
	}
	if s.sys != nil {
		s.sys.Stop()
	}
}

func (s *Server) addrOf(id uint32) string {
	for _, n := range s.nodes {
		if n.ID == id {
			return n.Addr
		}
	}
	log.Fatalf("node ID %d not in peer list", id)
	return ""
}

func InitLogger(id uint32, verbose bool) {
	level := slog.LevelWarn
	var w io.Writer = os.Stderr
	if verbose {
		level = slog.LevelDebug
		f, err := os.Create(fmt.Sprintf("node-%d.log", id))
		if err == nil {
			w = io.MultiWriter(os.Stderr, f)
		}
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{Level: level})))
}

func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := NewFromNodeInfo(id, nodes, false)
	s.Start(true)
	return s, nil
}

func RunServer(id uint32, nodes []NodeInfo, verbose, standby bool) {
	InitKeys(len(nodes))
	InitLogger(id, verbose)
	s := NewFromNodeInfo(id, nodes, standby)
	s.Start(true)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	slog.Info("shutting down", "node", id)
	s.Stop()
}
