package server

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

var keysMu sync.RWMutex
var privKeys map[uint32]ed25519.PrivateKey
var pubKeys map[uint32]ed25519.PublicKey

// InitKeys pre-generates ed25519 key pairs for n nodes (IDs 1..n).
// Uses a fixed seed so all nodes produce identical key pairs and can
// verify each other's signatures without key exchange.
// Must be called before StartServer.
func InitKeys(n int) {
	keysMu.Lock()
	defer keysMu.Unlock()
	rng := rand.New(rand.NewSource(0x5042465421)) //nolint:gosec
	privKeys = make(map[uint32]ed25519.PrivateKey, n)
	pubKeys = make(map[uint32]ed25519.PublicKey, n)
	for i := 1; i <= n; i++ {
		pub, priv, err := ed25519.GenerateKey(rng)
		if err != nil {
			log.Fatalf("pbft: ed25519.GenerateKey: %v", err)
		}
		privKeys[uint32(i)] = priv
		pubKeys[uint32(i)] = pub
	}
}

func getPrivKey(id uint32) ed25519.PrivateKey {
	keysMu.RLock()
	defer keysMu.RUnlock()
	return privKeys[id]
}

func getPubKey(id uint32) (ed25519.PublicKey, bool) {
	keysMu.RLock()
	defer keysMu.RUnlock()
	k, ok := pubKeys[id]
	return k, ok
}

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
	} else {
		level = slog.LevelWarn
	}
	handler := slog.NewTextHandler(output, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(handler))
}

// ── Server ────────────────────────────────────────────────────────────────────

type Server struct {
	id          uint32
	nodes       []NodeInfo
	standby     bool
	sys         *gorums.System
	watchCancel context.CancelFunc
}

// New creates a Server from the map[int]string format used by main.go.
func New(id uint32, peers map[int]string, standby bool) *Server {
	nodes := make([]NodeInfo, 0, len(peers))
	for nodeID, addr := range peers {
		nodes = append(nodes, NodeInfo{ID: uint32(nodeID), Addr: addr})
	}
	return &Server{id: id, nodes: nodes, standby: standby}
}

// NewFromNodeInfo creates a Server from a []NodeInfo slice (used in local/test mode).
func NewFromNodeInfo(id uint32, nodes []NodeInfo, standby bool) *Server {
	return &Server{id: id, nodes: nodes, standby: standby}
}

// isOriginalNode returns true if id belongs to the original cluster node list.
func (s *Server) isOriginalNode(id uint32) bool {
	for _, n := range s.nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// Start brings the gorums system up.
func (s *Server) Start(_ bool) {
	// InitKeys must run before any signing — guaranteed regardless of entrypoint.
	InitKeys(len(s.nodes))
	allPeerMap := make(map[uint32]NodeAddr)
	for _, n := range s.nodes {
		allPeerMap[n.ID] = NodeAddr{Addr_: n.Addr}
	}

	allPeers := gorums.WithNodes(allPeerMap)
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

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.Fatalf("server: bad addr %q: %v", addr, err)
	}

	sys, err := gorums.NewSystem(":"+port,
		gorums.WithServerOptions(
			gorums.WithConfig(s.id, allPeers),
			gorums.WithBufferSizes(1024, 1024),
		),
		gorums.WithOutboundNodes(allPeers),
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

	time.Sleep(2 * time.Second)

	outbound := sys.OutboundConfig()
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := pb.Ping(outbound.Context(ctx), &emptypb.Empty{}).Threshold(3)
		cancel()
		if err == nil {
			slog.Info("all peers connected", "node", s.id)
			break
		}
		slog.Info("waiting for peers", "node", s.id, "expected", len(s.nodes), "err", err)
		time.Sleep(5 * time.Second)
	}
	pbft.SetOutboundConfig(outbound)

	slog.Info("outbound config",
		"node", s.id,
		"size", outbound.Size(),
		"peers", outbound.NodeIDs(),
	)
	for _, n := range outbound.Nodes() {
		slog.Info("peer", "node", s.id, "peer", n.FullString())
	}

	watchCtx, watchCancel := context.WithCancel(context.Background())
	s.watchCancel = watchCancel
	go s.watchMembership(watchCtx, pbft)

	if s.standby {
		// Standby: pull state from the cluster then announce readiness.
		// Runs in a goroutine so the server keeps handling incoming RPCs
		// (StateTransfer responses arrive over gRPC).
		go func() {
			slog.Info("standby mode: initiating state transfer", "node", s.id)
			if err := pbft.SendStateTransfer(); err != nil {
				log.Fatalf("state transfer failed: %v", err)
			}
			time.Sleep(3 * time.Second)
			slog.Info("state transfer complete, announcing readiness", "node", s.id)
			pbft.sendViewChange()
		}()
	}

	slog.Info("ready", "node", s.id, "addr", addr)
}

// watchMembership monitors the outbound config for node failures.
// When a dead node is detected it blocks until a standby has connected
// and is visible in sys.Config() on this replica. Only then is the view
// change triggered — ensuring all replicas see the new membership before
// any ViewChange message is sent, so enterNewView can safely compute the
// config delta from gorums state alone.
func (s *Server) watchMembership(ctx context.Context, pbft *PBFTServer) {
	outbound := s.sys.OutboundConfig()
	ch := outbound.Watch(ctx, 2*time.Second,
		func(c gorums.Configuration) gorums.Configuration {
			return c.SortBy(gorums.LastNodeError)
		})

	for newCfg := range ch {
		slog.Info("watchMembership tick",
			"node", s.id,
			"config_size", newCfg.Size(),
			"node_ids", newCfg.NodeIDs(),
		)
		for _, n := range newCfg.Nodes() {
			if n.LastErr() != nil {
				slog.Warn("watchMembership: node has error",
					"node", s.id,
					"peer", n.ID(),
					"addr", n.Address(),
					"err", n.LastErr(),
				)
			} else {
				slog.Info("watchMembership: node healthy",
					"node", s.id,
					"peer", n.ID(),
					"addr", n.Address(),
					"latency", n.Latency(),
				)
			}
		}

		// log inbound peer set (sys.Config)
		inbound := s.sys.Config()
		slog.Info("watchMembership: inbound peers",
			"node", s.id,
			"inbound_size", inbound.Size(),
			"inbound_ids", inbound.NodeIDs(),
		)

		// log any non-original nodes visible inbound (potential standbys)
		for _, n := range inbound.Nodes() {
			if !s.isOriginalNode(n.ID()) {
				slog.Info("watchMembership: non-original node visible (standby?)",
					"node", s.id,
					"peer", n.ID(),
					"addr", n.Address(),
				)
			}
		}

		var deadIDs []uint32
		for _, n := range newCfg.Nodes() {
			if n.LastErr() != nil {
				deadIDs = append(deadIDs, n.ID())
			}
		}

		if len(deadIDs) > 0 {
			newCfg = newCfg.Remove(deadIDs...)
			slog.Info("watchMembership: removed dead nodes from config",
				"node", s.id,
				"removed", deadIDs,
				"new_config", newCfg.NodeIDs(),
			)
		}

		pbft.SetOutboundConfig(newCfg)
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

// ── Convenience constructors ──────────────────────────────────────────────────

func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := NewFromNodeInfo(id, nodes, false)
	s.Start(true)
	return s, nil
}

func RunServer(id uint32, nodes []NodeInfo, verbose bool, standby bool) {
	InitKeys(len(nodes))
	InitLogger(id, verbose)
	s := NewFromNodeInfo(id, nodes, standby)
	s.Start(true)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals

	slog.Info("shutting down", "node", id)
	s.Stop()
}
