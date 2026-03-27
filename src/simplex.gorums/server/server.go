package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/relab/gorums"
	pb "github.com/relab/gorums/examples/simplex/proto"
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

// ─── global key registry ─────────────────────────────────────────────────────

var keysMu sync.RWMutex
var privKeys map[uint32]ed25519.PrivateKey
var pubKeys map[uint32]ed25519.PublicKey

// InitKeys pre-generates ed25519 key pairs for n nodes (IDs 1..n).
// Must be called before StartServer.
func InitKeys(n int) {
	keysMu.Lock()
	defer keysMu.Unlock()
	privKeys = make(map[uint32]ed25519.PrivateKey, n)
	pubKeys = make(map[uint32]ed25519.PublicKey, n)
	for i := 1; i <= n; i++ {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			log.Fatalf("simplex: ed25519.GenerateKey: %v", err)
		}
		privKeys[uint32(i)] = priv
		pubKeys[uint32(i)] = pub
	}
}

func getKeyPair(id uint32) (ed25519.PrivateKey, map[uint32]ed25519.PublicKey) {
	keysMu.RLock()
	defer keysMu.RUnlock()
	priv := privKeys[id]
	pubs := make(map[uint32]ed25519.PublicKey, len(pubKeys))
	for k, v := range pubKeys {
		pubs[k] = v
	}
	return priv, pubs
}

// ─── in-process server registry ──────────────────────────────────────────────

var regMu sync.RWMutex
var registry = make(map[string]*Server)

func registerServer(addr string, s *Server) {
	regMu.Lock()
	registry[addr] = s
	regMu.Unlock()
}

// Lookup returns the in-process Server for addr, or nil.
func Lookup(addr string) *Server {
	regMu.RLock()
	s := registry[addr]
	regMu.RUnlock()
	return s
}

// ─── Server ──────────────────────────────────────────────────────────────────

// Server wraps a gorums system and the Simplex consensus node.
type Server struct {
	id    uint32
	nodes []NodeInfo
	sys   *gorums.System
	nd    *simplexNode
}

// New creates a Server ready to be started, following the same convention as
// the PBFT server: peers is a map of nodeID → address (including this node).
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

	dialOpt := gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))
	sys, err := gorums.NewSystem(addr,
		gorums.WithConfig(s.id, peerList),
		peerList,
		dialOpt,
	)
	if err != nil {
		log.Fatal("simplex: NewSystem:", err)
	}
	s.sys = sys

	priv, pubs := getKeyPair(s.id)
	nd := newSimplexNode(s.id, len(s.nodes), sys.OutboundConfig(), priv, pubs)
	s.nd = nd

	sys.RegisterService(nil, func(gs *gorums.Server) {
		pb.RegisterSimplexNodeServer(gs, nd)
	})

	go func() {
		if err := sys.Serve(); err != nil {
			log.Println("simplex: serve error:", err)
		}
	}()

	registerServer(addr, s)

	slog.Info("simplex: ready", "node", s.id, "addr", addr)
	// Protocol is started separately via StartProtocol() once all peers are up.
}

// Stop implements BenchmarkServer.
func (s *Server) Stop() {
	if s.nd != nil {
		s.nd.Stop()
	}
	if s.sys != nil {
		s.sys.Stop()
	}
}

// StartProtocol starts the Simplex consensus protocol on this node.
// It must be called after all peer servers are listening.
func (s *Server) StartProtocol() {
	s.nd.Start()
}

// AddTxAndWait submits a transaction directly to this node's pool and
// blocks until the transaction is finalized by the cluster.
func (s *Server) AddTxAndWait(ctx context.Context, tx string) error {
	return s.nd.addTxAndWait(ctx, tx)
}

// StartServer starts a Simplex node in-process and returns immediately.
// Used by the bench framework's CreateServer.
func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	return s, nil
}

// RunServer starts a replica and blocks until SIGINT/SIGTERM.
// Used by the CLI binary.
func RunServer(id uint32, nodes []NodeInfo, verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))

	// For standalone CLI use, generate keys for this node only (no benchmark key registry).
	// In this mode all nodes must be started with the same key set, which requires
	// out-of-band key distribution. For simplicity in testing, generate fresh keys and
	// note that signature verification will fail with other nodes unless keys are shared.
	InitKeys(len(nodes))

	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	// Wait for peer gRPC connections to be established before starting the protocol.
	time.Sleep(2 * time.Second)
	s.StartProtocol()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("simplex: shutting down", "node", id)
	s.Stop()
}

// ─── Client ──────────────────────────────────────────────────────────────────

// Client is used by the benchmark framework. It holds the addresses of all
// cluster nodes and submits transactions to the best available in-process node.
type Client struct {
	nodes []NodeInfo
}

// NewClient creates a Client for the given cluster nodes.
func NewClient(nodes []NodeInfo) *Client {
	return &Client{nodes: nodes}
}

// Close is a no-op for the simplex client (no network connections to close).
func (c *Client) Close() {}
