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
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/simplex.gorums/proto"
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

// ─── global key registry ─────────────────────────────────────────────────────

var keysMu sync.RWMutex
var privKeys map[uint32]ed25519.PrivateKey
var pubKeys map[uint32]ed25519.PublicKey

// InitKeys pre-generates ed25519 key pairs for n nodes (IDs 1..n).
// Uses a fixed seed so all nodes (including separate containers) produce
// identical key pairs and can verify each other's signatures.
// Must be called before StartServer.
func InitKeys(n int) {
	keysMu.Lock()
	defer keysMu.Unlock()
	// Fixed seed: all processes must generate keys in the same order.
	rng := rand.New(rand.NewSource(0x53696d706c6578)) //nolint:gosec
	privKeys = make(map[uint32]ed25519.PrivateKey, n)
	pubKeys = make(map[uint32]ed25519.PublicKey, n)
	for i := 1; i <= n; i++ {
		pub, priv, err := ed25519.GenerateKey(rng)
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
func (s *Server) Start(local bool) {
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

	// Listen on all interfaces so the container/VM doesn't need the config
	// IP bound to a specific interface. Peers still connect using the full
	// addr (IP:port) from the config via peerMap.
	_, port, splitErr := net.SplitHostPort(addr)
	if splitErr != nil {
		log.Fatalf("simplex: bad self addr %q: %v", addr, splitErr)
	}
	listenAddr := ":" + port

	dialOpt := gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials()))
	sys, err := gorums.NewSystem(listenAddr,
		gorums.WithServerOptions(gorums.WithConfig(s.id, peerList)),
		gorums.WithOutboundNodes(peerList),
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

	if !local {
		// Docker mode: start the protocol after a delay to allow all peers
		// to be up and gRPC connections to establish.
		// The HTTP tx-injection endpoint starts only after the protocol is
		// running so the health check (which watches port+100) keeps the
		// client container waiting until we are truly ready.
		go func() {
			time.Sleep(5 * time.Second)
			slog.Info("simplex: starting protocol", "node", s.id)
			s.nd.Start()
			go s.serveHTTP(addr)
		}()
	} else {
		// In local mode the HTTP endpoint is not needed for coordination,
		// but start it anyway so the same binary works for manual testing.
		go s.serveHTTP(addr)
	}
	// In local mode StartProtocol() is called explicitly by StartBenchmark().
}

// serveHTTP starts a minimal HTTP server for external transaction injection.
// It listens on the gRPC port + 100. POST /tx <body> submits a transaction
// and blocks until it is finalized (or the request context is cancelled).
func (s *Server) serveHTTP(grpcAddr string) {
	_, portStr, err := net.SplitHostPort(grpcAddr)
	if err != nil {
		log.Printf("simplex: serveHTTP: bad addr %s: %v", grpcAddr, err)
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		log.Printf("simplex: serveHTTP: bad port %s: %v", portStr, err)
		return
	}
	httpAddr := fmt.Sprintf(":%d", port+100)
	mux := http.NewServeMux()
	var txCount atomic.Int64
	var txSuccess atomic.Int64
	var txFail atomic.Int64
	mux.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tx := string(body)
		seq := txCount.Add(1)
		if seq <= 3 {
			slog.Info("DEBUG /tx: received", "node", s.id, "tx", tx, "seq", seq)
		}
		if err := s.nd.addTxAndWait(r.Context(), tx); err != nil {
			fails := txFail.Add(1)
			if fails <= 10 || fails%100 == 0 {
				slog.Warn("DEBUG /tx: FAILED", "node", s.id, "tx", tx, "err", err, "totalFails", fails, "totalReceived", seq)
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		ok := txSuccess.Add(1)
		if ok <= 5 || ok%100 == 0 {
			slog.Info("DEBUG /tx: SUCCESS", "node", s.id, "tx", tx, "totalSuccess", ok)
		}
		w.WriteHeader(http.StatusOK)
	})
	if err := http.ListenAndServe(httpAddr, mux); err != nil && err != http.ErrServerClosed {
		log.Printf("simplex: HTTP server error: %v", err)
	}
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
// cluster nodes and submits transactions to the best available in-process node
// or via HTTP in docker/VM mode.
type Client struct {
	nodes  []NodeInfo
	httpCl *http.Client
}

// NewClient creates a Client for the given cluster nodes.
func NewClient(nodes []NodeInfo) *Client {
	return &Client{
		nodes:  nodes,
		httpCl: &http.Client{Timeout: 30 * time.Second},
	}
}

// Close is a no-op for the simplex client (no network connections to close).
func (c *Client) Close() {}

// Submit sends a transaction to the node with the given ID via HTTP and waits
// for it to be finalized. Used in docker/VM mode where the server is remote.
func (c *Client) Submit(ctx context.Context, tx string, nodeID uint32) error {
	var addr string
	for _, n := range c.nodes {
		if n.ID == nodeID {
			addr = n.Addr
			break
		}
	}
	if addr == "" {
		return fmt.Errorf("simplex: unknown node %d", nodeID)
	}
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("simplex: bad addr %s: %w", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return fmt.Errorf("simplex: bad port %s: %w", portStr, err)
	}
	url := fmt.Sprintf("http://%s:%d/tx", host, port+100)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(tx))
	if err != nil {
		return err
	}
	resp, err := c.httpCl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("simplex: node %d error: %s", nodeID, body)
	}
	return nil
}
