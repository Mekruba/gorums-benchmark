package server

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
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
	"syscall"
	"time"

	"slices"

	pb "github.com/Mekruba/gorums-benchmark/simplex.gorums/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
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

// ─── Failure detector ────────────────────────────────────────────────────────

// FailureDetectorConfig controls the behaviour of the per-node failure detector.
// Each active member is probed every ProbeInterval. If a node misses
// MissThreshold consecutive probes it is declared failed. The failed node is
// replaced by the next available standby from StandbyPool (in order).
// Only one node submits the reconfig tx at a time; duplicates are no-ops.
type FailureDetectorConfig struct {
	// ProbeInterval is the period between health checks (default 500ms).
	ProbeInterval time.Duration
	// MissThreshold is the number of consecutive missed probes before a node
	// is declared failed (default 3, so 1.5s with 500ms interval).
	MissThreshold int
	// StandbyPool is the ordered list of node IDs to promote when a member
	// fails. These nodes must be started and connected but are excluded from
	// the initial active member set.
	StandbyPool []uint32
}

// failureDetector runs on each node and probes the HTTP /sync endpoint of
// every currently active member. When a node is detected as failed the
// detector computes a new member set (remove failed, add next standby) and
// submits a ReconfigureAndWait. ReconfigureAndWait is idempotent when the
// member set is unchanged, so simultaneous submissions from multiple nodes
// detecting the same failure converge safely.
type failureDetector struct {
	srv    *Server
	cfg    FailureDetectorConfig
	httpCl *http.Client
	// missCount tracks consecutive missed probes per node ID.
	missCount map[uint32]int
	// standbyIdx is the index into cfg.StandbyPool of the next standby to promote.
	standbyIdx int
	ctx        context.Context
	cancel     context.CancelFunc
}

func newFailureDetector(srv *Server, cfg FailureDetectorConfig) *failureDetector {
	if cfg.ProbeInterval <= 0 {
		cfg.ProbeInterval = 500 * time.Millisecond
	}
	if cfg.MissThreshold <= 0 {
		cfg.MissThreshold = 3
	}
	// Probe timeout must be comfortably larger than ProbeInterval to avoid
	// false positives under load. A slow but healthy node should still respond
	// to the lightweight /health endpoint within a few seconds.
	probeTimeout := cfg.ProbeInterval*4 + 2*time.Second
	ctx, cancel := context.WithCancel(context.Background())
	return &failureDetector{
		srv:       srv,
		cfg:       cfg,
		httpCl:    &http.Client{Timeout: probeTimeout},
		missCount: make(map[uint32]int),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (fd *failureDetector) run() {
	ticker := time.NewTicker(fd.cfg.ProbeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-fd.ctx.Done():
			return
		case <-ticker.C:
			fd.probe()
		}
	}
}

func (fd *failureDetector) probe() {
	active := fd.srv.nd.Members()
	for _, id := range active {
		if id == fd.srv.id {
			// Don't probe self — we know we're up.
			fd.missCount[id] = 0
			continue
		}
		addr := fd.addrForID(id)
		if addr == "" {
			continue
		}
		if fd.ping(addr) {
			fd.missCount[id] = 0
		} else {
			fd.missCount[id]++
			if fd.missCount[id] >= fd.cfg.MissThreshold {
				slog.Warn("simplex: failure detector: node unresponsive, triggering reconfig",
					"detector", fd.srv.id, "failed", id, "misses", fd.missCount[id])
				fd.handleFailure(id, active)
				return // re-probe next tick with updated membership
			}
		}
	}
}

func (fd *failureDetector) handleFailure(failedID uint32, currentActive []uint32) {
	// Build new member set: remove failed node, add next standby if available.
	newMembers := make([]uint32, 0, len(currentActive))
	for _, id := range currentActive {
		if id != failedID {
			newMembers = append(newMembers, id)
		}
	}
	if fd.standbyIdx < len(fd.cfg.StandbyPool) {
		newMembers = append(newMembers, fd.cfg.StandbyPool[fd.standbyIdx])
		fd.standbyIdx++
	}
	if len(newMembers) == 0 {
		slog.Warn("simplex: failure detector: new member set would be empty, skipping reconfig",
			"detector", fd.srv.id)
		return
	}
	slog.Info("simplex: failure detector: submitting reconfig",
		"detector", fd.srv.id, "removed", failedID, "newMembers", newMembers)
	// Reset miss counter so we don't re-trigger on the same node until a new
	// probe cycle. The node is now removed from active membership anyway.
	delete(fd.missCount, failedID)
	ctx, cancel := context.WithTimeout(fd.ctx, 30*time.Second)
	defer cancel()
	if err := fd.srv.ReconfigureAndWait(ctx, newMembers); err != nil {
		slog.Warn("simplex: failure detector: reconfig failed",
			"detector", fd.srv.id, "err", err)
	} else {
		slog.Info("simplex: failure detector: reconfig committed",
			"detector", fd.srv.id, "members", newMembers)
	}
}

func (fd *failureDetector) ping(addr string) bool {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return false
	}
	// Use the lightweight /health endpoint so probes don't compete with
	// heavy chain serialisation under load.
	url := fmt.Sprintf("http://%s:%d/health", host, port+100)
	req, err := http.NewRequestWithContext(fd.ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := fd.httpCl.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

func (fd *failureDetector) addrForID(id uint32) string {
	for _, n := range fd.srv.nodes {
		if n.ID == id {
			return n.Addr
		}
	}
	return ""
}

func (fd *failureDetector) stop() {
	fd.cancel()
}

// Server wraps a gorums system and the Simplex consensus node.
type Server struct {
	id             uint32
	nodes          []NodeInfo
	initialMembers []uint32 // nil = all nodes active (default)
	sys            *gorums.System
	nd             *simplexNode
	httpSrv        *http.Server           // non-nil once serveHTTP is running
	fd             *failureDetector       // nil if not configured
	fdCfg          *FailureDetectorConfig // non-nil if failure detection is enabled
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
	nd := newSimplexNode(s.id, sys.OutboundConfig(), s.initialMembers, priv, pubs)
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
			s.StartProtocol() // also starts failure detector if configured
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
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/tx", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		tx := string(body)
		if err := s.nd.addTxAndWait(r.Context(), tx); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		// Returns the full notarized chain as a length-prefixed protobuf binary
		// stream: [4-byte big-endian len][proto bytes] repeated per entry.
		// For each entry we wrap in a NotarizedChainMsg so the decoder can
		// reuse handleNotarizedChain on the receiving side.
		chain := s.nd.GetChain()
		w.Header().Set("Content-Type", "application/octet-stream")
		var lenBuf [4]byte
		for _, nota := range chain {
			msg := pb.NotarizedChainMsg_builder{Chain: []*pb.Notarization{nota}}.Build()
			data, encErr := proto.Marshal(msg)
			if encErr != nil {
				http.Error(w, encErr.Error(), http.StatusInternalServerError)
				return
			}
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
			w.Write(lenBuf[:])
			w.Write(data)
		}
	})
	mux.HandleFunc("/reconfigure", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		parts := strings.Split(string(body), ",")
		memberIDs := make([]uint32, 0, len(parts))
		for _, p := range parts {
			trimmed := strings.TrimSpace(p)
			if trimmed == "" {
				continue
			}
			v, convErr := strconv.ParseUint(trimmed, 10, 32)
			if convErr != nil {
				http.Error(w, fmt.Sprintf("invalid member ID %q", trimmed), http.StatusBadRequest)
				return
			}
			memberIDs = append(memberIDs, uint32(v))
		}
		if err := s.ReconfigureAndWait(r.Context(), memberIDs); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	httpServer := &http.Server{Addr: httpAddr, Handler: mux}
	s.httpSrv = httpServer
	if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("simplex: HTTP server error: %v", err)
	}
}

// Stop implements BenchmarkServer.
func (s *Server) Stop() {
	if s.fd != nil {
		s.fd.stop()
	}
	if s.httpSrv != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		s.httpSrv.Shutdown(ctx) //nolint:errcheck
	}
	if s.nd != nil {
		s.nd.Stop()
	}
	if s.sys != nil {
		s.sys.Stop()
	}
}

// StartProtocol starts the Simplex consensus protocol on this node.
// It must be called after all peer servers are listening.
// If a failure detector was configured via EnableFailureDetector it is
// also started here, after a short warm-up delay.
func (s *Server) StartProtocol() {
	s.nd.Start()
	if s.fdCfg != nil {
		s.fd = newFailureDetector(s, *s.fdCfg)
		// Short warm-up: let the cluster run for a full probe interval before
		// the detector starts issuing probes, to avoid false positives at boot.
		go func() {
			time.Sleep(s.fdCfg.ProbeInterval * 3)
			slog.Info("simplex: failure detector started", "node", s.id,
				"probe_interval", s.fdCfg.ProbeInterval,
				"miss_threshold", s.fdCfg.MissThreshold,
				"standbys", s.fdCfg.StandbyPool)
			s.fd.run()
		}()
	}
}

// EnableFailureDetector configures the failure detector. Must be called before
// StartProtocol. When a member fails (misses MissThreshold consecutive health
// checks), the detector submits a reconfig tx that removes the failed node and
// promotes the next standby from StandbyPool.
func (s *Server) EnableFailureDetector(cfg FailureDetectorConfig) {
	s.fdCfg = &cfg
}

// AddTxAndWait submits a transaction directly to this node's pool and
// blocks until the transaction is finalized by the cluster.
func (s *Server) AddTxAndWait(ctx context.Context, tx string) error {
	return s.nd.addTxAndWait(ctx, tx)
}

// ReconfigureAndWait submits a reconfiguration command transaction and blocks
// until it is finalized by the cluster. It is a no-op if the proposed member
// set is identical to the current active membership (deduplicates concurrent
// reconfig submissions from multiple nodes detecting the same failure).
func (s *Server) ReconfigureAndWait(ctx context.Context, members []uint32) error {
	// Deduplicate: if the cluster already has exactly this member set, skip.
	sorted := slices.Clone(members)
	slices.Sort(sorted)
	current := s.nd.Members()
	slices.Sort(current)
	if slices.Equal(sorted, current) {
		slog.Info("simplex: reconfig no-op, membership unchanged", "node", s.id, "members", sorted)
		return nil
	}
	tx, err := buildReconfigTx(members)
	if err != nil {
		return err
	}
	return s.nd.addTxAndWait(ctx, tx)
}

// ID returns this server's node ID.
func (s *Server) ID() uint32 {
	return s.id
}

// Members returns the current active consensus member IDs for this node.
// Safe to call concurrently. Returns nil before the protocol has started.
func (s *Server) Members() []uint32 {
	if s.nd == nil {
		return nil
	}
	return s.nd.Members()
}

// GetChain returns a snapshot of this server's notarized chain.
func (s *Server) GetChain() []*pb.Notarization {
	if s.nd == nil {
		return nil
	}
	return s.nd.GetChain()
}

// StartServer starts a Simplex node in-process and returns immediately.
// Used by the bench framework's CreateServer (all nodes active from the start).
func StartServer(id uint32, nodes []NodeInfo) (*Server, error) {
	s := &Server{id: id, nodes: nodes}
	s.Start(true)
	return s, nil
}

// StartServerWithInitialMembers starts a Simplex node where only the listed
// node IDs participate in consensus initially. All nodes still connect to each
// other via Gorums (for networking), but only the initial members propose,
// vote, and finalize until a reconfiguration tx widens the active set.
func StartServerWithInitialMembers(id uint32, nodes []NodeInfo, initialMembers []uint32) (*Server, error) {
	s := &Server{id: id, nodes: nodes, initialMembers: initialMembers}
	s.Start(true)
	return s, nil
}

// StartServerWithFailureDetector starts a node with an initial member set and
// enables automatic failure detection. failedNodes will be replaced from
// standbys when they stop responding.
//
//	Example — start with 4 active, standbys 5,6,7, auto-replace on failure:
//	  srv, _ := StartServerWithFailureDetector(id, nodes,
//	      []uint32{1,2,3,4},
//	      FailureDetectorConfig{
//	          ProbeInterval: 500*time.Millisecond,
//	          MissThreshold: 3,
//	          StandbyPool:   []uint32{5, 6, 7},
//	      })
func StartServerWithFailureDetector(id uint32, nodes []NodeInfo, initialMembers []uint32, fdCfg FailureDetectorConfig) (*Server, error) {
	s := &Server{id: id, nodes: nodes, initialMembers: initialMembers, fdCfg: &fdCfg}
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
	transport := &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
		MaxConnsPerHost:     0, // unlimited
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}
	return &Client{
		nodes:  nodes,
		httpCl: &http.Client{Timeout: 30 * time.Second, Transport: transport},
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

// Reconfigure submits a membership change to a node via HTTP and waits for it
// to be finalized by consensus. Body format is comma-separated IDs (e.g. "1,2,3").
func (c *Client) Reconfigure(ctx context.Context, members []uint32, nodeID uint32) error {
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
	tx, err := buildReconfigTx(members)
	if err != nil {
		return err
	}
	body := strings.TrimPrefix(tx, reconfigTxPrefix)
	url := fmt.Sprintf("http://%s:%d/reconfigure", host, port+100)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.httpCl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("simplex: node %d reconfigure error: %s", nodeID, respBody)
	}
	return nil
}
