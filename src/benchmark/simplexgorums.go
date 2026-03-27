package bench

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	simplexserver "github.com/Mekruba/gorums-benchmark/simplex.gorums/server"
)

// SimplexAddrs are the default local addresses for the 4-node simplex cluster.
var SimplexAddrs = []string{
	"127.0.0.1:9090",
	"127.0.0.1:9091",
	"127.0.0.1:9092",
	"127.0.0.1:9093",
}

var simplexNewNodes = []simplexserver.NodeInfo{
	{ID: 1, Addr: "127.0.0.1:9090"},
	{ID: 2, Addr: "127.0.0.1:9091"},
	{ID: 3, Addr: "127.0.0.1:9092"},
	{ID: 4, Addr: "127.0.0.1:9093"},
}

// ── benchmark implementation ──────────────────────────────────────────────────

type SimplexGorumsBenchmark struct {
	clients []*simplexserver.Client
	counter atomic.Uint64
}

func (b *SimplexGorumsBenchmark) CreateServer(addr string, _ []string) (*simplexserver.Server, func(), error) {
	var id uint32
	for _, n := range simplexNewNodes {
		if n.Addr == addr {
			id = n.ID
			break
		}
	}
	if id == 0 {
		return nil, nil, fmt.Errorf("simplex: unknown addr %s", addr)
	}
	srv, err := simplexserver.StartServer(id, simplexNewNodes)
	if err != nil {
		return nil, nil, err
	}
	return srv, func() { srv.Stop() }, nil
}

func (b *SimplexGorumsBenchmark) Init(opts RunOptions) {
	// Pre-generate keys for all nodes before any server is started.
	simplexserver.InitKeys(len(simplexNewNodes))
	b.clients = make([]*simplexserver.Client, 0, opts.numClients)
	createClients(b, opts)
}

func (b *SimplexGorumsBenchmark) AddClient(_ int, _ string, _ []string, _ *slog.Logger) {
	c := simplexserver.NewClient(simplexNewNodes)
	b.clients = append(b.clients, c)
}

func (b *SimplexGorumsBenchmark) Clients() []*simplexserver.Client {
	return b.clients
}

func (b *SimplexGorumsBenchmark) Config() *simplexserver.Client {
	if len(b.clients) == 0 {
		return nil
	}
	return b.clients[0]
}

func (b *SimplexGorumsBenchmark) Stop() {
	for _, c := range b.clients {
		c.Close()
	}
}

func (b *SimplexGorumsBenchmark) StartBenchmark(_ *simplexserver.Client) []Result {
	// All servers are now listening. Give gRPC time to establish peer connections,
	// then start the Simplex protocol on all nodes simultaneously.
	time.Sleep(1500 * time.Millisecond)
	for _, n := range simplexNewNodes {
		if srv := simplexserver.Lookup(n.Addr); srv != nil {
			srv.StartProtocol()
		}
	}
	return nil
}

func (b *SimplexGorumsBenchmark) StopBenchmark(_ *simplexserver.Client) []Result {
	return nil
}

// Run submits one transaction to the cluster and waits for it to be finalized.
// It picks the node that is most likely the current leader based on height,
// falling back to a round-robin if no nodes are reachable.
func (b *SimplexGorumsBenchmark) Run(c *simplexserver.Client, ctx context.Context, _ int) error {
	seq := b.counter.Add(1)
	tx := fmt.Sprintf("bm.%d", seq)

	// Try to submit to the current leader for minimal latency.
	// Leader is deterministic: leaderForHeight(h, n) using SHA-256.
	// We look up nodes in order and pick the one that is currently a leader.
	// Fallback: round-robin across all nodes.
	idx := int(seq-1) % len(simplexNewNodes)
	targetAddr := simplexNewNodes[idx].Addr

	// Walk forward to find a registered server.
	for i := range simplexNewNodes {
		tryAddr := simplexNewNodes[(idx+i)%len(simplexNewNodes)].Addr
		srv := simplexserver.Lookup(tryAddr)
		if srv != nil {
			targetAddr = tryAddr
			break
		}
	}

	srv := simplexserver.Lookup(targetAddr)
	if srv == nil {
		return fmt.Errorf("simplex: no server found at %s", targetAddr)
	}
	return srv.AddTxAndWait(ctx, tx)
}
