package bench

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	simplexserver "github.com/Mekruba/gorums-benchmark/simplex.gorums/server"
)

// simplexSrvAddrsToNodes converts the index-keyed address slice used by the
// benchmark framework into the NodeInfo slice expected by the simplex package.
func simplexSrvAddrsToNodes(srvAddrs []string) []simplexserver.NodeInfo {
	nodes := make([]simplexserver.NodeInfo, 0, len(srvAddrs))
	for i, addr := range srvAddrs {
		if addr == "" {
			continue
		}
		nodes = append(nodes, simplexserver.NodeInfo{ID: uint32(i), Addr: addr})
	}
	return nodes
}

// ── benchmark implementation ──────────────────────────────────────────────────

type SimplexGorumsBenchmark struct {
	clients []*simplexserver.Client
	nodes   []simplexserver.NodeInfo
	counter atomic.Uint64
}

func (b *SimplexGorumsBenchmark) CreateServer(addr string, srvAddrs []string) (*simplexserver.Server, func(), error) {
	nodes := simplexSrvAddrsToNodes(srvAddrs)
	var id uint32
	for _, n := range nodes {
		if n.Addr == addr {
			id = n.ID
			break
		}
	}
	if id == 0 {
		return nil, nil, fmt.Errorf("simplex: unknown addr %s", addr)
	}
	srv, err := simplexserver.StartServer(id, nodes)
	if err != nil {
		return nil, nil, err
	}
	return srv, func() { srv.Stop() }, nil
}

func (b *SimplexGorumsBenchmark) Init(opts RunOptions) {
	b.nodes = simplexSrvAddrsToNodes(opts.srvAddrs)
	if len(b.nodes) == 0 {
		panic("simplex: no server addresses provided in RunOptions.srvAddrs")
	}
	// Pre-generate keys for all nodes before any server is started.
	simplexserver.InitKeys(len(b.nodes))
	b.clients = make([]*simplexserver.Client, 0, opts.numClients)
	createClients(b, opts)
}

func (b *SimplexGorumsBenchmark) AddClient(_ int, _ string, _ []string, _ *slog.Logger) {
	c := simplexserver.NewClient(b.nodes)
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
	// In local mode all servers are in-process: start protocol on each.
	// In docker mode Lookup returns nil (servers are remote); the servers
	// auto-start their protocol via their Start(false) goroutine.
	time.Sleep(1500 * time.Millisecond)
	for _, n := range b.nodes {
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
func (b *SimplexGorumsBenchmark) Run(c *simplexserver.Client, ctx context.Context, _ int) error {
	seq := b.counter.Add(1)
	tx := fmt.Sprintf("bm.%d", seq)

	idx := int(seq-1) % len(b.nodes)
	targetAddr := b.nodes[idx].Addr

	// Walk forward to find a registered in-process server (local mode).
	// In docker mode all lookups return nil; the client submits via gRPC instead.
	for i := range b.nodes {
		tryAddr := b.nodes[(idx+i)%len(b.nodes)].Addr
		srv := simplexserver.Lookup(tryAddr)
		if srv != nil {
			targetAddr = tryAddr
			break
		}
	}

	srv := simplexserver.Lookup(targetAddr)
	if srv != nil {
		return srv.AddTxAndWait(ctx, tx)
	}
	// Docker mode: submit via the client's gRPC connection.
	return c.Submit(ctx, tx, b.nodes[idx].ID)
}
