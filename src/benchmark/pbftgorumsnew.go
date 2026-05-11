package bench

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	pbftclient "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/client"
	pbftserver "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
)

// srvAddrsToNodes converts the index-keyed address slice used by the benchmark
// framework (srvAddrs[id] = "host:port", index 0 is unused) into the NodeInfo
// slice expected by the pbft client/server packages.
func srvAddrsToNodes(srvAddrs []string) []pbftserver.NodeInfo {
	nodes := make([]pbftserver.NodeInfo, 0, len(srvAddrs))
	for i, addr := range srvAddrs {
		if addr == "" {
			continue
		}
		nodes = append(nodes, pbftserver.NodeInfo{ID: uint32(i), Addr: addr})
	}
	return nodes
}

// ── benchmark implementation ──────────────────────────────────────────────────

type PbftGorumsNewBenchmark struct {
	clients          []*pbftclient.Client
	nodes            []pbftserver.NodeInfo // derived from RunOptions.srvAddrs at Init time
	counter          atomic.Uint64
	killPrimaryAfter time.Duration
}

func (b *PbftGorumsNewBenchmark) Init(opts RunOptions) {
	b.nodes = srvAddrsToNodes(opts.srvAddrs)
	b.killPrimaryAfter = opts.killPrimaryAfter
	if len(b.nodes) == 0 {
		panic("pbftnew: no server addresses provided in RunOptions.srvAddrs")
	}
	pbftserver.InitKeys(len(opts.srvAddrs))
	b.clients = make([]*pbftclient.Client, 0, opts.numClients)
	createClients(b, opts)
}

// CreateServer is called by the benchmark framework only in local mode
// (opts.local == true). It starts a server for the given address.
func (b *PbftGorumsNewBenchmark) CreateServer(addr string, srvAddrs []string) (*pbftserver.Server, func(), error) {
	noop := func() {}
	if addr == "" {
		return nil, noop, nil
	}
	nodes := srvAddrsToNodes(srvAddrs)
	var id uint32
	for _, n := range nodes {
		if n.Addr == addr {
			id = n.ID
			break
		}
	}
	if id == 0 {
		return nil, noop, fmt.Errorf("pbftnew: no node found for addr %s in srvAddrs", addr)
	}
	srv, err := pbftserver.StartServer(id, nodes)
	if err != nil {
		return nil, noop, err
	}
	return srv, func() { srv.Stop() }, nil
}

// AddClient is called once per client by createClients. The srvAddrs slice
// carries the server addresses so each client connects to the right cluster.
func (b *PbftGorumsNewBenchmark) AddClient(_ int, _ string, srvAddrs []string, _ *slog.Logger) {
	nodes := srvAddrsToNodes(srvAddrs)
	if len(nodes) == 0 {
		panic("pbftnew: AddClient called with empty srvAddrs")
	}
	c, err := pbftclient.NewClient(nodes)
	if err != nil {
		panic(fmt.Sprintf("pbftnew: failed to create client: %v", err))
	}
	b.clients = append(b.clients, c)
}

func (b *PbftGorumsNewBenchmark) Clients() []*pbftclient.Client {
	return b.clients
}

func (b *PbftGorumsNewBenchmark) Config() *pbftclient.Client {
	return b.clients[0]
}

func (b *PbftGorumsNewBenchmark) Stop() {
	for _, c := range b.clients {
		c.Close()
	}
}

func (b *PbftGorumsNewBenchmark) StartBenchmark(c *pbftclient.Client) []Result {
	if b.killPrimaryAfter > 0 {
		time.AfterFunc(b.killPrimaryAfter, func() {
			c.KillNode(1)
		})
	}
	return nil
}

func (b *PbftGorumsNewBenchmark) StopBenchmark(_ *pbftclient.Client) []Result {
	return nil
}

func (b *PbftGorumsNewBenchmark) Run(c *pbftclient.Client, _ context.Context, _ int) error {
	req := pbftclient.NewRequest(b.counter.Add(1))
	reqCtx, _ := context.WithTimeout(context.Background(), 15*time.Second)
	_, err := pbftclient.Request(c.ActiveCfg(), req, reqCtx)
	return err
}
