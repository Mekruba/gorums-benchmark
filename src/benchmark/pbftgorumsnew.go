package bench

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	pbftclient "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/client"
	pbftserver "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
)

var pbftNewNodes = []pbftserver.NodeInfo{
	{ID: 1, Addr: "localhost:8080"},
	{ID: 2, Addr: "localhost:8081"},
	{ID: 3, Addr: "localhost:8082"},
	{ID: 4, Addr: "localhost:8083"},
}

// ── benchmark implementation ──────────────────────────────────────────────────

type PbftGorumsNewBenchmark struct {
	clients []*pbftclient.Client
	counter atomic.Uint64
}

func (b *PbftGorumsNewBenchmark) CreateServer(addr string, _ []string) (*pbftserver.Server, func(), error) {
	var id uint32
	for _, n := range pbftNewNodes {
		if n.Addr == addr {
			id = n.ID
			break
		}
	}
	if id == 0 {
		return nil, nil, fmt.Errorf("pbftnew: unknown addr %s", addr)
	}
	srv, err := pbftserver.StartServer(id, pbftNewNodes)
	if err != nil {
		return nil, nil, err
	}
	return srv, func() { srv.Stop() }, nil
}

func (b *PbftGorumsNewBenchmark) Init(opts RunOptions) {
	b.clients = make([]*pbftclient.Client, 0, opts.numClients)
	createClients(b, opts)
}

func (b *PbftGorumsNewBenchmark) AddClient(_ int, _ string, _ []string, _ *slog.Logger) {
	c, err := pbftclient.NewClient(pbftNewNodes)
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

func (b *PbftGorumsNewBenchmark) StartBenchmark(_ *pbftclient.Client) []Result {
	return nil
}

func (b *PbftGorumsNewBenchmark) StopBenchmark(_ *pbftclient.Client) []Result {
	return nil
}

func (b *PbftGorumsNewBenchmark) Run(c *pbftclient.Client, ctx context.Context, _ int) error {
	req := pbftclient.NewRequest(b.counter.Add(1))
	_, err := pbftclient.Request(c.Cfg, req, ctx)
	return err
}
