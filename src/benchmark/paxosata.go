package bench

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	paxosataClient "github.com/Mekruba/gorums-benchmark/paxos.ata/client"
	paxosataServer "github.com/Mekruba/gorums-benchmark/paxos.ata/server"
)

// PaxosATABenchmark implements Benchmark[paxosataServer.PaxosServer, paxosataClient.Client]
// using the gorums-ata Multi-Paxos protocol with the new gorums v0.11 API.
type PaxosATABenchmark struct {
	clients []*paxosataClient.Client
}

func (*PaxosATABenchmark) CreateServer(addr string, srvAddrs []string) (*paxosataServer.PaxosServer, func(), error) {
	srv := paxosataServer.New(addr, srvAddrs)
	srv.Start(true)
	return srv, func() {
		srv.Stop()
	}, nil
}

func (b *PaxosATABenchmark) Init(opts RunOptions) {
	b.clients = make([]*paxosataClient.Client, 0, opts.numClients)
	createClients(b, opts)
	warmupFunc(b.clients, b.warmup)
	b.setStrides()
}

// setStrides assigns each client a disjoint set of Paxos instance numbers.
func (b *PaxosATABenchmark) setStrides() {
	n := uint32(len(b.clients))
	for i, c := range b.clients {
		c.SetStride(uint32(i)+1, n)
	}
}

func (b *PaxosATABenchmark) Clients() []*paxosataClient.Client {
	return b.clients
}

func (b *PaxosATABenchmark) Config() *paxosataClient.Client {
	return b.clients[0]
}

func (b *PaxosATABenchmark) Stop() {
	for _, c := range b.clients {
		c.Stop()
	}
}

func (b *PaxosATABenchmark) AddClient(id int, addr string, srvAddrs []string, logger *slog.Logger) {
	qSize := 1 + len(srvAddrs)/2
	b.clients = append(b.clients, paxosataClient.New(id, addr, srvAddrs, qSize, logger))
}

func (*PaxosATABenchmark) warmup(client *paxosataClient.Client) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = ctx
	client.Write(context.Background(), "warmup") //nolint:errcheck
}

func (b *PaxosATABenchmark) StartBenchmark(_ *paxosataClient.Client) []Result {
	// Reset client strides so each run starts from a clean slate.
	b.setStrides()
	return nil
}

func (*PaxosATABenchmark) StopBenchmark(_ *paxosataClient.Client) []Result {
	return nil
}

func (*PaxosATABenchmark) Run(client *paxosataClient.Client, ctx context.Context, val int) error {
	return client.Write(ctx, strconv.Itoa(val))
}
