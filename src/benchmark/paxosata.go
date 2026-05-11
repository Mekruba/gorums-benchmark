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
	local   bool // true when servers are spawned in-process by the benchmark framework
}

func (*PaxosATABenchmark) CreateServer(addr string, srvAddrs []string) (*paxosataServer.PaxosServer, func(), error) {
	srv := paxosataServer.New(addr, srvAddrs)
	srv.Start(true)
	return srv, func() {
		srv.Stop()
	}, nil
}

func (b *PaxosATABenchmark) Init(opts RunOptions) {
	b.local = opts.local
	b.clients = make([]*paxosataClient.Client, 0, opts.numClients)
	createClients(b, opts)
	// In local mode the servers haven't been created yet (runBenchmark does that
	// after Init returns), so warmup would accumulate gRPC backoff and prevent
	// connections from being established in time. Warmup is deferred to
	// StartBenchmark, which is called after the servers are running.
	if !opts.local {
		warmupFunc(b.clients, b.warmup)
	}
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
	// Filter empty addresses (index 0 is always "" in the 1-based slice built by
	// main.go) to avoid creating a gorums node for ":0" that inflates quorum size.
	var filtered []string
	for _, a := range srvAddrs {
		if a != "" {
			filtered = append(filtered, a)
		}
	}
	qSize := 1 + len(filtered)/2
	b.clients = append(b.clients, paxosataClient.New(id, addr, filtered, qSize, logger))
}

func (*PaxosATABenchmark) warmup(client *paxosataClient.Client) {
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err := client.Write(ctx, "warmup")
		cancel()
		if err == nil {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (b *PaxosATABenchmark) StartBenchmark(_ *paxosataClient.Client) []Result {
	// In local mode the servers have just been started by runBenchmark; run
	// warmup now so gRPC connections are established before the timed window.
	if b.local {
		warmupFunc(b.clients, b.warmup)
	}
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
