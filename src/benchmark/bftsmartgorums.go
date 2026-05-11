package bench

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	bftclient "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/client"
	bftserver "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/server"
)

// srvAddrsToBFTNodes converts the index-keyed address slice used by the
// benchmark framework into the NodeInfo slice expected by the BFT-Smart
// client/server packages. Index 0 is unused and skipped.
func srvAddrsToBFTNodes(srvAddrs []string) []bftserver.NodeInfo {
	nodes := make([]bftserver.NodeInfo, 0, len(srvAddrs))
	for i, addr := range srvAddrs {
		if addr == "" {
			continue
		}
		nodes = append(nodes, bftserver.NodeInfo{ID: uint32(i), Addr: addr})
	}
	return nodes
}

// ── benchmark implementation ──────────────────────────────────────────────────

type BFTSmartBenchmark struct {
	clients          []*bftclient.Client
	nodes            []bftserver.NodeInfo
	counter          atomic.Uint64
	killPrimaryAfter time.Duration
}

func (b *BFTSmartBenchmark) Init(opts RunOptions) {
	b.nodes = srvAddrsToBFTNodes(opts.srvAddrs)
	b.killPrimaryAfter = opts.killPrimaryAfter
	if len(b.nodes) == 0 {
		panic("bftsmart: no server addresses provided in RunOptions.srvAddrs")
	}
	bftserver.InitKeys(len(opts.srvAddrs))
	b.clients = make([]*bftclient.Client, 0, opts.numClients)
	createClients(b, opts)
}

// CreateServer is called by the benchmark framework only in local mode.
func (b *BFTSmartBenchmark) CreateServer(addr string, srvAddrs []string) (*bftserver.Server, func(), error) {
	nodes := srvAddrsToBFTNodes(srvAddrs)
	var id uint32
	for _, n := range nodes {
		if n.Addr == addr {
			id = n.ID
			break
		}
	}
	if id == 0 {
		return nil, nil, fmt.Errorf("bftsmart: no node found for addr %s in srvAddrs", addr)
	}
	srv, err := bftserver.StartServer(id, nodes)
	if err != nil {
		return nil, nil, err
	}
	return srv, func() { srv.Stop() }, nil
}

// AddClient is called once per client by createClients.
func (b *BFTSmartBenchmark) AddClient(_ int, _ string, srvAddrs []string, _ *slog.Logger) {
	nodes := srvAddrsToBFTNodes(srvAddrs)
	if len(nodes) == 0 {
		panic("bftsmart: AddClient called with empty srvAddrs")
	}
	c, err := bftclient.NewClient(nodes)
	if err != nil {
		panic(fmt.Sprintf("bftsmart: failed to create client: %v", err))
	}
	b.clients = append(b.clients, c)
}

func (b *BFTSmartBenchmark) Clients() []*bftclient.Client {
	return b.clients
}

func (b *BFTSmartBenchmark) Config() *bftclient.Client {
	return b.clients[0]
}

func (b *BFTSmartBenchmark) Stop() {
	for _, c := range b.clients {
		c.Close()
	}
}

func (b *BFTSmartBenchmark) StartBenchmark(c *bftclient.Client) []Result {
	if b.killPrimaryAfter > 0 {
		time.AfterFunc(b.killPrimaryAfter, func() {
			c.KillNode(1) // node 1 is always leader at view 0
		})
	}
	return nil
}

func (b *BFTSmartBenchmark) StopBenchmark(_ *bftclient.Client) []Result {
	return nil
}

func (b *BFTSmartBenchmark) Run(c *bftclient.Client, _ context.Context, _ int) error {
	req := bftclient.NewRequest(b.counter.Add(1))
	reqCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := bftclient.Request(c.ActiveCfg(), req, reqCtx)
	return err
}
