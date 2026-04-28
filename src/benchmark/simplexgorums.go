package bench

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
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

// SimplexGorumsBenchmark measures Simplex consensus performance and optionally
// triggers a membership reconfiguration mid-run.
//
// To benchmark reconfiguration:
//
//	b := &SimplexGorumsBenchmark{}
//	b.SetInitialMembers([]uint32{1, 2, 3, 4})          // start with 4 nodes
//	b.SetReconfig(15*time.Second, []uint32{1,2,3,4,5,6,7}) // expand to 7 after 15 s
type SimplexGorumsBenchmark struct {
	clients         []*simplexserver.Client
	nodes           []simplexserver.NodeInfo
	counter         atomic.Uint64
	initialMembers  []uint32      // nil = all nodes active
	reconfigAfter   time.Duration // 0 = no reconfig
	reconfigMembers []uint32
}

// SetInitialMembers configures which node IDs form the initial consensus set.
// All nodes in the server address list must still be started (they listen and
// hold connections), but only the listed IDs participate in proposals/votes/finalizes
// until a reconfiguration tx changes the membership.
// Must be called before Init.
func (b *SimplexGorumsBenchmark) SetInitialMembers(members []uint32) {
	b.initialMembers = members
}

// SetReconfig schedules a membership reconfiguration to be triggered after
// afterDur from the start of the benchmark run. The reconfig tx is submitted
// to the first available in-process server (local mode) or via the HTTP
// /reconfigure endpoint (docker/VM mode). afterDur must be less than the
// total benchmark duration to take effect.
func (b *SimplexGorumsBenchmark) SetReconfig(afterDur time.Duration, newMembers []uint32) {
	b.reconfigAfter = afterDur
	b.reconfigMembers = newMembers
}

func (b *SimplexGorumsBenchmark) CreateServer(addr string, srvAddrs []string) (*simplexserver.Server, func(), error) {
	noop := func() {}
	// The benchmark framework passes a 1-based slice where index 0 is always "".
	// Skip it silently so the deferred cleanup in the framework is never nil.
	if addr == "" {
		return nil, noop, nil
	}
	nodes := simplexSrvAddrsToNodes(srvAddrs)
	var id uint32
	for _, n := range nodes {
		if n.Addr == addr {
			id = n.ID
			break
		}
	}
	if id == 0 {
		return nil, noop, fmt.Errorf("simplex: unknown addr %s", addr)
	}
	var srv *simplexserver.Server
	var err error
	if len(b.initialMembers) > 0 {
		srv, err = simplexserver.StartServerWithInitialMembers(id, nodes, b.initialMembers)
	} else {
		srv, err = simplexserver.StartServer(id, nodes)
	}
	if err != nil {
		return nil, noop, err
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
	anyLocal := false
	for _, n := range b.nodes {
		if srv := simplexserver.Lookup(n.Addr); srv != nil {
			anyLocal = true
			time.Sleep(1500 * time.Millisecond) // let gorums connections settle
			srv.StartProtocol()
		}
	}

	if !anyLocal {
		// Docker / VM mode: poll the first node's HTTP endpoint until it
		// responds, then let the other nodes a moment to catch up.
		b.waitForServersReady(30 * time.Second)
	}

	// Fire the reconfiguration at the configured time if requested.
	if b.reconfigAfter > 0 && len(b.reconfigMembers) > 0 {
		go b.triggerReconfigAfter(b.reconfigAfter, b.reconfigMembers)
	}

	return nil
}

// waitForServersReady polls the HTTP /sync endpoint of the first node until it
// responds (meaning the server has started its protocol and its HTTP listener
// is up). Gives up after timeout and continues anyway.
func (b *SimplexGorumsBenchmark) waitForServersReady(timeout time.Duration) {
	if len(b.nodes) == 0 {
		return
	}
	n := b.nodes[0]
	host, portStr, err := net.SplitHostPort(n.Addr)
	if err != nil {
		return
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return
	}
	url := fmt.Sprintf("http://%s:%d/sync", host, port+100)
	cl := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := cl.Get(url)
		if err == nil {
			resp.Body.Close()
			slog.Info("simplex: servers ready, starting benchmark")
			time.Sleep(500 * time.Millisecond) // let all nodes finish startup
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	slog.Warn("simplex: timed out waiting for servers, proceeding anyway")
}

// triggerReconfigAfter sleeps for d then submits a reconfiguration tx to the
// cluster. It tries in-process servers first (local mode), then falls back to
// the HTTP /reconfigure endpoint via the first client (docker/VM mode).
func (b *SimplexGorumsBenchmark) triggerReconfigAfter(d time.Duration, members []uint32) {
	time.Sleep(d)
	slog.Info("simplex: triggering reconfiguration", "members", members)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Local mode: submit directly to first available in-process server.
	for _, n := range b.nodes {
		if srv := simplexserver.Lookup(n.Addr); srv != nil {
			if err := srv.ReconfigureAndWait(ctx, members); err != nil {
				slog.Warn("simplex: reconfig failed", "err", err)
			} else {
				slog.Info("simplex: reconfiguration committed", "members", members)
			}
			return
		}
	}

	// Docker/VM mode: submit via the HTTP client of the first node.
	if len(b.clients) > 0 && len(b.nodes) > 0 {
		if err := b.clients[0].Reconfigure(ctx, members, b.nodes[0].ID); err != nil {
			slog.Warn("simplex: remote reconfig failed", "err", err)
		} else {
			slog.Info("simplex: reconfiguration committed", "members", members)
		}
	}
}

func (b *SimplexGorumsBenchmark) StopBenchmark(_ *simplexserver.Client) []Result {
	return nil
}

// Run submits one transaction to the cluster and waits for it to be finalized.
func (b *SimplexGorumsBenchmark) Run(c *simplexserver.Client, ctx context.Context, _ int) error {
	seq := b.counter.Add(1)
	tx := fmt.Sprintf("bm.%d", seq)

	idx := int(seq-1) % len(b.nodes)

	// Local mode: find an in-process server and submit directly.
	for i := range b.nodes {
		tryAddr := b.nodes[(idx+i)%len(b.nodes)].Addr
		srv := simplexserver.Lookup(tryAddr)
		if srv != nil {
			return srv.AddTxAndWait(ctx, tx)
		}
	}

	// Docker/VM mode: submit all txs to node 1. In Simplex only the current
	// leader can propose, so round-robining across all nodes causes txs to
	// queue on non-leader nodes until they rotate into the leader slot,
	// leading to timeouts at high throughput.  Concentrating txs on one node
	// ensures they get proposed whenever that node is leader (~every 7th
	// block) with the entire backlog in a single proposal.
	return c.Submit(ctx, tx, b.nodes[0].ID)
}
