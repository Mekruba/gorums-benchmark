package client

import (
	"context"
	"log"
	"log/slog"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
)

// Client wraps a gorums configuration.
// allCfg holds all known nodes (active + standbys) — the manager knows
// about all of them from startup so they are reachable when they come online.
// activeCfg is the current reachable subset used for requests, kept up to
// date by the watcher goroutine.
type Client struct {
	allCfg      gorums.Configuration
	activeCfg   gorums.Configuration
	cancelWatch context.CancelFunc
}

// ActiveCfg returns the current reachable configuration for sending requests.
func (c *Client) ActiveCfg() gorums.Configuration {
	return c.activeCfg
}

func (c *Client) Node(id uint32) *gorums.Node {
	for _, n := range c.allCfg.Nodes() {
		if n.ID() == id {
			return n
		}
	}
	return nil
}

func (c *Client) Close() {
	if c.cancelWatch != nil {
		c.cancelWatch()
	}
	c.allCfg.Close()
}

// NewClient creates a client connected to all known nodes (active + standbys).
// A watcher goroutine keeps activeCfg in sync with reachability automatically —
// dead nodes drop out and new nodes appear as their errors clear.
func NewClient(nodes []server.NodeInfo) (*Client, error) {
	peerMap := make(map[uint32]server.NodeAddr)
	for _, n := range nodes {
		peerMap[n.ID] = server.NodeAddr{Addr_: n.Addr}
	}
	allCfg, err := gorums.NewConfig(
		gorums.WithNodes(peerMap),
		gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, err
	}

	watchCtx, cancelWatch := context.WithCancel(context.Background())
	c := &Client{
		allCfg:      allCfg,
		activeCfg:   reachable(allCfg),
		cancelWatch: cancelWatch,
	}
	go c.watchConfig(watchCtx)
	return c, nil
}

// KillNode sends a kill signal to the node with the given ID.
// Used by the benchmark framework to simulate crash failures.
// Fire-and-forget — no response expected since the node exits immediately.
func (c *Client) KillNode(id uint32) {
	node := c.Node(id)
	if node == nil {
		slog.Warn("KillNode: node not found", "id", id)
		return
	}
	slog.Info("killing node", "id", id)
	nodeCtx := node.Context(context.Background())
	pb.Kill(nodeCtx, &emptypb.Empty{}, gorums.IgnoreErrors())
}

// watchConfig keeps activeCfg current by watching allCfg for reachability changes.
// Dead nodes drop out when LastErr() appears; nodes that recover or newly connect
// appear automatically when their errors clear.
func (c *Client) watchConfig(ctx context.Context) {
	ch := c.allCfg.Watch(ctx, 2*time.Second,
		func(cfg gorums.Configuration) gorums.Configuration {
			return reachable(cfg)
		})

	for newCfg := range ch {
		slog.Info("client config update",
			"size", newCfg.Size(),
			"ids", newCfg.NodeIDs(),
		)
		c.activeCfg = newCfg
	}
}

// reachable returns the subset of cfg where LastErr() == nil.
func reachable(cfg gorums.Configuration) gorums.Configuration {
	healthy := make(gorums.Configuration, 0, cfg.Size())
	for _, n := range cfg.Nodes() {
		if n.LastErr() == nil {
			healthy = append(healthy, n)
		}
	}
	return healthy
}

// RunClient sends a single request and logs the reply. Used by the CLI.
func RunClient(nodes []server.NodeInfo) {
	c, err := NewClient(nodes)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer c.Close()

	req := pb.Request_builder{
		Operation: pb.Operation_WRITE,
		Timestamp: time.Now().UnixNano(),
		ClientId:  42,
	}.Build()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	reply, err := Request(c.ActiveCfg(), req, ctx)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	log.Printf("Got reply: view=%d replica=%d result=%s",
		reply.GetView(), reply.GetReplicaId(), reply.GetResult())
}

// NewRequest builds a Request proto with a unique counter as timestamp.
func NewRequest(counter uint64) *pb.Request {
	return pb.Request_builder{
		Operation: pb.Operation_WRITE,
		Timestamp: int64(counter),
		ClientId:  1,
	}.Build()
}

// Request sends a PBFT client request and waits for f+1 matching replies.
// Uses the active reachable config so f is computed from healthy nodes only.
func Request(cfg gorums.Configuration, req *pb.Request, ctx context.Context) (*pb.Reply, error) {
	f := (cfg.Size() - 1) / 3
	cfgCtx := cfg.Context(ctx)
	reply, err := pb.ClientRequest(cfgCtx, req).Threshold(f + 1)
	if err != nil {
		slog.Warn("request incomplete",
			"ts", req.GetTimestamp(),
			"needed", f+1,
			"err", err,
		)
	}
	return reply, err
}
