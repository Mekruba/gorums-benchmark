package client

import (
	"context"
	"log"
	"log/slog"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
	"github.com/Mekruba/gorums-benchmark/bft-smart.gorums/server"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Client struct {
	allCfg      gorums.Configuration
	activeCfg   gorums.Configuration
	cancelWatch context.CancelFunc
}

func (c *Client) ActiveCfg() gorums.Configuration { return c.activeCfg }

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

func NewClient(nodes []server.NodeInfo) (*Client, error) {
	peers := make(map[uint32]server.NodeAddr)
	for _, n := range nodes {
		peers[n.ID] = server.NodeAddr{Addr_: n.Addr}
	}
	cfg, err := gorums.NewConfig(
		gorums.WithNodes(peers),
		gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, err
	}

	_, cancel := context.WithCancel(context.Background())
	c := &Client{allCfg: cfg, activeCfg: reachable(cfg), cancelWatch: cancel}
	// go c.watch(ctx)
	return c, nil
}

func (c *Client) KillNode(id uint32) {
	n := c.Node(id)
	if n == nil {
		return
	}
	pb.Kill(n.Context(context.Background()), &emptypb.Empty{}, gorums.IgnoreErrors())
}

func (c *Client) watch(ctx context.Context) {
	ch := c.allCfg.Watch(ctx, 2*time.Second, func(cfg gorums.Configuration) gorums.Configuration {
		return reachable(cfg)
	})
	for fresh := range ch {
		slog.Info("client config update", "size", fresh.Size(), "ids", fresh.NodeIDs())
		c.activeCfg = fresh
	}
}

func reachable(cfg gorums.Configuration) gorums.Configuration {
	out := make(gorums.Configuration, 0, cfg.Size())
	for _, n := range cfg.Nodes() {
		err := n.LastErr()
		if err == nil {
			out = append(out, n)
			continue
		}
		code := status.Code(err)
		if code != codes.Unavailable && code != codes.Internal {
			out = append(out, n) // transient error, keep the node
		}
	}
	return out
}

func NewRequest(counter uint64) *pb.Request {
	return pb.Request_builder{Operation: pb.Operation_WRITE, Timestamp: int64(counter), ClientId: 1}.Build()
}

func Request(cfg gorums.Configuration, req *pb.Request, ctx context.Context) (*pb.Reply, error) {
	f := (cfg.Size() - 1) / 3
	return pb.ClientRequest(cfg.Context(ctx), req).Threshold(f + 1)
}

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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	reply, err := Request(c.ActiveCfg(), req, ctx)
	if err != nil {
		log.Fatalf("request failed: %v", err)
	}
	log.Printf("reply: view=%d replica=%d result=%s",
		reply.GetView(), reply.GetReplicaId(), reply.GetResult())
}
