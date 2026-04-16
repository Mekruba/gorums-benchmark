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

	"github.com/Mekruba/gorums-benchmark/pbft.gorums.new/server"
)

// Client wraps a gorums configuration.
type Client struct {
	Cfg gorums.Configuration
}

func (c *Client) Close() {
	c.Cfg.Close()
}

// NewClient creates a client connected to the given nodes.
func NewClient(nodes []server.NodeInfo) (*Client, error) {
	peerMap := make(map[uint32]server.NodeAddr)
	for _, n := range nodes {
		peerMap[n.ID] = server.NodeAddr{Addr_: n.Addr}
	}
	cfg, err := gorums.NewConfig(
		gorums.WithNodes(peerMap),
		gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		return nil, err
	}
	return &Client{Cfg: cfg}, nil
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

	reply, err := Request(c.Cfg, req, ctx)
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
