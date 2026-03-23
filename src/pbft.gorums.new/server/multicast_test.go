package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type countingServer struct {
	received atomic.Uint64
}

func (s *countingServer) ClientRequest(ctx gorums.ServerCtx, req *pb.Request) (*pb.Reply, error) {
	return nil, nil
}
func (s *countingServer) PrePrepare(ctx gorums.ServerCtx, req *pb.PrePrepareMsg) {
	s.received.Add(1)
}
func (s *countingServer) Prepare(ctx gorums.ServerCtx, req *pb.PrepareMsg) {
	s.received.Add(1)
}
func (s *countingServer) Commit(ctx gorums.ServerCtx, req *pb.CommitMsg)     {}
func (s *countingServer) Benchmark(ctx gorums.ServerCtx, req *emptypb.Empty) {}

func TestMulticastDelivery(t *testing.T) {
	const n = 4
	const messages = 1000

	nodes := []NodeInfo{
		{ID: 1, Addr: "localhost:19080"},
		{ID: 2, Addr: "localhost:19081"},
		{ID: 3, Addr: "localhost:19082"},
		{ID: 4, Addr: "localhost:19083"},
	}

	servers := make([]*countingServer, n)
	systems := make([]*gorums.System, n)

	peerMap := make(map[uint32]NodeAddr)
	for _, node := range nodes {
		peerMap[node.ID] = NodeAddr{Addr_: node.Addr}
	}
	peerList := gorums.WithNodes(peerMap)

	// start all systems
	for i, node := range nodes {
		srv := &countingServer{}
		servers[i] = srv
		sys, err := gorums.NewSystem(node.Addr,
			gorums.WithConfig(node.ID, peerList),
			gorums.WithReceiveBufferSize(1024),
			peerList,
			gorums.WithDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
		if err != nil {
			t.Fatal(err)
		}
		mgr := pb.NewManager(gorums.WithDialOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		))
		sys.RegisterService(mgr, func(gs *gorums.Server) {
			pb.RegisterPBFTServer(gs, srv)
		})
		go sys.Serve()
		systems[i] = sys
		defer sys.Stop()
	}

	time.Sleep(2 * time.Second)

	// send messages from node 1 only
	cfgCtx := systems[0].OutboundConfig().Context(context.Background())
	for i := 0; i < messages; i++ {
		pb.Prepare(cfgCtx, pb.PrepareMsg_builder{
			Sequence:  uint64(i),
			ReplicaId: 1,
		}.Build())
	}

	time.Sleep(1 * time.Second)

	// each node should receive exactly messages times
	for i, srv := range servers {
		got := srv.received.Load()
		if got != messages {
			t.Errorf("node %d: expected %d prepares, got %d", i+1, messages, got)
		}
	}
}
