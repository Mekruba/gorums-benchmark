package server

import (
	"net"
	"sync"

	pb "github.com/Mekruba/gorums-benchmark/paxos.ata/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// instanceState holds per-Paxos-instance acceptor state.
type instanceState struct {
	promisedProposalNum uint32
	acceptedProposalNum uint32
	acceptedValue       string
	learnedValue        string
}

// PaxosServer is an acceptor+learner for Multi-Paxos.
type PaxosServer struct {
	id        uint32
	addr      string
	srv       *gorums.Server
	mu        sync.Mutex
	instances map[uint32]*instanceState
	stopped   bool
}

// New creates a PaxosServer. addr is this server's listen address;
// srvAddrs is the full list of server addresses (used to assign an ID).
func New(addr string, srvAddrs []string) *PaxosServer {
	var id uint32
	for i, a := range srvAddrs {
		if a == addr {
			id = uint32(i + 1)
			break
		}
	}
	srv := gorums.NewServer(
		gorums.WithGRPCServerOptions(
			grpc.Creds(insecure.NewCredentials()),
		),
	)
	s := &PaxosServer{
		id:        id,
		addr:      addr,
		srv:       srv,
		instances: make(map[uint32]*instanceState),
	}
	pb.RegisterPaxosServer(srv, s)
	return s
}

// Start begins serving in a background goroutine.
func (s *PaxosServer) Start(_ bool) {
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		panic("paxosata: listen: " + err.Error())
	}
	go s.srv.Serve(lis) //nolint:errcheck
}

// Stop shuts down the server.
func (s *PaxosServer) Stop() {
	if s.stopped {
		return
	}
	s.stopped = true
	s.srv.Stop()
}

// Reset clears all Paxos instance state for a fresh benchmark run.
func (s *PaxosServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.instances = make(map[uint32]*instanceState)
}

func (s *PaxosServer) getInstance(instance uint32) *instanceState {
	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.instances[instance]; ok {
		return st
	}
	st := &instanceState{}
	s.instances[instance] = st
	return st
}

// Prepare handles Phase 1a: Proposer asks acceptors to promise.
func (s *PaxosServer) Prepare(ctx gorums.ServerCtx, req *pb.PrepareRequest) (*pb.PromiseResponse, error) {
	state := s.getInstance(req.GetInstance())
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetProposalNum() > state.promisedProposalNum {
		state.promisedProposalNum = req.GetProposalNum()
		return pb.PromiseResponse_builder{
			AcceptorId:          s.id,
			Instance:            req.GetInstance(),
			Promised:            true,
			AcceptedProposalNum: state.acceptedProposalNum,
			AcceptedValue:       state.acceptedValue,
		}.Build(), nil
	}
	return pb.PromiseResponse_builder{
		AcceptorId: s.id,
		Instance:   req.GetInstance(),
		Promised:   false,
	}.Build(), nil
}

// Accept handles Phase 2a: Proposer asks acceptors to accept a value.
func (s *PaxosServer) Accept(ctx gorums.ServerCtx, req *pb.AcceptRequest) (*pb.AcceptedResponse, error) {
	state := s.getInstance(req.GetInstance())
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.GetProposalNum() >= state.promisedProposalNum {
		state.promisedProposalNum = req.GetProposalNum()
		state.acceptedProposalNum = req.GetProposalNum()
		state.acceptedValue = req.GetValue()
		return pb.AcceptedResponse_builder{
			AcceptorId:  s.id,
			Instance:    req.GetInstance(),
			Accepted:    true,
			ProposalNum: req.GetProposalNum(),
		}.Build(), nil
	}
	return pb.AcceptedResponse_builder{
		AcceptorId:  s.id,
		Instance:    req.GetInstance(),
		Accepted:    false,
		ProposalNum: state.promisedProposalNum,
	}.Build(), nil
}

// Learn handles Phase 3: Proposer notifies learners of chosen value.
func (s *PaxosServer) Learn(ctx gorums.ServerCtx, req *pb.LearnRequest) (*pb.LearnResponse, error) {
	state := s.getInstance(req.GetInstance())
	s.mu.Lock()
	defer s.mu.Unlock()
	if state.learnedValue == "" {
		state.learnedValue = req.GetValue()
	}
	return pb.LearnResponse_builder{
		LearnerId: s.id,
		Instance:  req.GetInstance(),
		Learned:   true,
	}.Build(), nil
}
