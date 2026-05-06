package client

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/paxos.ata/proto"
	"github.com/relab/gorums"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Client is a Multi-Paxos proposer.
type Client struct {
	mu                  sync.Mutex
	id                  uint32
	cfg                 pb.Configuration
	proposalNum         uint32
	preparedInstances   map[uint32]bool
	freshInstances      map[uint32]bool
	nextInstance        uint32
	stride              uint32
	highestSeenInstance uint32
}

// New creates a proposer client connecting to srvAddresses.
func New(id int, _ string, srvAddresses []string, _ int, _ *slog.Logger) *Client {
	cfg, err := pb.NewConfig(
		gorums.WithNodeList(srvAddresses),
		gorums.WithDialOptions(
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		),
	)
	if err != nil {
		panic(fmt.Sprintf("paxosata client: config error: %v", err))
	}
	uid := uint32(id + 1)
	return &Client{
		id:                uid,
		cfg:               cfg,
		proposalNum:       uid * 100000,
		preparedInstances: make(map[uint32]bool),
		freshInstances:    make(map[uint32]bool),
		nextInstance:      uid,
		stride:            1,
	}
}

// SetStride configures disjoint instance sets so concurrent clients don't collide.
func (c *Client) SetStride(startInstance, stride uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.nextInstance = startInstance
	c.stride = stride
	c.preparedInstances = make(map[uint32]bool)
	c.freshInstances = make(map[uint32]bool)
	c.highestSeenInstance = 0
}

// Stop closes the client's connection.
func (c *Client) Stop() {
	c.cfg.Close() //nolint:errcheck
}

// Write proposes value and returns when consensus is reached.
func (c *Client) Write(ctx context.Context, value string) error {
	return c.propose(value)
}

// propose runs all three Paxos phases with retry on rejection.
func (c *Client) propose(value string) error {
	const maxRetries = 8

	c.mu.Lock()
	instance := c.nextInstance
	c.nextInstance += c.stride
	c.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*attempt) * 2 * time.Millisecond)
			c.mu.Lock()
			c.proposalNum += uint32(attempt * 50000)
			delete(c.preparedInstances, instance)
			c.mu.Unlock()
		}
		if lastErr = c.tryPropose(instance, value); lastErr == nil {
			return nil
		}
	}
	return fmt.Errorf("paxosata: gave up after %d attempts on instance %d: %w", maxRetries, instance, lastErr)
}

func (c *Client) tryPropose(instance uint32, value string) error {
	c.mu.Lock()
	c.proposalNum++
	currentProposal := c.proposalNum
	skipPhase1 := c.preparedInstances[instance] ||
		(c.highestSeenInstance > 0 &&
			instance == c.highestSeenInstance+c.stride &&
			c.preparedInstances[c.highestSeenInstance] &&
			c.freshInstances[c.highestSeenInstance])
	c.mu.Unlock()

	var valueToPropose string

	if !skipPhase1 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		cfgCtx := c.cfg.Context(ctx)
		responses := pb.Prepare(cfgCtx, pb.PrepareRequest_builder{
			Instance:    instance,
			ProposalNum: currentProposal,
			ProposerId:  c.id,
		}.Build())

		var promiseCount int
		var highestAcceptedNum uint32
		var highestAcceptedValue string
		for resp := range responses.Seq() {
			if resp.Err != nil {
				continue
			}
			if resp.Value.GetPromised() {
				promiseCount++
				if resp.Value.GetAcceptedProposalNum() > highestAcceptedNum {
					highestAcceptedNum = resp.Value.GetAcceptedProposalNum()
					highestAcceptedValue = resp.Value.GetAcceptedValue()
				}
			}
		}

		quorumSize := int(c.cfg.Size())/2 + 1
		if promiseCount < quorumSize {
			return fmt.Errorf("phase1: got %d promises, need %d", promiseCount, quorumSize)
		}

		c.mu.Lock()
		c.preparedInstances[instance] = true
		if instance > c.highestSeenInstance {
			c.highestSeenInstance = instance
		}
		if highestAcceptedValue != "" {
			valueToPropose = highestAcceptedValue
		} else {
			valueToPropose = value
			c.freshInstances[instance] = true
		}
		c.mu.Unlock()
	} else {
		c.mu.Lock()
		c.freshInstances[instance] = true
		c.preparedInstances[instance] = true
		if instance > c.highestSeenInstance {
			c.highestSeenInstance = instance
		}
		c.mu.Unlock()
		valueToPropose = value
	}

	// Phase 2: Accept
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	cfgCtx2 := c.cfg.Context(ctx2)
	accepteds := pb.Accept(cfgCtx2, pb.AcceptRequest_builder{
		Instance:    instance,
		ProposalNum: currentProposal,
		Value:       valueToPropose,
		ProposerId:  c.id,
	}.Build())

	quorumSize := int(c.cfg.Size())/2 + 1
	var acceptCount int
	for accepted := range accepteds.Seq() {
		if accepted.Err != nil {
			continue
		}
		if accepted.Value.GetAccepted() {
			acceptCount++
		}
	}
	if acceptCount < quorumSize {
		c.mu.Lock()
		delete(c.preparedInstances, instance)
		c.mu.Unlock()
		return fmt.Errorf("phase2: got %d acceptances, need %d", acceptCount, quorumSize)
	}

	// Phase 3: Learn
	ctx3, cancel3 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel3()

	cfgCtx3 := c.cfg.Context(ctx3)
	learned := pb.Learn(cfgCtx3, pb.LearnRequest_builder{
		Instance:    instance,
		ProposalNum: currentProposal,
		Value:       valueToPropose,
		ProposerId:  c.id,
	}.Build())
	for range learned.Seq() {
		// drain
	}

	return nil
}
