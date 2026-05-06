package server

import (
	"context"
	"encoding/binary"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
	"github.com/relab/gorums"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BFTSmartServer struct {
	id          uint32
	view        uint32
	leader      bool
	clusterSize int
	consensusID atomic.Uint64

	mu sync.Mutex // protects view, leader, inViewChange

	pendingMu sync.Mutex
	pending   map[int64]chan<- *pb.Reply
	delivered map[int64]*pb.Reply

	deliverCh chan *ConsensusEntry
	msgLog    *MessageLog
	reqQueue  chan *pb.Request

	batchMax     int
	batchTimeout time.Duration

	received  atomic.Uint64
	committed atomic.Uint64

	outboundCfg gorums.Configuration

	// sync phase state (used by sync.go)
	syncMu       sync.Mutex
	syncTimer    *time.Timer
	stopMsgs     map[uint32]*pb.StopMsg
	inViewChange bool

	// request timers (used by sync.go)
	reqTimersMu sync.Mutex
	reqTimers   map[int64]*reqTimer

	leaderRunning atomic.Bool

	// reconfiguration support
	getInboundConfig func() gorums.Configuration // returns sys.Config()
	originalNodes    map[uint32]bool             // node IDs present at startup
}

type reqTimer struct {
	timer     *time.Timer
	forwarded bool
}

func NewBFTSmartServer(id uint32, clusterSize int) *BFTSmartServer {
	s := &BFTSmartServer{
		id:           id,
		leader:       id == 1,
		clusterSize:  clusterSize,
		pending:      make(map[int64]chan<- *pb.Reply),
		delivered:    make(map[int64]*pb.Reply),
		msgLog:       NewMessageLog(),
		reqQueue:     make(chan *pb.Request, 10000),
		deliverCh:    make(chan *ConsensusEntry, 2000),
		batchMax:     1000,
		batchTimeout: 5 * time.Millisecond,
		reqTimers:    make(map[int64]*reqTimer),
	}
	if s.leader {
		go s.runLeader()
	}
	go s.runDeliver()
	return s
}

// Leader batching loop. Blocks until at least one request arrives, then
// collects up to batchMax requests or until batchTimeout elapses.
// (Paper §III: "BFT-SMART fills the batch with pending requests until
// (a) its size reaches a limit or (b) it has no requests left to add.")

func (s *BFTSmartServer) runLeader() {
	if !s.leaderRunning.CompareAndSwap(false, true) {
		return
	}
	defer s.leaderRunning.Store(false)

	for {
		first, ok := <-s.reqQueue
		if !ok {
			return
		}
		batch := []*pb.Request{first}

		deadline := time.After(s.batchTimeout)
	fill:
		for len(batch) < s.batchMax {
			select {
			case req, ok := <-s.reqQueue:
				if !ok {
					break fill
				}
				batch = append(batch, req)
			case <-deadline:
				break fill
			}
		}

		s.proposeBatch(batch)
	}
}

func (s *BFTSmartServer) proposeBatch(batch []*pb.Request) {
	cid := s.consensusID.Add(1)

	if cid > s.msgLog.HighWaterMark() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		s.msgLog.WaitForWaterMark(ctx, cid)
		cancel()
	}

	bd := batchDigest(batch)
	// timestamps := extractTimestamps(batch)
	// s.recordPropose(cid, timestamps, batch, bd)

	cfg := s.outbound()
	sig := sign(getPrivKey(s.id), proposeDigest(cid, s.view, bd))
	pb.Propose(cfg.Context(context.Background()), pb.ProposeMsg_builder{
		ConsensusId: cid,
		View:        s.view,
		LeaderId:    s.id,
		Batch:       batch,
		BatchDigest: bd,
		Signature:   sig,
	}.Build(), gorums.IgnoreErrors())

	slog.Debug("propose sent", "node", s.id, "cid", cid, "batch_size", len(batch))
}

// RPC handlers

func (s *BFTSmartServer) ClientRequest(ctx gorums.ServerCtx, request *pb.Request) (*pb.Reply, error) {
	ts := request.GetTimestamp()
	s.received.Add(1)
	// RECONFIG is a system operation — do not start a request timer
	if request.GetOperation() != pb.Operation_RECONFIG {
		s.startReqTimer(ts)
	}

	s.pendingMu.Lock()
	if reply, ok := s.delivered[ts]; ok {
		delete(s.delivered, ts)
		s.pendingMu.Unlock()
		return reply, nil
	}
	ch := make(chan *pb.Reply, 1)
	s.pending[ts] = ch
	s.pendingMu.Unlock()

	if s.leader {
		select {
		case s.reqQueue <- request:
		case <-ctx.Done():
			s.removePending(ts)
			return nil, context.Canceled
		}
	}

	ctx.Release()

	select {
	case reply := <-ch:
		s.cancelReqTimer(ts)
		return reply, nil
	case <-ctx.Done():
		grace := time.NewTimer(time.Second)
		defer grace.Stop()
		select {
		case reply := <-ch:
			s.cancelReqTimer(ts)
			return reply, nil
		case <-grace.C:
			s.removePending(ts)
			return nil, context.Canceled
		}
	}
}

func (s *BFTSmartServer) UnorderedRequest(_ gorums.ServerCtx, request *pb.Request) (*pb.Reply, error) {
	return pb.Reply_builder{
		View:      s.view,
		ReplicaId: s.id,
		Timestamp: request.GetTimestamp(),
		ClientId:  request.GetClientId(),
		Result:    []byte("ok"),
	}.Build(), nil
}

func (s *BFTSmartServer) ForwardRequest(ctx gorums.ServerCtx, request *pb.ForwardedRequest) {
	if !s.leader {
		return
	}
	from := request.GetForwarderId()
	if !verifyMsg(from, forwardDigest(from, request.GetRequest().GetTimestamp()), request.GetSignature()) {
		slog.Warn("ForwardRequest sig invalid", "node", s.id, "from", from)
		return
	}
	ctx.Release()
	select {
	case s.reqQueue <- request.GetRequest():
	default:
		slog.Warn("reqQueue full, dropping forwarded request", "node", s.id)
	}
}

func (s *BFTSmartServer) Propose(ctx gorums.ServerCtx, msg *pb.ProposeMsg) {
	if s.isInViewChange() {
		slog.Debug("Propose dropped: in view change", "node", s.id, "cid", msg.GetConsensusId())
		return
	}
	cid := msg.GetConsensusId()
	expectedLeader := s.leaderID()
	if !verifyMsg(expectedLeader, proposeDigest(cid, msg.GetView(), msg.GetBatchDigest()), msg.GetSignature()) {
		slog.Warn("Propose sig invalid", "node", s.id, "cid", cid,
			"expected_leader", expectedLeader, "view", msg.GetView())
		return
	}
	if !s.msgLog.WithinWaterMarks(cid) {
		slog.Warn("Propose outside water marks", "node", s.id, "cid", cid,
			"low_wm", s.msgLog.LowWaterMark(), "high_wm", s.msgLog.HighWaterMark())
		return
	}

	slog.Debug("Propose received", "node", s.id, "cid", cid,
		"view", msg.GetView(), "batch_size", len(msg.GetBatch()))

	timestamps := extractTimestamps(msg.GetBatch())
	if s.recordPropose(cid, timestamps, msg.GetBatch(), msg.GetBatchDigest()) {
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			slog.Warn("Propose: nil cfgCtx", "node", s.id, "cid", cid)
			return
		}
		bd := msg.GetBatchDigest()
		sig := sign(getPrivKey(s.id), writeDigest(cid, msg.GetView(), bd))
		ctx.Release()
		slog.Debug("Write sent", "node", s.id, "cid", cid, "view", msg.GetView())
		pb.Write(cfgCtx, pb.WriteMsg_builder{
			ConsensusId: cid, View: msg.GetView(), ReplicaId: s.id,
			BatchDigest: bd, Signature: sig,
		}.Build(), gorums.IgnoreErrors())

		if s.recheckWritten(cid) {
			sig = sign(getPrivKey(s.id), acceptDigest(cid, msg.GetView(), bd))
			slog.Debug("Accept sent (recheck)", "node", s.id, "cid", cid)
			pb.Accept(cfgCtx, pb.AcceptMsg_builder{
				ConsensusId: cid, View: msg.GetView(), ReplicaId: s.id,
				BatchDigest: bd, Signature: sig,
			}.Build(), gorums.IgnoreErrors())
			if s.recheckAccepted(cid) {
				s.deliver(s.msgLog.GetOrCreate(cid))
			}
		}
	} else {
		slog.Debug("Propose dropped: recordPropose returned false",
			"node", s.id, "cid", cid)
	}
}

func (s *BFTSmartServer) Write(ctx gorums.ServerCtx, msg *pb.WriteMsg) {
	if s.isInViewChange() {
		slog.Debug("Write dropped: in view change", "node", s.id, "cid", msg.GetConsensusId())
		return
	}
	cid := msg.GetConsensusId()
	if !verifyMsg(msg.GetReplicaId(), writeDigest(cid, msg.GetView(), msg.GetBatchDigest()), msg.GetSignature()) {
		slog.Warn("Write sig invalid", "node", s.id, "cid", cid, "from", msg.GetReplicaId())
		return
	}
	if !s.msgLog.WithinWaterMarks(cid) {
		slog.Warn("Write outside water marks", "node", s.id, "cid", cid,
			"low_wm", s.msgLog.LowWaterMark(), "high_wm", s.msgLog.HighWaterMark())
		return
	}

	slog.Debug("Write received", "node", s.id, "cid", cid,
		"from", msg.GetReplicaId(), "view", msg.GetView())

	if s.recordWrite(cid, msg.GetView(), msg.GetReplicaId(), msg.GetBatchDigest()) {
		slog.Debug("Write: quorum reached, sending Accept", "node", s.id, "cid", cid)
		cfgCtx := ctx.ConfigContext()
		if cfgCtx == nil {
			slog.Warn("Write: nil cfgCtx", "node", s.id, "cid", cid)
			return
		}
		sig := sign(getPrivKey(s.id), acceptDigest(cid, msg.GetView(), msg.GetBatchDigest()))
		ctx.Release()
		slog.Debug("Accept sent", "node", s.id, "cid", cid, "view", msg.GetView())
		pb.Accept(cfgCtx, pb.AcceptMsg_builder{
			ConsensusId: cid, View: msg.GetView(), ReplicaId: s.id,
			BatchDigest: msg.GetBatchDigest(), Signature: sig,
		}.Build(), gorums.IgnoreErrors())
	} else {
		slog.Debug("Write: recordWrite returned false",
			"node", s.id, "cid", cid, "from", msg.GetReplicaId())
	}
}

func (s *BFTSmartServer) Accept(ctx gorums.ServerCtx, msg *pb.AcceptMsg) {
	if s.isInViewChange() {
		slog.Debug("Accept dropped: in view change", "node", s.id, "cid", msg.GetConsensusId())
		return
	}
	cid := msg.GetConsensusId()
	if !verifyMsg(msg.GetReplicaId(), acceptDigest(cid, msg.GetView(), msg.GetBatchDigest()), msg.GetSignature()) {
		slog.Warn("Accept sig invalid", "node", s.id, "cid", cid, "from", msg.GetReplicaId())
		return
	}
	if !s.msgLog.WithinWaterMarks(cid) {
		slog.Warn("Accept outside water marks", "node", s.id, "cid", cid,
			"low_wm", s.msgLog.LowWaterMark(), "high_wm", s.msgLog.HighWaterMark())
		return
	}

	slog.Debug("Accept received", "node", s.id, "cid", cid,
		"from", msg.GetReplicaId(), "view", msg.GetView())

	if s.recordAccept(cid, msg.GetView(), msg.GetReplicaId(), msg.GetBatchDigest()) {
		slog.Debug("Accept quorum reached, delivering", "node", s.id, "cid", cid)
		s.deliver(s.msgLog.GetOrCreate(cid))
	} else {
		slog.Debug("Accept: recordAccept returned false",
			"node", s.id, "cid", cid, "from", msg.GetReplicaId())
	}
}

// Ordered delivery

func (s *BFTSmartServer) deliver(e *ConsensusEntry) {
	s.deliverCh <- e
}

func (s *BFTSmartServer) runDeliver() {
	nextCID := s.consensusID.Load() + 1
	buf := make(map[uint64]*ConsensusEntry)

	for e := range s.deliverCh {
		buf[e.ConsensusID] = e
		for {
			if lwm := s.msgLog.LowWaterMark(); nextCID <= lwm {
				for k := range buf {
					if k <= lwm {
						delete(buf, k)
					}
				}
				nextCID = lwm + 1
			}
			entry, ok := buf[nextCID]
			if !ok {
				break
			}
			delete(buf, nextCID)
			s.executeEntry(entry)
			nextCID++
		}
	}
}

func (s *BFTSmartServer) executeEntry(e *ConsensusEntry) {
	e.Executed = true
	s.committed.Add(1)

	// advance consensusID on every replica so STOP last_cid is correct
	for {
		cur := s.consensusID.Load()
		if cur >= e.ConsensusID {
			break
		}
		if s.consensusID.CompareAndSwap(cur, e.ConsensusID) {
			break
		}
	}

	clientCount := 0
	if e.Batch != nil {
		// normal path — Propose was received, full batch is available
		for _, req := range e.Batch {
			if req.GetOperation() == pb.Operation_RECONFIG {
				s.applyReconfig(req)
				continue
			}
			ts := req.GetTimestamp()
			reply := pb.Reply_builder{
				View: s.view, ReplicaId: s.id, Timestamp: ts, Result: []byte("ok"),
			}.Build()
			s.pendingMu.Lock()
			ch, ok := s.pending[ts]
			if ok {
				delete(s.pending, ts)
				s.pendingMu.Unlock()
				ch <- reply
			} else {
				s.delivered[ts] = reply
				s.pendingMu.Unlock()
			}
			s.cancelReqTimer(ts)
			clientCount++
		}
	} else if len(e.BatchTimestamps) > 0 {
		// Propose arrived after delivery — timestamps known, batch content unknown
		for _, ts := range e.BatchTimestamps {
			reply := pb.Reply_builder{
				View: s.view, ReplicaId: s.id, Timestamp: ts, Result: []byte("ok"),
			}.Build()
			s.pendingMu.Lock()
			ch, ok := s.pending[ts]
			if ok {
				delete(s.pending, ts)
				s.pendingMu.Unlock()
				ch <- reply
			} else {
				s.delivered[ts] = reply
				s.pendingMu.Unlock()
			}
			s.cancelReqTimer(ts)
			clientCount++
		}
	} else {
		// Propose never received (joined mid-stream) — cancel all pending
		// timers since the batch is committed and any timer is now stale.
		slog.Debug("executeEntry: no batch info, cancelling all timers",
			"node", s.id, "cid", e.ConsensusID)
		s.cancelAllReqTimers()
	}

	slog.Info("batch committed", "node", s.id, "cid", e.ConsensusID,
		"batch_size", len(e.Batch), "client_reqs", clientCount,
		"committed", s.committed.Load(), "received", s.received.Load())

	if s.msgLog.ShouldCheckpoint(e.ConsensusID) {
		s.sendCheckpoint(e.ConsensusID)
	}
}

// applyReconfig applies a committed RECONFIG request. The payload encodes
// the joining node ID (4 bytes) followed by the address as a string.
// All replicas call this at the same consensus point so membership is
// updated identically on every node.
func (s *BFTSmartServer) applyReconfig(req *pb.Request) {
	payload := req.GetPayload()
	if len(payload) < 4 {
		slog.Warn("applyReconfig: payload too short", "node", s.id, "len", len(payload))
		return
	}
	targetID := binary.BigEndian.Uint32(payload[:4])
	targetAddr := string(payload[4:])

	current := s.outbound()
	updated := current.Add(targetID)
	// if err != nil {
	// 	slog.Warn("applyReconfig: Extend failed", "node", s.id, "target", targetID, "err", err)
	// 	return
	// }
	s.SetOutboundConfig(updated)
	s.mu.Lock()
	s.clusterSize = updated.Size()
	if s.originalNodes != nil {
		s.originalNodes[targetID] = true
	}
	s.mu.Unlock()

	// unblock the View Manager client that submitted this request
	ts := req.GetTimestamp()
	reply := pb.Reply_builder{
		View: s.view, ReplicaId: s.id, Timestamp: ts, Result: []byte("ok"),
	}.Build()
	s.pendingMu.Lock()
	ch, ok := s.pending[ts]
	if ok {
		delete(s.pending, ts)
		s.pendingMu.Unlock()
		ch <- reply
	} else {
		s.delivered[ts] = reply
		s.pendingMu.Unlock()
	}

	slog.Info("reconfig applied: JOIN", "node", s.id,
		"target_id", targetID, "target_addr", targetAddr,
		"cluster_size", updated.Size(), "config", updated.NodeIDs())
}

// Log mutation helpers

func (s *BFTSmartServer) recordPropose(cid uint64, timestamps []int64, batch []*pb.Request, bd []byte) bool {
	e := s.msgLog.GetOrCreate(cid)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.SentWrite {
		slog.Debug("recordPropose: SentWrite already true",
			"node", s.id, "cid", cid,
			"written", e.Written, "accepted", e.Accepted,
			"propose_nil", e.Propose == nil,
			"writes_count", len(e.Writes))
		return false
	}
	e.View = s.view
	e.Batch = batch
	e.BatchTimestamps = timestamps
	e.SentWrite = true
	e.Propose = &ProposeRecord{
		ConsensusID: cid, View: s.view, LeaderID: s.leaderID(),
		BatchDigest: bd, Batch: batch,
	}
	if e.Writes == nil {
		e.Writes = make(map[uint32]*VoteRecord)
	}
	e.Writes[s.id] = &VoteRecord{ConsensusID: cid, View: s.view, BatchDigest: bd, ReplicaID: s.id}
	return true
}

func (s *BFTSmartServer) recordWrite(cid uint64, view, replicaID uint32, bd []byte) bool {
	e := s.msgLog.GetOrCreate(cid)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Writes == nil {
		e.Writes = make(map[uint32]*VoteRecord)
	}
	if _, dup := e.Writes[replicaID]; dup {
		return false
	}
	e.Writes[replicaID] = &VoteRecord{ConsensusID: cid, View: view, BatchDigest: bd, ReplicaID: replicaID}
	result := e.CheckWritten(s.f())
	slog.Debug("recordWrite result", "node", s.id, "cid", cid,
		"from", replicaID, "check_written", result,
		"writes_count", len(e.Writes))
	return result
}

func (s *BFTSmartServer) recordAccept(cid uint64, view, replicaID uint32, bd []byte) bool {
	e := s.msgLog.GetOrCreate(cid)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Accepts == nil {
		e.Accepts = make(map[uint32]*VoteRecord)
	}
	if _, dup := e.Accepts[replicaID]; dup {
		return false
	}
	e.Accepts[replicaID] = &VoteRecord{ConsensusID: cid, View: view, BatchDigest: bd, ReplicaID: replicaID}
	return e.CheckAccepted(s.f())
}

func (s *BFTSmartServer) recheckWritten(cid uint64) bool {
	e := s.msgLog.GetOrCreate(cid)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.CheckWritten(s.f())
}

func (s *BFTSmartServer) recheckAccepted(cid uint64) bool {
	e := s.msgLog.GetOrCreate(cid)
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.CheckAccepted(s.f())
}

// Utility methods

func (s *BFTSmartServer) isInViewChange() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inViewChange
}

func (s *BFTSmartServer) leaderID() uint32 {
	return s.view%uint32(s.clusterSize) + 1
}

func (s *BFTSmartServer) f() int { return (s.clusterSize - 1) / 3 }

func (s *BFTSmartServer) SetOutboundConfig(cfg gorums.Configuration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outboundCfg = cfg
}

func (s *BFTSmartServer) outbound() gorums.Configuration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.outboundCfg
}

func (s *BFTSmartServer) removePending(ts int64) {
	s.pendingMu.Lock()
	delete(s.pending, ts)
	s.pendingMu.Unlock()
}

func (s *BFTSmartServer) Ping(_ gorums.ServerCtx, _ *emptypb.Empty) (*emptypb.Empty, error) {
	return &emptypb.Empty{}, nil
}

func (s *BFTSmartServer) Kill(_ gorums.ServerCtx, _ *emptypb.Empty) {
	slog.Info("kill received", "node", s.id)
	os.Exit(0)
}

func (s *BFTSmartServer) Benchmark(ctx gorums.ServerCtx, _ *emptypb.Empty) {
	ctx.Release()
	s.msgLog.Reset()
	s.pendingMu.Lock()
	s.pending = make(map[int64]chan<- *pb.Reply)
	s.delivered = make(map[int64]*pb.Reply)
	s.view = 0
	s.pendingMu.Unlock()
	for len(s.reqQueue) > 0 {
		<-s.reqQueue
	}
	for len(s.deliverCh) > 0 {
		<-s.deliverCh
	}
	s.received.Store(0)
	s.committed.Store(0)
}

// Pure helpers

func extractTimestamps(batch []*pb.Request) []int64 {
	ts := make([]int64, len(batch))
	for i, r := range batch {
		ts[i] = r.GetTimestamp()
	}
	return ts
}
