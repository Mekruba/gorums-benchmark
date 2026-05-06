package server

import (
	"context"
	"encoding/binary"
	"log/slog"
	"time"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
	"github.com/relab/gorums"
)

const reqTimerTimeout = 2 * time.Second

// Two-stage request timer (Paper §III):
//   first expiry  → forward request to leader
//   second expiry → suspect leader, broadcast STOP

func (s *BFTSmartServer) cancelAllReqTimers() {
	s.reqTimersMu.Lock()
	defer s.reqTimersMu.Unlock()
	for ts, rt := range s.reqTimers {
		rt.timer.Stop()
		delete(s.reqTimers, ts)
	}
}

func (s *BFTSmartServer) startReqTimer(ts int64) {
	if s.leader {
		return
	}
	s.reqTimersMu.Lock()
	defer s.reqTimersMu.Unlock()
	if _, ok := s.reqTimers[ts]; ok {
		return
	}
	rt := &reqTimer{}
	rt.timer = time.AfterFunc(reqTimerTimeout, func() { s.onReqTimerFire(ts) })
	s.reqTimers[ts] = rt
}

func (s *BFTSmartServer) cancelReqTimer(ts int64) {
	s.reqTimersMu.Lock()
	defer s.reqTimersMu.Unlock()
	if rt, ok := s.reqTimers[ts]; ok {
		rt.timer.Stop()
		delete(s.reqTimers, ts)
	}
}

func (s *BFTSmartServer) onReqTimerFire(ts int64) {
	s.reqTimersMu.Lock()
	rt, ok := s.reqTimers[ts]
	if !ok {
		s.reqTimersMu.Unlock()
		return
	}
	if !rt.forwarded {
		rt.forwarded = true
		rt.timer.Reset(reqTimerTimeout)
		s.reqTimersMu.Unlock()
		s.forwardToLeader(ts)
	} else {
		delete(s.reqTimers, ts)
		s.reqTimersMu.Unlock()
		cfg := s.outbound()
		// find which cid this ts belongs to
		entry := s.msgLog.FindByTimestamp(ts)
		var pendingCID uint64
		if entry != nil {
			pendingCID = entry.ConsensusID
		}
		slog.Warn("leader suspected", "node", s.id, "ts", ts, "view", s.view,
			"cfg_size", cfg.Size(), "cfg_nodes", cfg.NodeIDs(),
			"pending_cid", pendingCID)
		s.sendStop()
	}
}

func (s *BFTSmartServer) forwardToLeader(ts int64) {
	req := pb.Request_builder{Operation: pb.Operation_WRITE, Timestamp: ts, ClientId: 1}.Build()
	lid := s.leaderID()
	cfg := s.outbound()
	for _, n := range cfg.Nodes() {
		if n.ID() == lid {
			sig := sign(getPrivKey(s.id), forwardDigest(s.id, ts))
			pb.ForwardRequest(n.Context(context.Background()), pb.ForwardedRequest_builder{
				ForwarderId: s.id, Request: req, Signature: sig,
			}.Build(), gorums.IgnoreErrors())
			return
		}
	}
}

// STOP / SYNC protocol

func (s *BFTSmartServer) sendStop() {
	s.mu.Lock()
	if s.inViewChange {
		s.mu.Unlock()
		return
	}
	newView := s.view + 1
	s.inViewChange = true
	s.mu.Unlock()

	lastCID := s.msgLog.LastStableSeq()

	s.pendingMu.Lock()
	pending := make([]*pb.Request, 0, len(s.pending))
	for ts := range s.pending {
		pending = append(pending, pb.Request_builder{Operation: pb.Operation_WRITE, Timestamp: ts, ClientId: 1}.Build())
	}
	s.pendingMu.Unlock()

	sig := sign(getPrivKey(s.id), stopDigest(newView, lastCID))
	pb.Stop(s.outbound().Context(context.Background()), pb.StopMsg_builder{
		NewView: newView, ReplicaId: s.id, LastConsensusId: lastCID,
		PendingRequests: pending, Signature: sig,
	}.Build(), gorums.IgnoreErrors())

	slog.Info("STOP sent", "node", s.id, "new_view", newView,
		"last_cid", lastCID, "pending", len(pending),
		"outbound", s.outbound().NodeIDs(),
		"outbound_size", s.outbound().Size())
}

func (s *BFTSmartServer) Stop(ctx gorums.ServerCtx, msg *pb.StopMsg) {
	ctx.Release()
	nv := msg.GetNewView()
	from := msg.GetReplicaId()

	slog.Debug("STOP received", "node", s.id, "new_view", nv, "from", from,
		"last_cid", msg.GetLastConsensusId(), "pending", len(msg.GetPendingRequests()))

	if !verifyMsg(from, stopDigest(nv, msg.GetLastConsensusId()), msg.GetSignature()) {
		slog.Warn("STOP sig invalid", "node", s.id, "from", from, "new_view", nv)
		return
	}

	newLeader := s.newLeader(nv)
	slog.Debug("STOP checking leader", "node", s.id, "new_view", nv,
		"new_leader", newLeader, "am_leader", newLeader == s.id)

	if newLeader != s.id {
		return
	}

	s.syncMu.Lock()
	if s.stopMsgs == nil {
		s.stopMsgs = make(map[uint32]*pb.StopMsg)
	}
	s.stopMsgs[from] = msg
	count := len(s.stopMsgs)
	s.syncMu.Unlock()

	slog.Info("STOP counted", "node", s.id, "new_view", nv,
		"from", from, "count", count, "needed", 2*s.f()+1)

	if count >= 2*s.f() {
		s.sendSync(nv)
	}
}

func (s *BFTSmartServer) sendSync(newView uint32) {
	s.syncMu.Lock()
	msgs := make([]*pb.StopMsg, 0, len(s.stopMsgs))
	for _, m := range s.stopMsgs {
		msgs = append(msgs, m)
	}
	s.syncMu.Unlock()

	seen := make(map[int64]bool)
	var union []*pb.Request
	for _, m := range msgs {
		for _, r := range m.GetPendingRequests() {
			if !seen[r.GetTimestamp()] {
				seen[r.GetTimestamp()] = true
				union = append(union, r)
			}
		}
	}

	var maxCID uint64 = 1
	for _, m := range msgs {
		if m.GetLastConsensusId() > maxCID {
			maxCID = m.GetLastConsensusId()
		}
	}
	nextCID := maxCID + 1

	if cur := s.consensusID.Load(); cur+1 > nextCID {
		nextCID = cur + 1
	}

	slog.Debug("SYNC computing", "node", s.id, "new_view", newView,
		"max_cid", maxCID, "next_cid", nextCID,
		"union_pending", len(union), "stop_count", len(msgs))

	sig := sign(getPrivKey(s.id), syncDigest(newView, nextCID))
	pb.Sync(s.outbound().Context(context.Background()), pb.SyncMsg_builder{
		NewView: newView, LeaderId: s.id, StopProofs: msgs,
		PendingRequests: union, NextConsensusId: nextCID, Signature: sig,
	}.Build(), gorums.IgnoreErrors())

	slog.Info("SYNC sent", "node", s.id, "new_view", newView,
		"next_cid", nextCID, "pending", len(union),
		"outbound", s.outbound().NodeIDs())

	s.enterNewView(newView, nextCID, union)
}

func (s *BFTSmartServer) Sync(ctx gorums.ServerCtx, msg *pb.SyncMsg) {
	ctx.Release()
	nv := msg.GetNewView()
	from := msg.GetLeaderId()

	if !verifyMsg(from, syncDigest(nv, msg.GetNextConsensusId()), msg.GetSignature()) {
		slog.Warn("SYNC sig invalid", "node", s.id, "from", from, "new_view", nv)
		return
	}
	if from != s.newLeader(nv) {
		slog.Warn("SYNC from wrong leader", "node", s.id,
			"from", from, "expected", s.newLeader(nv), "new_view", nv)
		return
	}

	// new leader already entered new view in sendSync — skip self delivery
	if from == s.id {
		slog.Debug("SYNC self delivery ignored", "node", s.id, "new_view", nv)
		return
	}

	slog.Info("SYNC accepted", "node", s.id, "new_view", nv,
		"next_cid", msg.GetNextConsensusId())
	s.enterNewView(nv, msg.GetNextConsensusId(), msg.GetPendingRequests())
}

func (s *BFTSmartServer) enterNewView(newView uint32, nextCID uint64, pending []*pb.Request) {
	s.mu.Lock()
	oldView := s.view
	s.view = newView
	s.leader = (s.id == s.newLeader(newView))
	s.inViewChange = false
	s.mu.Unlock()

	s.syncMu.Lock()
	s.stopMsgs = make(map[uint32]*pb.StopMsg)
	s.syncMu.Unlock()

	for {
		cur := s.consensusID.Load()
		if cur >= nextCID-1 {
			break
		}
		if s.consensusID.CompareAndSwap(cur, nextCID-1) {
			break
		}
	}

	s.msgLog.ResetUncommitted(nextCID)

	slog.Info("entered new view", "node", s.id,
		"old_view", oldView, "new_view", newView,
		"leader", s.leader, "next_cid", nextCID,
		"pending_to_requeue", len(pending))

	if s.leader {
		seen := make(map[int64]bool)
		requeued := 0

		// check inbound for newly joining nodes — enqueue RECONFIG as first batch
		// so membership is updated atomically through consensus before client requests.
		if s.getInboundConfig != nil {
			inbound := s.getInboundConfig()
			for _, n := range inbound.Nodes() {
				id := n.ID()
				s.mu.Lock()
				known := s.originalNodes[id]
				s.mu.Unlock()
				if known {
					continue
				}
				addr := n.Address()
				slog.Info("new node detected, enqueuing RECONFIG",
					"node", s.id, "target_id", id, "target_addr", addr)
				payload := make([]byte, 4+len(addr))
				binary.BigEndian.PutUint32(payload[:4], id)
				copy(payload[4:], addr)
				reconfigReq := pb.Request_builder{
					Operation: pb.Operation_RECONFIG,
					Timestamp: int64(id), // unique enough — node IDs are stable
					Payload:   payload,
				}.Build()
				select {
				case s.reqQueue <- reconfigReq:
					requeued++
					seen[int64(id)] = true
				default:
					slog.Warn("reqQueue full, dropping RECONFIG", "node", s.id, "target", id)
				}
			}
		}

		// first add from STOP union
		for _, r := range pending {
			if !seen[r.GetTimestamp()] {
				seen[r.GetTimestamp()] = true
				select {
				case s.reqQueue <- r:
					requeued++
				default:
					slog.Warn("reqQueue full, dropping pending request",
						"node", s.id, "ts", r.GetTimestamp())
				}
			}
		}

		// then add from local pending map, skipping already seen
		s.pendingMu.Lock()
		for ts := range s.pending {
			if !seen[ts] {
				seen[ts] = true
				select {
				case s.reqQueue <- pb.Request_builder{
					Operation: pb.Operation_WRITE, Timestamp: ts, ClientId: 1,
				}.Build():
					requeued++
				default:
					slog.Warn("reqQueue full, dropping pending request",
						"node", s.id, "ts", ts)
				}
			}
		}
		s.pendingMu.Unlock()

		slog.Info("leader requeued requests", "node", s.id,
			"new_view", newView, "requeued", requeued)
		go s.runLeader()
	}
}

func (s *BFTSmartServer) newLeader(view uint32) uint32 {
	return view%uint32(s.clusterSize) + 1
}

// ViewUpdate handles a membership change notification decided through consensus.
// Gorums handles the actual connections; we just mutate the outbound config.

func (s *BFTSmartServer) ViewUpdate(ctx gorums.ServerCtx, msg *pb.ViewUpdateMsg) {
	ctx.Release()
	nv := msg.GetNewView()
	if !verifyMsg(msg.GetLeaderId(), viewUpdateDigest(nv, msg.GetTargetId(), msg.GetConsensusId()), msg.GetSignature()) {
		return
	}

	cfg := s.outbound()
	switch msg.GetAction() {
	case pb.MembershipAction_JOIN:
		s.SetOutboundConfig(cfg.Add(msg.GetTargetId()))
		s.mu.Lock()
		s.clusterSize++
		s.view = nv
		s.mu.Unlock()
	case pb.MembershipAction_LEAVE:
		s.SetOutboundConfig(cfg.Remove(msg.GetTargetId()))
		s.mu.Lock()
		s.clusterSize--
		s.view = nv
		s.mu.Unlock()
	}

	slog.Info("view update applied", "node", s.id, "new_view", nv,
		"action", msg.GetAction(), "target", msg.GetTargetId())
}
