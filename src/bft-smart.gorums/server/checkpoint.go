package server

import (
	"context"
	"log/slog"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
	"github.com/relab/gorums"
)

func (s *BFTSmartServer) sendCheckpoint(cid uint64) {
	cfg := s.outbound()
	digest := stateDigest(cid)
	sig := sign(getPrivKey(s.id), checkpointDigest(cid, digest))

	self := pb.CheckpointMsg_builder{
		ConsensusId: cid, StateDigest: digest,
		ReplicaId: s.id, Signature: sig,
	}.Build()

	s.recordCheckpoint(cid, s.id, digest, self)
	pb.Checkpoint(cfg.Context(context.Background()), self, gorums.IgnoreErrors())
}

func (s *BFTSmartServer) Checkpoint(ctx gorums.ServerCtx, msg *pb.CheckpointMsg) {
	cid := msg.GetConsensusId()
	if !verifyMsg(msg.GetReplicaId(), checkpointDigest(cid, msg.GetStateDigest()), msg.GetSignature()) {
		return
	}
	ctx.Release()
	s.recordCheckpoint(cid, msg.GetReplicaId(), msg.GetStateDigest(), msg)
}

func (s *BFTSmartServer) recordCheckpoint(cid uint64, replicaID uint32, digest []byte, msg *pb.CheckpointMsg) {
	stable, gc := s.msgLog.RecordCheckpoint(cid, replicaID, digest, msg, s.f())
	if stable {
		slog.Info("checkpoint stable", "node", s.id, "cid", cid,
			"low_wm", s.msgLog.LowWaterMark(), "high_wm", s.msgLog.HighWaterMark(), "gc", gc)
	}
}

// State transfer — pull side (recovering replica calls this)

func (s *BFTSmartServer) SendStateTransfer() error {
	cfg := s.outbound()
	lastCID := s.msgLog.LastStableSeq()
	sig := sign(getPrivKey(s.id), stateTransferReqDigest(s.id, lastCID))

	resp, err := pb.StateTransfer(cfg.Context(context.Background()), pb.StateTransferRequest_builder{
		ReplicaId: s.id, LastConsensusId: lastCID, Signature: sig,
	}.Build()).Threshold(1)
	if err != nil {
		return err
	}

	from := resp.GetReplicaId()
	if !verifyMsg(from, stateTransferRespDigest(resp.GetLastConsensusId(), resp.GetView(), from), resp.GetSignature()) {
		slog.Warn("state transfer response sig invalid", "node", s.id, "from", from)
		return nil
	}

	for _, m := range resp.GetCheckpointProof() {
		if verifyMsg(m.GetReplicaId(), checkpointDigest(m.GetConsensusId(), m.GetStateDigest()), m.GetSignature()) {
			s.recordCheckpoint(m.GetConsensusId(), m.GetReplicaId(), m.GetStateDigest(), m)
		}
	}

	s.mu.Lock()
	s.view = resp.GetView()
	s.mu.Unlock()

	slog.Info("state transfer done", "node", s.id,
		"last_cid", resp.GetLastConsensusId(), "view", resp.GetView())
	return nil
}

// State transfer — serve side (live replica responds to a recovering peer)

func (s *BFTSmartServer) StateTransfer(_ gorums.ServerCtx, req *pb.StateTransferRequest) (*pb.StateTransferResponse, error) {
	from := req.GetReplicaId()
	if !verifyMsg(from, stateTransferReqDigest(from, req.GetLastConsensusId()), req.GetSignature()) {
		return nil, nil
	}

	cid, proof := s.msgLog.StableCheckpointProof()
	s.mu.Lock()
	view := s.view
	s.mu.Unlock()

	sig := sign(getPrivKey(s.id), stateTransferRespDigest(cid, view, s.id))
	return pb.StateTransferResponse_builder{
		LastConsensusId: cid, CheckpointId: cid,
		View: view, CheckpointProof: proof,
		ReplicaId: s.id, PartIndex: 0, TotalParts: 1,
		Signature: sig,
	}.Build(), nil
}
