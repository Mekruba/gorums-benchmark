package server

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"log/slog"

	pb "github.com/Mekruba/gorums-benchmark/pbft.gorums.new/proto"
	"github.com/relab/gorums"
)

// stateDigest computes a deterministic digest representing the service state
// at the given committed sequence number. Since this is a benchmark with no
// real state, we use a SHA-256 hash of the sequence number — all honest nodes
// will produce the same digest for the same sequence.
func stateDigest(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	digest := sha256.Sum256(b)
	return digest[:]
}

// sendCheckpoint multicasts a CHECKPOINT message to all replicas after
// executing a request at a checkpoint sequence number (seq % K == 0).
// Called from deliver in pbft.go.
func (p *PBFTServer) sendCheckpoint(seq uint64) {
	cfg := p.outbound()
	// if cfg == nil {
	// 	slog.Warn("checkpoint skipped — outbound config not set yet",
	// 		"node", p.id, "seq", seq)
	// 	return
	// }

	digest := stateDigest(seq)
	slog.Debug("checkpoint sent", "node", p.id, "seq", seq, "peers", cfg.NodeIDs())

	sig := sign(getPrivKey(p.id), checkpointDigest(seq, p.id, digest))
	selfMsg := pb.CheckpointMsg_builder{
		Sequence:  seq,
		Digest:    digest,
		ReplicaId: p.id,
		Signature: sig,
	}.Build()

	// Record our own vote before multicasting — gorums multicast
	// does not deliver to self so we count ourselves explicitly.
	p.recordCheckpoint(seq, p.id, digest, selfMsg)

	pb.Checkpoint(cfg.Context(context.Background()), selfMsg, gorums.IgnoreErrors())
}

// Checkpoint handles an incoming CHECKPOINT message.
// Once 2f+1 matching checkpoints arrive for the same seq and digest,
// the checkpoint becomes stable and garbage collection runs.
func (p *PBFTServer) Checkpoint(ctx gorums.ServerCtx, request *pb.CheckpointMsg) {
	seq := request.GetSequence()
	if !verifyMsg(request.GetReplicaId(), checkpointDigest(request.GetSequence(), request.GetReplicaId(), request.GetDigest()), request.GetSignature()) {
		slog.Warn("Checkpoint signature invalid", "node", p.id, "seq", seq, "from", request.GetReplicaId())
		return
	}
	ctx.Release()

	digest := request.GetDigest()
	fromID := request.GetReplicaId()
	slog.Debug("checkpoint received", "node", p.id, "seq", seq, "from", fromID)

	p.recordCheckpoint(seq, fromID, digest, request)
}

// SendStateTransfer is called by a standby replica to pull state from the
// cluster. It signs the request, sends it as a quorum call, verifies the
// response signature, then replays the proof messages through RecordCheckpoint
// to advance the local MessageLog to the stable checkpoint.
func (p *PBFTServer) SendStateTransfer() error {
	cfg := p.outbound()
	cfgCtx := cfg.Context(context.Background())

	sig := sign(getPrivKey(p.id), stateTransferRequestDigest(p.id))
	req := pb.StateTransferRequest_builder{
		ReplicaId: p.id,
		Signature: sig,
	}.Build()

	resp, err := pb.StateTransfer(cfgCtx, req).Threshold(1)
	if err != nil {
		return err
	}

	fromID := resp.GetReplicaId()
	if !verifyMsg(fromID, stateTransferResponseDigest(resp.GetLastStableSeq(), resp.GetView(), fromID), resp.GetSignature()) {
		slog.Warn("StateTransfer response signature invalid", "node", p.id, "from", fromID)
		return nil
	}

	// Replay the 2f+1 proof messages through the normal checkpoint path so
	// the MessageLog advances its water marks exactly as any live replica would.
	for _, msg := range resp.GetCheckpointProof() {
		if !verifyMsg(msg.GetReplicaId(), checkpointDigest(msg.GetSequence(), msg.GetReplicaId(), msg.GetDigest()), msg.GetSignature()) {
			slog.Warn("StateTransfer: proof message signature invalid",
				"node", p.id, "from", msg.GetReplicaId(), "seq", msg.GetSequence())
			continue
		}
		p.recordCheckpoint(msg.GetSequence(), msg.GetReplicaId(), msg.GetDigest(), msg)
	}

	p.mu.Lock()
	p.view = resp.GetView()
	p.mu.Unlock()

	slog.Info("state transfer complete",
		"node", p.id,
		"last_stable_seq", resp.GetLastStableSeq(),
		"view", resp.GetView(),
		"low_wm", p.msgLog.LowWaterMark(),
		"high_wm", p.msgLog.HighWaterMark(),
	)
	return nil
}
func (p *PBFTServer) recordCheckpoint(seq uint64, replicaID uint32, digest []byte, msg *pb.CheckpointMsg) {
	stable, gcCount := p.msgLog.RecordCheckpoint(seq, replicaID, digest, msg, p.f())
	if stable {
		if replicaID == p.id {
			slog.Info("checkpoint stable (self-vote)",
				"node", p.id,
				"seq", seq,
				"low_wm", p.msgLog.LowWaterMark(),
				"high_wm", p.msgLog.HighWaterMark(),
				"gc_entries", gcCount,
			)
		} else {
			slog.Info("checkpoint stable",
				"node", p.id,
				"seq", seq,
				"replica", replicaID,
				"low_wm", p.msgLog.LowWaterMark(),
				"high_wm", p.msgLog.HighWaterMark(),
				"gc_entries", gcCount,
			)
		}
	}
}

// StateTransfer handles an incoming STATE-TRANSFER request from a standby
// replica that wants to join the cluster. We respond with the last stable
// checkpoint sequence number, the current water marks and view, and the
// 2f+1 CheckpointMsg proof set so the standby can replay them through its
// own RecordCheckpoint path and advance its log exactly as any live replica
// would have done. (Section 4.4 of Castro & Liskov: a joining replica obtains
// the stable checkpoint from any f+1 replicas that certified it.)
func (p *PBFTServer) StateTransfer(ctx gorums.ServerCtx, request *pb.StateTransferRequest) (*pb.StateTransferResponse, error) {
	fromID := request.GetReplicaId()

	if !verifyMsg(fromID, stateTransferRequestDigest(fromID), request.GetSignature()) {
		slog.Warn("StateTransfer request signature invalid", "node", p.id, "from", fromID)
		return nil, nil
	}

	slog.Info("state transfer request", "node", p.id, "from", fromID)

	seq, proof := p.msgLog.StableCheckpointProof()

	p.mu.Lock()
	view := p.view
	p.mu.Unlock()

	sig := sign(getPrivKey(p.id), stateTransferResponseDigest(seq, view, p.id))

	resp := pb.StateTransferResponse_builder{
		LastStableSeq:   seq,
		LowWaterMark:    p.msgLog.LowWaterMark(),
		HighWaterMark:   p.msgLog.HighWaterMark(),
		View:            view,
		CheckpointProof: proof,
		ReplicaId:       p.id,
		Signature:       sig,
	}.Build()

	slog.Info("state transfer response sent",
		"node", p.id, "to", fromID,
		"last_stable_seq", seq, "proof_msgs", len(proof))
	return resp, nil
}
