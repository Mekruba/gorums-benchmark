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

	// Record our own vote before multicasting — gorums multicast
	// does not deliver to self so we count ourselves explicitly.
	p.recordCheckpoint(seq, p.id, digest)

	sig := sign(getPrivKey(p.id), checkpointDigest(seq, p.id, digest))
	pb.Checkpoint(cfg.Context(context.Background()), pb.CheckpointMsg_builder{
		Sequence:  seq,
		Digest:    digest,
		ReplicaId: p.id,
		Signature: sig,
	}.Build(), gorums.IgnoreErrors())
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

	p.recordCheckpoint(seq, fromID, digest)
}

// recordCheckpoint records a checkpoint vote and logs if it becomes stable.
func (p *PBFTServer) recordCheckpoint(seq uint64, replicaID uint32, digest []byte) {
	stable, gcCount := p.msgLog.RecordCheckpoint(seq, replicaID, digest, p.f())
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
