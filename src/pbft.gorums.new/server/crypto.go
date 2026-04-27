package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
)

// ── Key management ────────────────────────────────────────────────────────────

type NodeKeys struct {
	PrivKey ed25519.PrivateKey
	PubKey  ed25519.PublicKey
}

func GenerateKeys() (NodeKeys, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return NodeKeys{}, err
	}
	return NodeKeys{PrivKey: priv, PubKey: pub}, nil
}

// ── Digest helpers ────────────────────────────────────────────────────────────

// requestDigest computes SHA256(timestamp) — deterministic across all nodes
// for the same client request since all nodes receive the same timestamp.
func requestDigest(timestamp int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(timestamp))
	d := sha256.Sum256(b)
	return d[:]
}

// prePrepareDigest computes the digest signed in a PrePrepare message.
// Covers: view + sequence + request digest.
func prePrepareDigest(view uint32, seq uint64, digest []byte) []byte {
	b := make([]byte, 4+8+len(digest))
	binary.BigEndian.PutUint32(b[0:], view)
	binary.BigEndian.PutUint64(b[4:], seq)
	copy(b[12:], digest)
	d := sha256.Sum256(b)
	return d[:]
}

// prepareDigest computes the digest signed in a Prepare message.
// Covers: view + sequence + replica_id + request digest.
func prepareDigest(view uint32, seq uint64, replicaID uint32, digest []byte) []byte {
	b := make([]byte, 4+8+4+len(digest))
	binary.BigEndian.PutUint32(b[0:], view)
	binary.BigEndian.PutUint64(b[4:], seq)
	binary.BigEndian.PutUint32(b[12:], replicaID)
	copy(b[16:], digest)
	d := sha256.Sum256(b)
	return d[:]
}

// commitDigest computes the digest signed in a Commit message.
// Same fields as prepare.
func commitDigest(view uint32, seq uint64, replicaID uint32, digest []byte) []byte {
	return prepareDigest(view, seq, replicaID, digest)
}

// checkpointDigest computes the digest signed in a Checkpoint message.
// Covers: sequence + replica_id + state digest.
func checkpointDigest(seq uint64, replicaID uint32, stateDigest []byte) []byte {
	b := make([]byte, 8+4+len(stateDigest))
	binary.BigEndian.PutUint64(b[0:], seq)
	binary.BigEndian.PutUint32(b[8:], replicaID)
	copy(b[12:], stateDigest)
	d := sha256.Sum256(b)
	return d[:]
}

// ── Sign / Verify ─────────────────────────────────────────────────────────────

func sign(priv ed25519.PrivateKey, digest []byte) []byte {
	return ed25519.Sign(priv, digest)
}

func verify(pub ed25519.PublicKey, digest, signature []byte) bool {
	return ed25519.Verify(pub, digest, signature)
}

// verifyMsg verifies an Ed25519 signature from senderID over msgDigest.
// Returns true if the key is unknown (no key registry yet) or if valid.
// Returns false only on a known-bad signature.
func verifyMsg(senderID uint32, msgDigest []byte, signature []byte) bool {
	pub, ok := getPubKey(senderID)
	if !ok {
		return true
	}
	return verify(pub, msgDigest, signature)
}
