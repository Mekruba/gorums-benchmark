package server

import (
	"crypto/ed25519"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"log"
	"math/rand/v2"
	"sync"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
	"google.golang.org/protobuf/proto"
)

type NodeKeys struct {
	PrivKey ed25519.PrivateKey
	PubKey  ed25519.PublicKey
}

func GenerateKeys() (NodeKeys, error) {
	pub, priv, err := ed25519.GenerateKey(crand.Reader)
	if err != nil {
		return NodeKeys{}, err
	}
	return NodeKeys{PrivKey: priv, PubKey: pub}, nil
}

// Key registry — deterministic keys from a fixed seed so all nodes agree
// without key exchange.

var (
	keysMu   sync.RWMutex
	privKeys map[uint32]ed25519.PrivateKey
	pubKeys  map[uint32]ed25519.PublicKey
)

func InitKeys(n int) {
	keysMu.Lock()
	defer keysMu.Unlock()
	src := rand.NewChaCha8([32]byte{0x42, 0x46, 0x54, 0x53, 0x4d, 0x41, 0x52, 0x54})
	rng := rand.New(src)
	privKeys = make(map[uint32]ed25519.PrivateKey, n)
	pubKeys = make(map[uint32]ed25519.PublicKey, n)
	for i := 1; i <= n; i++ {
		seed := make([]byte, ed25519.SeedSize)
		for j := range seed {
			seed[j] = byte(rng.IntN(256))
		}
		priv := ed25519.NewKeyFromSeed(seed)
		pub := priv.Public().(ed25519.PublicKey)
		privKeys[uint32(i)] = priv
		pubKeys[uint32(i)] = pub
	}
}

func getPrivKey(id uint32) ed25519.PrivateKey {
	keysMu.RLock()
	defer keysMu.RUnlock()
	k := privKeys[id]
	if k == nil {
		log.Fatalf("no private key for node %d", id)
	}
	return k
}

func getPubKey(id uint32) (ed25519.PublicKey, bool) {
	keysMu.RLock()
	defer keysMu.RUnlock()
	k, ok := pubKeys[id]
	return k, ok
}

// Digest helpers — each returns SHA-256 over the message's signed fields.

func batchDigest(batch []*pb.Request) []byte {
	h := sha256.New()
	for _, r := range batch {
		data, _ := proto.Marshal(r)
		h.Write(data)
	}
	return h.Sum(nil)
}

func proposeDigest(cid uint64, view uint32, bd []byte) []byte {
	buf := make([]byte, 8+4+len(bd))
	binary.BigEndian.PutUint64(buf, cid)
	binary.BigEndian.PutUint32(buf[8:], view)
	copy(buf[12:], bd)
	return sha256sum(buf)
}

// WRITE and ACCEPT sign the same fields as PROPOSE.
func writeDigest(cid uint64, view uint32, bd []byte) []byte  { return proposeDigest(cid, view, bd) }
func acceptDigest(cid uint64, view uint32, bd []byte) []byte { return proposeDigest(cid, view, bd) }

func checkpointDigest(cid uint64, sd []byte) []byte {
	buf := make([]byte, 8+len(sd))
	binary.BigEndian.PutUint64(buf, cid)
	copy(buf[8:], sd)
	return sha256sum(buf)
}

func stopDigest(newView uint32, lastCID uint64) []byte {
	return viewSeqDigest(newView, lastCID)
}

func syncDigest(newView uint32, nextCID uint64) []byte {
	return viewSeqDigest(newView, nextCID)
}

func viewUpdateDigest(newView, targetID uint32, cid uint64) []byte {
	buf := make([]byte, 4+4+8)
	binary.BigEndian.PutUint32(buf, newView)
	binary.BigEndian.PutUint32(buf[4:], targetID)
	binary.BigEndian.PutUint64(buf[8:], cid)
	return sha256sum(buf)
}

func forwardDigest(forwarderID uint32, ts int64) []byte {
	buf := make([]byte, 4+8)
	binary.BigEndian.PutUint32(buf, forwarderID)
	binary.BigEndian.PutUint64(buf[4:], uint64(ts))
	return sha256sum(buf)
}

func stateTransferReqDigest(replicaID uint32, lastCID uint64) []byte {
	buf := make([]byte, 4+8)
	binary.BigEndian.PutUint32(buf, replicaID)
	binary.BigEndian.PutUint64(buf[4:], lastCID)
	return sha256sum(buf)
}

func stateTransferRespDigest(lastCID uint64, view, replicaID uint32) []byte {
	buf := make([]byte, 8+4+4)
	binary.BigEndian.PutUint64(buf, lastCID)
	binary.BigEndian.PutUint32(buf[8:], view)
	binary.BigEndian.PutUint32(buf[12:], replicaID)
	return sha256sum(buf)
}

func stateDigest(cid uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, cid)
	return sha256sum(buf)
}

func nullDigest() []byte { return sha256sum([]byte("null")) }

// Sign / Verify

func sign(priv ed25519.PrivateKey, digest []byte) []byte {
	return ed25519.Sign(priv, digest)
}

func verifyMsg(senderID uint32, digest, signature []byte) bool {
	pub, ok := getPubKey(senderID)
	if !ok {
		return true // unknown key — accept (same behaviour as PBFT impl)
	}
	return ed25519.Verify(pub, digest, signature)
}

// internal helpers

func sha256sum(data []byte) []byte {
	h := sha256.Sum256(data)
	return h[:]
}

func viewSeqDigest(view uint32, seq uint64) []byte {
	buf := make([]byte, 4+8)
	binary.BigEndian.PutUint32(buf, view)
	binary.BigEndian.PutUint64(buf[4:], seq)
	return sha256sum(buf)
}
