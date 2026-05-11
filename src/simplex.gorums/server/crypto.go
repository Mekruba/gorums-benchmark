package server

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"

	pb "github.com/Mekruba/gorums-benchmark/simplex.gorums/proto"
)

// Domain separation prefixes prevent signature reuse across message types.
const (
	domainVote     = "simplex-vote\x00"
	domainPropose  = "simplex-propose\x00"
	domainFinalize = "simplex-finalize\x00"
)

func voteBytes(height uint64, block *pb.Block) []byte {
	return appendUint64([]byte(domainVote), height, blockID(block))
}

func proposeBytes(height uint64, block *pb.Block, chainH string) []byte {
	msg := appendUint64([]byte(domainPropose), height, blockID(block))
	msg = append(msg, '|')
	msg = append(msg, chainH...)
	return msg
}

func finalizeBytes(height uint64) []byte {
	return appendUint64([]byte(domainFinalize), height, "")
}

func appendUint64(dst []byte, v uint64, suffix string) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	dst = append(dst, buf[:]...)
	dst = append(dst, suffix...)
	return dst
}

func signVote(msg *pb.VoteMsg, priv ed25519.PrivateKey) {
	msg.SetSignature(ed25519.Sign(priv, voteBytes(msg.GetHeight(), msg.GetBlock())))
}

func signPropose(msg *pb.ProposeMsg, priv ed25519.PrivateKey) {
	data := proposeBytes(msg.GetHeight(), msg.GetBlock(), chainHash(msg.GetChain()))
	msg.SetSignature(ed25519.Sign(priv, data))
}

func signFinalize(msg *pb.FinalizeMsg, priv ed25519.PrivateKey) {
	msg.SetSignature(ed25519.Sign(priv, finalizeBytes(msg.GetHeight())))
}

func verifyVote(msg *pb.VoteMsg, pub ed25519.PublicKey) error {
	sig := msg.GetSignature()
	if len(sig) == 0 {
		return errors.New("missing signature")
	}
	if !ed25519.Verify(pub, voteBytes(msg.GetHeight(), msg.GetBlock()), sig) {
		return fmt.Errorf("invalid vote signature from node %d at height %d", msg.GetVoterId(), msg.GetHeight())
	}
	return nil
}

func verifyPropose(msg *pb.ProposeMsg, pub ed25519.PublicKey) error {
	sig := msg.GetSignature()
	if len(sig) == 0 {
		return errors.New("missing signature")
	}
	data := proposeBytes(msg.GetHeight(), msg.GetBlock(), chainHash(msg.GetChain()))
	if !ed25519.Verify(pub, data, sig) {
		return fmt.Errorf("invalid propose signature from node %d at height %d", msg.GetLeaderId(), msg.GetHeight())
	}
	return nil
}

func verifyFinalize(msg *pb.FinalizeMsg, pub ed25519.PublicKey) error {
	sig := msg.GetSignature()
	if len(sig) == 0 {
		return errors.New("missing signature")
	}
	if !ed25519.Verify(pub, finalizeBytes(msg.GetHeight()), sig) {
		return fmt.Errorf("invalid finalize signature from node %d at height %d", msg.GetNodeId(), msg.GetHeight())
	}
	return nil
}
