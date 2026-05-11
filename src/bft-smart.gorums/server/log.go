package server

import (
	"bytes"
	"context"
	"log/slog"
	"sync"

	pb "github.com/Mekruba/gorums-benchmark/bft-smart.gorums/proto"
)

// ConsensusEntry tracks one consensus instance (one batch of requests)
// through the three Mod-SMaRt phases: PROPOSE → WRITE → ACCEPT.
type ConsensusEntry struct {
	mu          sync.Mutex
	ConsensusID uint64
	View        uint32

	Propose *ProposeRecord
	Writes  map[uint32]*VoteRecord // replica → WRITE
	Accepts map[uint32]*VoteRecord // replica → ACCEPT

	// Full batch of requests, needed to inspect operation types on delivery.
	Batch []*pb.Request
	// Timestamps extracted from Batch for reply routing.
	BatchTimestamps []int64

	SentWrite  bool
	SentAccept bool
	Written    bool // WRITE quorum reached
	Accepted   bool // ACCEPT quorum reached
	Executed   bool
}

type ProposeRecord struct {
	ConsensusID uint64
	View        uint32
	LeaderID    uint32
	BatchDigest []byte
	Batch       []*pb.Request
}

type VoteRecord struct {
	ConsensusID uint64
	View        uint32
	ReplicaID   uint32
	BatchDigest []byte
}

// CheckWritten returns true the first time a WRITE quorum (2f+1) is reached.
// Caller must hold e.mu.
func (e *ConsensusEntry) CheckWritten(f int) bool {
	if e.Written {
		slog.Debug("CheckWritten: already written", "cid", e.ConsensusID)
		return false
	}
	if len(e.Writes) == 0 {
		slog.Debug("CheckWritten: no writes yet", "cid", e.ConsensusID)
		return false
	}
	// pick any vote to use as the reference digest
	var ref *VoteRecord
	for _, v := range e.Writes {
		ref = v
		break
	}
	matching := 0
	for _, v := range e.Writes {
		if v.View == ref.View &&
			v.ConsensusID == ref.ConsensusID &&
			bytes.Equal(v.BatchDigest, ref.BatchDigest) {
			matching++
		}
	}
	slog.Debug("CheckWritten", "cid", e.ConsensusID,
		"matching", matching, "needed", 2*f+1,
		"sent_write", e.SentWrite, "writes_count", len(e.Writes))

	if e.SentWrite && matching >= 2*f+1 {
		e.Written = true
		e.SentAccept = true
		return true
	}
	return false
}

// CheckAccepted returns true the first time an ACCEPT quorum (2f+1) is reached.
// Caller must hold e.mu.
func (e *ConsensusEntry) CheckAccepted(f int) bool {
	if e.Accepted {
		return false
	}
	if len(e.Accepts) == 0 {
		return false
	}
	var ref *VoteRecord
	for _, v := range e.Accepts {
		ref = v
		break
	}
	matching := 0
	for _, v := range e.Accepts {
		if v.View == ref.View &&
			v.ConsensusID == ref.ConsensusID &&
			bytes.Equal(v.BatchDigest, ref.BatchDigest) {
			matching++
		}
	}
	slog.Debug("CheckAccepted", "cid", e.ConsensusID,
		"matching", matching, "needed", 2*f+1,
		"written", e.Written, "accepts_count", len(e.Accepts))
	if matching >= 2*f+1 {
		e.Accepted = true
		e.Executed = true
		return true
	}
	return false
}

func countMatching(votes map[uint32]*VoteRecord, p *ProposeRecord) int {
	n := 0
	for _, v := range votes {
		if v.View == p.View &&
			v.ConsensusID == p.ConsensusID &&
			bytes.Equal(v.BatchDigest, p.BatchDigest) {
			n++
		}
	}
	return n
}

// CheckpointState tracks a pending or stable checkpoint.
type CheckpointState struct {
	ConsensusID uint64
	digests     map[string]int
	voters      map[uint32]bool
	Proof       []*pb.CheckpointMsg
	Stable      bool
}

func (c *CheckpointState) recordVote(replicaID uint32, digest []byte, msg *pb.CheckpointMsg, f int) bool {
	if c.Stable || c.voters[replicaID] {
		return false
	}
	c.voters[replicaID] = true
	c.Proof = append(c.Proof, msg)
	key := string(digest)
	c.digests[key]++
	return c.digests[key] >= 2*f+1
}

const (
	CheckpointInterval = 18000
	WaterMarkWindow    = 36000
)

// MessageLog is the central state store for in-flight consensus entries and checkpoints.
type MessageLog struct {
	mu   sync.RWMutex
	cond *sync.Cond

	entries map[uint64]*ConsensusEntry

	lowWaterMark  uint64
	highWaterMark uint64

	checkpoints   map[uint64]*CheckpointState
	lastStableSeq uint64
}

func NewMessageLog() *MessageLog {
	ml := &MessageLog{
		entries:       make(map[uint64]*ConsensusEntry),
		checkpoints:   make(map[uint64]*CheckpointState),
		highWaterMark: WaterMarkWindow,
	}
	ml.cond = sync.NewCond(&ml.mu)
	return ml
}

func (ml *MessageLog) GetOrCreate(cid uint64) *ConsensusEntry {
	ml.mu.RLock()
	e, ok := ml.entries[cid]
	ml.mu.RUnlock()
	if ok {
		return e
	}
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if e, ok = ml.entries[cid]; ok {
		return e
	}
	e = &ConsensusEntry{ConsensusID: cid}
	ml.entries[cid] = e
	return e
}

func (ml *MessageLog) FindByTimestamp(ts int64) *ConsensusEntry {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	for _, e := range ml.entries {
		for _, bts := range e.BatchTimestamps {
			if bts == ts {
				return e
			}
		}
	}
	return nil
}

func (ml *MessageLog) WithinWaterMarks(cid uint64) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return cid > ml.lowWaterMark && cid <= ml.highWaterMark
}

func (ml *MessageLog) ShouldCheckpoint(cid uint64) bool {
	return cid > 0 && cid%CheckpointInterval == 0
}

func (ml *MessageLog) LowWaterMark() uint64 {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.lowWaterMark
}

func (ml *MessageLog) HighWaterMark() uint64 {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.highWaterMark
}

func (ml *MessageLog) LastStableSeq() uint64 {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.lastStableSeq
}

func (ml *MessageLog) StableCheckpointProof() (uint64, []*pb.CheckpointMsg) {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	cp, ok := ml.checkpoints[ml.lastStableSeq]
	if !ok || !cp.Stable {
		return 0, nil
	}
	out := make([]*pb.CheckpointMsg, len(cp.Proof))
	copy(out, cp.Proof)
	return cp.ConsensusID, out
}

func (ml *MessageLog) RecordCheckpoint(cid uint64, replicaID uint32, digest []byte, msg *pb.CheckpointMsg, f int) (stable bool, gcCount int) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	cp, ok := ml.checkpoints[cid]
	if !ok {
		cp = &CheckpointState{
			ConsensusID: cid,
			digests:     make(map[string]int),
			voters:      make(map[uint32]bool),
		}
		ml.checkpoints[cid] = cp
	}
	if !cp.recordVote(replicaID, digest, msg, f) {
		return false, 0
	}
	cp.Stable = true
	if cid <= ml.lastStableSeq {
		return true, 0
	}
	ml.lastStableSeq = cid
	ml.lowWaterMark = cid
	ml.highWaterMark = cid + WaterMarkWindow
	gcCount = ml.gc(cid)
	ml.cond.Broadcast()
	return true, gcCount
}

func (ml *MessageLog) gc(stableSeq uint64) int {
	count := 0
	for cid, e := range ml.entries {
		if cid <= stableSeq && e.Executed {
			delete(ml.entries, cid)
			count++
		}
	}
	for cid := range ml.checkpoints {
		if cid < stableSeq {
			delete(ml.checkpoints, cid)
		}
	}
	return count
}

func (ml *MessageLog) WaitForWaterMark(ctx context.Context, cid uint64) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	for cid > ml.highWaterMark {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		ml.cond.Wait()
	}
	return true
}

func (ml *MessageLog) ResetUncommitted(fromCID uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	count := 0
	for cid, e := range ml.entries {
		if cid >= fromCID {
			e.mu.Lock()
			if !e.Accepted {
				e.SentWrite = false
				e.SentAccept = false
				e.Written = false
				e.Writes = nil
				e.Accepts = nil
				e.Propose = nil
				e.Batch = nil
				count++
			}
			e.mu.Unlock()
		}
	}
	slog.Debug("ResetUncommitted", "from_cid", fromCID, "reset_count", count)
}

func (ml *MessageLog) SetLowWaterMark(cid uint64) {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if cid > ml.lowWaterMark {
		ml.lowWaterMark = cid
		ml.highWaterMark = cid + WaterMarkWindow
		ml.cond.Broadcast()
	}
}

func (ml *MessageLog) Reset() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.entries = make(map[uint64]*ConsensusEntry)
	ml.checkpoints = make(map[uint64]*CheckpointState)
	ml.lowWaterMark = 0
	ml.highWaterMark = WaterMarkWindow
	ml.lastStableSeq = 0
}
