package server

import (
	"bytes"
	"context"
	"log/slog"
	"sync"
)

// logEntry tracks the state of a single request in the PBFT protocol.
type logEntry struct {
	mu  sync.Mutex
	seq uint64
	ts  int64

	// Paper Section 4.2 — stored messages
	prePrepare *prePrepareRecord         // the accepted pre-prepare
	prepares   map[uint32]*prepareRecord // replica_id → prepare (deduplicates by sender)
	commits    map[uint32]*commitRecord  // replica_id → commit (deduplicates by sender)

	// Derived state
	sentPrepare bool
	sentCommit  bool
	prepared    bool
	commited    bool
	executed    bool
}

type prePrepareRecord struct {
	view     uint32
	sequence uint64
	digest   []byte
}

type prepareRecord struct {
	view      uint32
	sequence  uint64
	digest    []byte
	replicaID uint32
}

type commitRecord struct {
	view      uint32
	sequence  uint64
	digest    []byte
	replicaID uint32
}

func (e *logEntry) checkPrepared(f int) bool {
	if e.prePrepare == nil {
		slog.Warn("checkPrepared: prePrepare is nil", "seq", e.seq)
		return false
	}
	if e.prepared {
		return false
	}
	matching := 0
	for _, p := range e.prepares {
		if p.view == e.prePrepare.view &&
			p.sequence == e.prePrepare.sequence &&
			bytes.Equal(p.digest, e.prePrepare.digest) {
			matching++
		} else {
			slog.Warn("checkPrepared: digest mismatch",
				"seq", e.seq,
				"from", p.replicaID,
				"got_view", p.view, "want_view", e.prePrepare.view,
				"got_digest", p.digest, "want_digest", e.prePrepare.digest,
			)
		}
	}
	if e.sentPrepare && matching >= 2*f {
		e.prepared = true
		e.sentCommit = true
		return true
	}
	return false
}

func (e *logEntry) checkCommitted(f int) bool {
	if e.prePrepare == nil {
		slog.Warn("checkCommitted: prePrepare is nil", "seq", e.seq)
		return false
	}
	if !e.prepared || e.commited {
		return false
	}
	matching := 0
	for _, c := range e.commits {
		if c.view == e.prePrepare.view &&
			c.sequence == e.prePrepare.sequence &&
			bytes.Equal(c.digest, e.prePrepare.digest) {
			matching++
		} else {
			slog.Warn("checkCommitted: digest mismatch",
				"seq", e.seq,
				"from", c.replicaID,
				"got_view", c.view, "want_view", e.prePrepare.view,
				"got_digest", c.digest, "want_digest", e.prePrepare.digest,
			)
		}
	}
	if matching >= 2*f+1 {
		e.commited = true
		e.executed = true
		return true
	}
	return false
}

// isPrepared returns true if this entry has reached the prepared state.
func (e *logEntry) isPrepared() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.prepared
}

// ── checkpoint ────────────────────────────────────────────────────────────────

// checkpointState tracks a pending or stable checkpoint.
type checkpointState struct {
	seq     uint64
	digests map[string]int
	voters  map[uint32]bool // track who has voted to prevent double counting
	stable  bool
}

func (c *checkpointState) recordVote(replicaID uint32, digest []byte, f int) bool {
	if c.stable {
		return false
	}
	if c.voters[replicaID] {
		return false // already voted
	}
	c.voters[replicaID] = true
	key := string(digest)
	c.digests[key]++
	return c.digests[key] >= 2*f+1
}

// ── MessageLog ────────────────────────────────────────────────────────────────

const (
	checkpointInterval = 1000
	waterMarkWindow    = 2000
)

// MessageLog tracks all in-flight log entries plus checkpoint state
// for garbage collection and water marks.
type MessageLog struct {
	mu          sync.RWMutex
	cond        *sync.Cond
	entries     map[uint64]*logEntry // seq → entry
	entriesByTs map[int64]*logEntry  // ts  → same entry objects

	lowWaterMark  uint64
	highWaterMark uint64

	checkpoints   map[uint64]*checkpointState
	lastStableSeq uint64
}

func NewMessageLog() *MessageLog {
	ml := &MessageLog{
		entries:       make(map[uint64]*logEntry),
		entriesByTs:   make(map[int64]*logEntry),
		checkpoints:   make(map[uint64]*checkpointState),
		lowWaterMark:  0,
		highWaterMark: waterMarkWindow,
	}
	ml.cond = sync.NewCond(&ml.mu)
	return ml
}

func (ml *MessageLog) GetOrCreate(seq uint64) *logEntry {
	ml.mu.RLock()
	e, ok := ml.entries[seq]
	ml.mu.RUnlock()
	if ok {
		return e
	}
	ml.mu.Lock()
	defer ml.mu.Unlock()
	if e, ok = ml.entries[seq]; ok {
		return e
	}
	e = &logEntry{seq: seq}
	ml.entries[seq] = e
	return e
}

func (ml *MessageLog) FindByTs(ts int64) *logEntry {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	for _, e := range ml.entries {
		if e.ts == ts {
			return e
		}
	}
	return nil
}

// WithinWaterMarks returns true if seq falls within the current window.
func (ml *MessageLog) WithinWaterMarks(seq uint64) bool {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return seq > ml.lowWaterMark && seq <= ml.highWaterMark
}

// ShouldCheckpoint returns true if seq is a checkpoint sequence number.
func (ml *MessageLog) ShouldCheckpoint(seq uint64) bool {
	return seq > 0 && seq%checkpointInterval == 0
}

// LowWaterMark returns the current low water mark.
func (ml *MessageLog) LowWaterMark() uint64 {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.lowWaterMark
}

// HighWaterMark returns the current high water mark.
func (ml *MessageLog) HighWaterMark() uint64 {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.highWaterMark
}

// LastStableSeq returns the sequence number of the last stable checkpoint.
func (ml *MessageLog) LastStableSeq() uint64 {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	return ml.lastStableSeq
}

// PreparedEntries returns all log entries that have reached prepared state
// with sequence number above aboveSeq. Used when building the P set for
// a VIEW-CHANGE message.
func (ml *MessageLog) PreparedEntries(aboveSeq uint64) []*logEntry {
	ml.mu.RLock()
	defer ml.mu.RUnlock()
	var result []*logEntry
	for seq, e := range ml.entries {
		if seq > aboveSeq && e.isPrepared() {
			result = append(result, e)
		}
	}
	return result
}

// RecordCheckpoint records a CHECKPOINT vote for the given seq and digest.
// Returns (stable, gcCount) where stable is true if this vote made the
// checkpoint stable, and gcCount is the number of log entries discarded.
func (ml *MessageLog) RecordCheckpoint(seq uint64, replicaID uint32, digest []byte, f int) (stable bool, gcCount int) {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	cp, ok := ml.checkpoints[seq]
	if !ok {
		cp = &checkpointState{
			seq:     seq,
			digests: make(map[string]int),
			voters:  make(map[uint32]bool),
		}
		ml.checkpoints[seq] = cp
	}

	if !cp.recordVote(replicaID, digest, f) {
		return false, 0
	}

	// checkpoint is now stable
	cp.stable = true
	if seq <= ml.lastStableSeq {
		return true, 0
	}

	ml.lastStableSeq = seq
	ml.lowWaterMark = seq
	ml.highWaterMark = seq + waterMarkWindow
	gcCount = ml.garbageCollect(seq)
	ml.cond.Broadcast() // wake up anyone waiting on water marks
	return true, gcCount
}

// garbageCollect discards log entries and old checkpoints at or below stableSeq.
// Must be called with ml.mu held.
// Returns the number of log entries discarded.
func (ml *MessageLog) garbageCollect(stableSeq uint64) int {
	count := 0
	for seq, entry := range ml.entries {
		if seq <= stableSeq && entry.executed {
			delete(ml.entries, seq)
			count++
		}
	}
	for seq := range ml.checkpoints {
		if seq < stableSeq {
			delete(ml.checkpoints, seq)
		}
	}
	return count
}

func (ml *MessageLog) WaitForWaterMark(ctx context.Context, seq uint64) bool {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	for seq > ml.highWaterMark {
		select {
		case <-ctx.Done():
			return false
		default:
		}
		ml.cond.Wait()
	}
	return true
}

func (ml *MessageLog) Reset() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.entries = make(map[uint64]*logEntry)
	ml.checkpoints = make(map[uint64]*checkpointState)
	ml.lowWaterMark = 0
	ml.highWaterMark = waterMarkWindow
	ml.lastStableSeq = 0
}
