package server

import "sync"

type logEntry struct {
	mu           sync.Mutex
	seq          uint64
	ts           int64
	prepareCount int
	commitCount  int
	sentPrepare  bool
	prepared     bool
	sentCommit   bool
	commited     bool
	executed     bool
}

func (e *logEntry) checkPrepared(f int) bool {

	if e.sentPrepare && e.prepareCount >= 2*f && !e.prepared {
		e.prepared = true
		e.sentCommit = true
		return true
	}
	return false
}

func (e *logEntry) checkCommitted(f int) bool {

	if e.prepared && e.commitCount >= (2*f+1) && !e.commited {
		e.commited = true
		e.executed = true
		return true
	}
	return false
}

type MessageLog struct {
	mu      sync.RWMutex
	entries map[uint64]*logEntry
}

func NewMessageLog() *MessageLog {
	return &MessageLog{entries: make(map[uint64]*logEntry)}
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

func (ml *MessageLog) Reset() {
	ml.mu.Lock()
	defer ml.mu.Unlock()
	ml.entries = make(map[uint64]*logEntry)
}
