package server

import "sync"

type logEntry struct {
	seq          uint64
	ts           int64
	prepareCount int
	commitCount  int
	sentPrepare  bool
	sentCommit   bool
	committed    bool
	executed     bool
}

type MessageLog struct {
	mu      sync.RWMutex
	entries map[uint64]*logEntry
}

func NewMessageLog() *MessageLog {
	return &MessageLog{entries: make(map[uint64]*logEntry)}
}

// Update handles the lock and lookup, then lets you run logic safely.
func (ml *MessageLog) Update(seq uint64, fn func(e *logEntry)) *logEntry {
	ml.mu.Lock()
	defer ml.mu.Unlock()

	e, ok := ml.entries[seq]
	if !ok {
		e = &logEntry{seq: seq}
		ml.entries[seq] = e
	}
	fn(e)
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
