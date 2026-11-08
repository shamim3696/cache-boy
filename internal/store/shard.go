package store

import (
	"sort"
	"sync"
)

type shard struct {
	mu       sync.Mutex
	data     map[string]*Entry
	volatile map[string]struct{} // keys with an expiry, sampled by volatile-* eviction
	wheel    wheel               // the same keys bucketed by expiry, for active expiry
	freq     *sketch             // recent access frequency, for allkeys/volatile-tinylfu
	unlock   func()              // mu.Unlock, made once: a method value per Lock allocates
}

func newShard() *shard {
	s := &shard{
		data:     map[string]*Entry{},
		volatile: map[string]struct{}{},
		wheel:    newWheel(nowMs()),
		freq:     newSketch(0),
	}
	s.unlock = s.mu.Unlock
	return s
}

// NumShards returns the shard count.
func (db *DB) NumShards() int { return len(db.shards) }

// Lock locks the shards holding keys (in index order, to avoid deadlocks) and
// returns the unlock function.
func (db *DB) Lock(keys ...string) func() {
	switch len(keys) {
	case 0:
		return func() {}
	case 1:
		s := db.shards[db.shardIndex(keys[0])]
		s.mu.Lock()
		return s.unlock
	}
	idx := make([]int, 0, len(keys))
	seen := map[uint32]bool{}
	for _, k := range keys {
		i := db.shardIndex(k)
		if !seen[i] {
			seen[i] = true
			idx = append(idx, int(i))
		}
	}
	sort.Ints(idx)
	for _, i := range idx {
		db.shards[i].mu.Lock()
	}
	return func() {
		for j := len(idx) - 1; j >= 0; j-- {
			db.shards[idx[j]].mu.Unlock()
		}
	}
}

// LockAll locks every shard (KEYS, FLUSHALL, MULTI/EXEC, scripts, snapshots).
func (db *DB) LockAll() func() {
	for _, s := range db.shards {
		s.mu.Lock()
	}
	return func() {
		for i := len(db.shards) - 1; i >= 0; i-- {
			db.shards[i].mu.Unlock()
		}
	}
}

// ActiveExpireCycle deletes expired keys found in the due buckets of each
// shard's timing wheel. It checks at most budget wheel entries per shard and
// holds one shard lock at a time, so a mass expiry can't stall the server;
// the rest waits for the next cycle. It returns the number of keys deleted
// and whether due entries are left over. Self-locking.
func (db *DB) ActiveExpireCycle(budget int) (expired int, more bool) {
	for _, s := range db.shards {
		s.mu.Lock()
		n, m := db.expireDue(s, budget, nowMs())
		s.mu.Unlock()
		expired += n
		more = more || m
	}
	return expired, more
}

// Candidate is a key sampled for eviction.
type Candidate struct {
	Key      string
	Access   int64 // last access, unix ms
	Freq     uint8 // Redis-style logarithmic LFU counter
	Sketch   uint8 // TinyLFU estimate of recent accesses, 0..16
	ExpireAt int64
}

func (s *shard) candidate(k string, e *Entry) Candidate {
	return Candidate{k, e.access, e.freq, s.freq.estimate(sketchHash(e.hash)), e.ExpireAt}
}

// Sample returns up to n random keys from one shard. With volatileOnly only
// keys that have a TTL are considered. Self-locking.
func (db *DB) Sample(shardIdx, n int, volatileOnly bool) []Candidate {
	s := db.shards[shardIdx%len(db.shards)]
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Candidate, 0, n)
	if volatileOnly {
		for k := range s.volatile {
			if len(out) >= n {
				break
			}
			out = append(out, s.candidate(k, s.data[k]))
		}
		return out
	}
	for k, e := range s.data {
		if len(out) >= n {
			break
		}
		out = append(out, s.candidate(k, e))
	}
	return out
}

// Recheck returns the current eviction data of key, for candidates that
// waited in an eviction pool. Self-locking.
func (db *DB) Recheck(key string) (Candidate, bool) {
	s := db.shards[db.shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.data[key]
	if !ok {
		return Candidate{}, false
	}
	return s.candidate(key, e), true
}

// Evict deletes key and emits an "evicted" event. Self-locking.
func (db *DB) Evict(key string) bool {
	s := db.shards[db.shardIndex(key)]
	s.mu.Lock()
	defer s.mu.Unlock()
	if !db.Delete(key) {
		return false
	}
	db.Evicted.Add(1)
	db.Emit(Event{Key: key, Op: "evicted"})
	return true
}

// RandomKey returns a random live key. Requires LockAll.
func (db *DB) RandomKey() (string, bool) {
	start := int(nowMs()) & int(db.mask)
	for i := 0; i < len(db.shards); i++ {
		s := db.shards[(start+i)%len(db.shards)]
		for k := range s.data {
			if db.lookup(k) != nil {
				return k, true
			}
		}
	}
	return "", false
}
