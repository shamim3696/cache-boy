package store

import (
	"sort"
	"sync"
)

type shard struct {
	mu       sync.Mutex
	data     map[string]*Entry
	volatile map[string]struct{} // keys with an expiry, for active expiry
}

func newShard() *shard {
	return &shard{data: map[string]*Entry{}, volatile: map[string]struct{}{}}
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
		s := db.shardOf(keys[0])
		s.mu.Lock()
		return s.mu.Unlock
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

// ActiveExpireCycle checks up to sample keys with a TTL in every shard and
// deletes the expired ones, like Redis' activeExpireCycle. If more than 25% of
// a shard's sample was expired it samples that shard again. Self-locking.
func (db *DB) ActiveExpireCycle(sample int) int {
	total := 0
	for _, s := range db.shards {
		for {
			s.mu.Lock()
			checked, expired := 0, 0
			now := nowMs()
			for k := range s.volatile { // map order is random in Go: free sampling
				if checked >= sample {
					break
				}
				checked++
				if e := s.data[k]; e != nil && e.ExpireAt <= now {
					db.Delete(k)
					db.Expired.Add(1)
					db.Emit(Event{Key: k, Op: "expired"})
					expired++
				}
			}
			s.mu.Unlock()
			total += expired
			if checked == 0 || expired*4 <= checked {
				break
			}
		}
	}
	return total
}

// Candidate is a key sampled for eviction.
type Candidate struct {
	Key      string
	Access   int64
	Freq     uint8
	ExpireAt int64
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
			e := s.data[k]
			out = append(out, Candidate{k, e.access, e.freq, e.ExpireAt})
		}
		return out
	}
	for k, e := range s.data {
		if len(out) >= n {
			break
		}
		out = append(out, Candidate{k, e.access, e.freq, e.ExpireAt})
	}
	return out
}

// Evict deletes key and emits an "evicted" event. Self-locking.
func (db *DB) Evict(key string) bool {
	s := db.shardOf(key)
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
