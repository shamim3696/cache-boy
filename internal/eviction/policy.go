// Package eviction frees memory when maxmemory is reached. Like Redis it uses
// approximated algorithms: sample a few random keys and evict the best
// candidate, instead of keeping an exact LRU list of every key. The best
// candidates seen so far are kept in a small pool across rounds and calls,
// which makes the approximation much closer to the exact algorithm.
package eviction

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"cache-boy/internal/config"
	"cache-boy/internal/store"
)

var ErrOOM = errors.New("OOM command not allowed when used memory > 'maxmemory'.")

const maxRounds = 1024

// better reports whether candidate a should be evicted before b.
type better func(a, b store.Candidate, now int64) bool

type Evictor struct {
	db   *store.DB
	cfg  *config.Store
	next atomic.Uint32 // shard to sample next

	mu   sync.Mutex // serialises evictions, so concurrent writers don't overshoot
	pool pool
}

func New(db *store.DB, cfg *config.Store) *Evictor {
	return &Evictor{db: db, cfg: cfg}
}

// Free evicts keys until memory is under maxmemory. It returns ErrOOM if that
// isn't possible (policy noeviction, or nothing left to evict). It must be
// called without holding any shard lock.
func (ev *Evictor) Free() error {
	c := ev.cfg.Get()
	if c.MaxMemory <= 0 || ev.db.UsedMemory() <= c.MaxMemory {
		return nil
	}
	if c.MaxMemoryPolicy == "noeviction" {
		return ErrOOM
	}
	ev.mu.Lock()
	defer ev.mu.Unlock()
	volatileOnly := strings.HasPrefix(c.MaxMemoryPolicy, "volatile-")
	pick := policy(c.MaxMemoryPolicy)
	usePool := !strings.HasSuffix(c.MaxMemoryPolicy, "-random")
	if ev.pool.policy != c.MaxMemoryPolicy {
		ev.pool = pool{policy: c.MaxMemoryPolicy}
	}
	samples := c.MaxMemorySamples
	if samples <= 0 {
		samples = 5
	}
	n := ev.db.NumShards()
	for round := 0; ev.db.UsedMemory() > c.MaxMemory; round++ {
		if round >= maxRounds {
			return ErrOOM
		}
		// Sample a few shards per round so we compare keys across the keyspace.
		now := nowMs()
		var best *store.Candidate
		for i := 0; i < 4; i++ {
			shard := int(ev.next.Add(1)) % n
			for _, cand := range ev.db.Sample(shard, samples, volatileOnly) {
				if usePool {
					ev.pool.offer(cand, pick, now)
				} else if best == nil {
					best = &cand
				}
			}
		}
		if usePool {
			best = ev.pool.take(ev.db, pick, now, volatileOnly)
		}
		if best == nil {
			if ev.db.Len() == 0 || round > n {
				return ErrOOM
			}
			continue
		}
		ev.db.Evict(best.Key)
	}
	return nil
}

func policy(name string) better {
	switch {
	case strings.HasSuffix(name, "-tinylfu"):
		return tinyLFUBetter
	case strings.HasSuffix(name, "-lru"):
		return lruBetter
	case strings.HasSuffix(name, "-lfu"):
		return lfuBetter
	case name == "volatile-ttl":
		return ttlBetter
	}
	return randomBetter
}

// volatile-ttl: evict the key that would expire soonest anyway.
func ttlBetter(a, b store.Candidate, _ int64) bool { return a.ExpireAt < b.ExpireAt }
