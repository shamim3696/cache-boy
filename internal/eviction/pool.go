package eviction

import "github.com/shamim3696/cache-boy/internal/store"

const poolSize = 16

// pool holds the best eviction candidates seen so far, best first, like
// Redis' EvictionPoolLRU. Keys sampled in earlier rounds stay eligible, so
// each eviction effectively chooses among many more keys than one sample.
type pool struct {
	policy string // the policy the entries were ranked by
	c      []store.Candidate
}

// offer adds cand to the pool if it ranks among the best poolSize entries.
func (p *pool) offer(cand store.Candidate, pick better, now int64) {
	if len(p.c) == poolSize && !pick(cand, p.c[poolSize-1], now) {
		return // the common case once the pool is warm
	}
	for i := range p.c {
		if p.c[i].Key == cand.Key {
			p.remove(i)
			break
		}
	}
	i := 0
	for i < len(p.c) && !pick(cand, p.c[i], now) {
		i++
	}
	if i == poolSize {
		return
	}
	if len(p.c) == poolSize {
		p.c = p.c[:poolSize-1]
	}
	p.c = append(p.c, store.Candidate{})
	copy(p.c[i+1:], p.c[i:])
	p.c[i] = cand
}

func (p *pool) remove(i int) {
	p.c = append(p.c[:i], p.c[i+1:]...)
}

// take removes and returns the best candidate that still exists. Entries may
// have been accessed since they were sampled, so the best one is re-read and
// put back if it no longer beats the runner-up.
func (p *pool) take(db *store.DB, pick better, now int64, volatileOnly bool) *store.Candidate {
	for range 2 * poolSize {
		if len(p.c) == 0 {
			return nil
		}
		key := p.c[0].Key
		p.remove(0)
		cur, ok := db.Recheck(key)
		if !ok || (volatileOnly && cur.ExpireAt == 0) {
			continue
		}
		if len(p.c) > 0 && pick(p.c[0], cur, now) {
			p.offer(cur, pick, now)
			continue
		}
		return &cur
	}
	return nil
}
