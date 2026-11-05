// Package ratelimit implements GCRA (Generic Cell Rate Algorithm) rate
// limiting. Each key stores one number, its theoretical arrival time (TAT),
// so memory is O(1) per key. A key whose TAT is in the past is equivalent to
// a fresh key, so such entries are dropped by periodic per-shard sweeps.
//
// State lives outside the keyspace and is not persisted or replicated.
package ratelimit

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	numShards  = 64
	sweepEvery = 1024 // operations on a shard between sweeps
)

type shard struct {
	mu  sync.Mutex
	tat map[string]int64 // unix ns
	ops int
}

type Limiter struct {
	shards [numShards]*shard
	rotor  atomic.Uint32
}

func New() *Limiter {
	l := &Limiter{}
	for i := range l.shards {
		l.shards[i] = &shard{tat: map[string]int64{}}
	}
	return l
}

// Result is the outcome of a rate-limit check.
type Result struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration // 0 when allowed
	ResetAfter time.Duration // until the limiter is back to a full burst
}

// Take checks whether cost units may be consumed from key's budget of limit
// per period, and consumes them if so (unless peek is set).
func (l *Limiter) Take(key string, limit int64, period time.Duration, cost int64, peek bool) Result {
	return l.take(key, limit, period, cost, peek, time.Now().UnixNano())
}

func (l *Limiter) take(key string, limit int64, period time.Duration, cost int64, peek bool, now int64) Result {
	s := l.shardOf(key)
	s.mu.Lock()
	swept := !peek && s.maybeSweep(now)
	defer func() {
		s.mu.Unlock()
		if swept {
			l.sweepNext(now)
		}
	}()

	p := int64(period)
	t := p / limit // emission interval
	if t <= 0 {
		t = 1
	}
	tat, ok := s.tat[key]
	if !ok || tat < now {
		tat = now
	}
	newTat := tat + cost*t
	allowAt := newTat - p

	var r Result
	if now >= allowAt {
		r.Allowed = true
		if peek { // report the current state: nothing is consumed
			r.Remaining = (now + p - tat) / t
			r.ResetAfter = time.Duration(tat - now)
		} else {
			r.Remaining = (now + p - newTat) / t
			r.ResetAfter = time.Duration(newTat - now)
			if cost > 0 {
				s.tat[key] = newTat
			}
		}
	} else {
		r.RetryAfter = time.Duration(allowAt - now)
		r.Remaining = (now + p - tat) / t
		r.ResetAfter = time.Duration(tat - now)
	}
	if r.Remaining < 0 {
		r.Remaining = 0
	}
	return r
}

// Reset forgets key. Returns true if it had state.
func (l *Limiter) Reset(key string) bool {
	return l.reset(key, time.Now().UnixNano())
}

func (l *Limiter) reset(key string, now int64) bool {
	s := l.shardOf(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	tat, ok := s.tat[key]
	delete(s.tat, key)
	return ok && tat > now
}

// Len returns the number of tracked keys (including idle ones not yet swept).
func (l *Limiter) Len() int {
	n := 0
	for _, s := range l.shards {
		s.mu.Lock()
		n += len(s.tat)
		s.mu.Unlock()
	}
	return n
}

// maybeSweep drops idle entries of s every sweepEvery operations and reports
// whether it did (the caller then sweeps one more shard in rotation, so idle
// entries in quiet shards are reclaimed too).
func (s *shard) maybeSweep(now int64) bool {
	s.ops++
	if s.ops < sweepEvery {
		return false
	}
	s.ops = 0
	s.sweep(now)
	return true
}

func (s *shard) sweep(now int64) {
	for k, tat := range s.tat {
		if tat <= now {
			delete(s.tat, k)
		}
	}
}

// sweepNext sweeps the next shard in rotation. Call without any shard lock.
func (l *Limiter) sweepNext(now int64) {
	s := l.shards[l.rotor.Add(1)%numShards]
	s.mu.Lock()
	s.sweep(now)
	s.mu.Unlock()
}

func (l *Limiter) shardOf(key string) *shard {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return l.shards[h%numShards]
}
