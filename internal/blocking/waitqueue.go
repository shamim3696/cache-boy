// Package blocking lets commands like BLPOP wait until one of their keys is
// written. A Waiter is registered while shard locks are held (so no write can
// be missed), then the client waits on Waiter.C without holding any lock.
package blocking

import (
	"sync"
	"sync/atomic"

	"cache-boy/internal/store"
)

type Waiter struct {
	C    chan struct{} // receives a signal when a key is written
	keys []string
}

type Queue struct {
	mu      sync.Mutex
	waiters map[string]map[*Waiter]struct{}
	count   atomic.Int64 // read without the lock on every write
}

func New(db *store.DB) *Queue {
	q := &Queue{waiters: map[string]map[*Waiter]struct{}{}}
	db.Subscribe(q.onEvent)
	return q
}

func (q *Queue) Register(keys ...string) *Waiter {
	w := &Waiter{C: make(chan struct{}, 1), keys: keys}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, k := range keys {
		if q.waiters[k] == nil {
			q.waiters[k] = map[*Waiter]struct{}{}
		}
		q.waiters[k][w] = struct{}{}
	}
	q.count.Add(1)
	return w
}

func (q *Queue) Unregister(w *Waiter) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, k := range w.keys {
		delete(q.waiters[k], w)
		if len(q.waiters[k]) == 0 {
			delete(q.waiters, k)
		}
	}
	q.count.Add(-1)
}

// Blocked returns the number of waiting clients.
func (q *Queue) Blocked() int { return int(q.count.Load()) }

func (q *Queue) onEvent(ev store.Event) {
	switch ev.Op {
	case "expired", "evicted", "del", "flushall":
		return
	}
	if q.count.Load() == 0 {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for w := range q.waiters[ev.Key] {
		select {
		case w.C <- struct{}{}:
		default:
		}
	}
}
