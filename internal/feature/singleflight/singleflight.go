// Package singleflight holds the compute locks behind CACHE.GET: when a
// cached key is missing, only one client gets to recompute it while the others
// wait for the key to be written.
//
// A compute lock is cleared by any write event on its key (db listener), by
// CACHE.RELEASE, or when it expires.
package singleflight

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/shamim3696/cache-boy/internal/store"
)

type flight struct {
	owner int64
	until time.Time
}

type Group struct {
	mu    sync.Mutex
	locks map[string]flight
	n     atomic.Int64 // len(locks), read without the mutex by the listener
}

func New(db *store.DB) *Group {
	g := &Group{locks: map[string]flight{}}
	db.Subscribe(g.onEvent)
	return g
}

// Acquire takes the compute lock of key for ttl unless a live one exists.
func (g *Group) Acquire(key string, owner int64, ttl time.Duration) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	if f, ok := g.locks[key]; ok && now.Before(f.until) {
		return false
	}
	g.locks[key] = flight{owner: owner, until: now.Add(ttl)}
	g.n.Store(int64(len(g.locks)))
	return true
}

// Locked reports whether key has a live compute lock.
func (g *Group) Locked(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, ok := g.locks[key]
	if ok && !time.Now().Before(f.until) {
		g.deleteLocked(key)
		return false
	}
	return ok
}

// Release clears the compute lock of key. Returns true if a live one existed.
func (g *Group) Release(key string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	f, ok := g.locks[key]
	if !ok {
		return false
	}
	g.deleteLocked(key)
	return time.Now().Before(f.until)
}

// Len returns the number of compute locks (including expired ones not yet removed).
func (g *Group) Len() int { return int(g.n.Load()) }

func (g *Group) deleteLocked(key string) {
	delete(g.locks, key)
	g.n.Store(int64(len(g.locks)))
}

func (g *Group) onEvent(ev store.Event) {
	if g.n.Load() == 0 {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if ev.Op == "flushall" {
		g.locks = map[string]flight{}
		g.n.Store(0)
		return
	}
	if _, ok := g.locks[ev.Key]; ok {
		g.deleteLocked(ev.Key)
	}
}
