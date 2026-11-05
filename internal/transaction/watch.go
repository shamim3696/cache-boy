// Package transaction implements WATCH: optimistic locking for MULTI/EXEC.
// A Watcher becomes dirty when any watched key is modified; EXEC then aborts.
// MULTI command queuing lives in the command package (multi.go).
package transaction

import (
	"sync"
	"sync/atomic"

	"cache-boy/internal/store"
)

type Watcher struct {
	dirty atomic.Bool
	keys  []string
}

// Dirty reports whether a watched key changed since WATCH.
func (w *Watcher) Dirty() bool { return w.dirty.Load() }

type Manager struct {
	mu       sync.Mutex
	watchers map[string]map[*Watcher]struct{}
	all      map[*Watcher]struct{}
	active   atomic.Int64 // len(all), read without the lock on every write
}

func New(db *store.DB) *Manager {
	m := &Manager{watchers: map[string]map[*Watcher]struct{}{}, all: map[*Watcher]struct{}{}}
	db.Subscribe(m.onEvent)
	return m
}

// Watch adds keys to w (creating it if nil) and returns it.
func (m *Manager) Watch(w *Watcher, keys ...string) *Watcher {
	if w == nil {
		w = &Watcher{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.all[w] = struct{}{}
	m.active.Store(int64(len(m.all)))
	for _, k := range keys {
		if m.watchers[k] == nil {
			m.watchers[k] = map[*Watcher]struct{}{}
		}
		m.watchers[k][w] = struct{}{}
		w.keys = append(w.keys, k)
	}
	return w
}

// Unwatch forgets every key of w.
func (m *Manager) Unwatch(w *Watcher) {
	if w == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, k := range w.keys {
		delete(m.watchers[k], w)
		if len(m.watchers[k]) == 0 {
			delete(m.watchers, k)
		}
	}
	delete(m.all, w)
	m.active.Store(int64(len(m.all)))
	w.keys = nil
}

func (m *Manager) onEvent(ev store.Event) {
	if m.active.Load() == 0 { // fast path: every write lands here
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ev.Op == "flushall" {
		for w := range m.all {
			w.dirty.Store(true)
		}
		return
	}
	for w := range m.watchers[ev.Key] {
		w.dirty.Store(true)
	}
}
