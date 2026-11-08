// Package history keeps the last N versions of every string key (CONFIG SET
// history-depth N), including deletions, so old values can be read back or
// restored.
//
// A db listener records a version after every write: the new string value
// (skipped when equal to the previous version) or a deletion when the key
// is gone. Non-string values are ignored. History is kept in memory only; it
// is not persisted or replicated (each node records its own writes).
package history

import (
	"bytes"
	"sync"
	"time"

	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/store"
)

// Version is one recorded value of a key. Deleted versions have a nil Value.
type Version struct {
	N       int64
	TimeMs  int64
	Value   []byte
	Deleted bool
}

type keyHistory struct {
	versions []Version // oldest first
	next     int64
}

type Store struct {
	db   *store.DB
	cfg  *config.Store
	mu   sync.Mutex
	keys map[string]*keyHistory
}

func New(db *store.DB, cfg *config.Store) *Store {
	s := &Store{db: db, cfg: cfg, keys: map[string]*keyHistory{}}
	db.Subscribe(s.onEvent)
	return s
}

func (s *Store) depth() int {
	if s.cfg == nil {
		return 0
	}
	return s.cfg.Get().HistoryDepth
}

func (s *Store) onEvent(ev store.Event) {
	depth := s.depth()
	if depth <= 0 {
		return
	}
	switch ev.Op {
	case "flushall":
		s.mu.Lock()
		now := time.Now().UnixMilli()
		for _, h := range s.keys {
			h.add(Version{TimeMs: now, Deleted: true}, depth)
		}
		s.mu.Unlock()
		return
	case "cache.release":
		return // not a change
	}
	// The shard lock of ev.Key is held by our caller, so Peek is safe.
	e := s.db.Peek(ev.Key)
	now := time.Now().UnixMilli()
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.keys[ev.Key]
	if e == nil {
		if h != nil {
			h.add(Version{TimeMs: now, Deleted: true}, depth)
		}
		return
	}
	if e.Kind != store.KindString {
		return
	}
	v := e.Str().Bytes()
	if h == nil {
		h = &keyHistory{}
		s.keys[ev.Key] = h
	}
	h.add(Version{TimeMs: now, Value: bytes.Clone(v)}, depth)
}

// add appends v unless it repeats the latest version, then trims to depth.
func (h *keyHistory) add(v Version, depth int) {
	if n := len(h.versions); n > 0 {
		last := h.versions[n-1]
		if last.Deleted == v.Deleted && (v.Deleted || bytes.Equal(last.Value, v.Value)) {
			return
		}
	}
	h.next++
	v.N = h.next
	h.versions = append(h.versions, v)
	if over := len(h.versions) - depth; over > 0 {
		h.versions = append(h.versions[:0:0], h.versions[over:]...)
	}
}

// Versions returns up to count versions of key, newest first (count < 0: all).
func (s *Store) Versions(key string, count int) []Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.keys[key]
	if h == nil {
		return nil
	}
	n := len(h.versions)
	if count >= 0 && count < n {
		n = count
	}
	out := make([]Version, 0, n)
	for i := len(h.versions) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, h.versions[i])
	}
	return out
}

// Get returns version n of key.
func (s *Store) Get(key string, n int64) (Version, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.keys[key]
	if h == nil {
		return Version{}, false
	}
	for _, v := range h.versions {
		if v.N == n {
			return v, true
		}
	}
	return Version{}, false
}

// Clear forgets key's history. Returns true if it had any.
func (s *Store) Clear(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.keys[key]
	delete(s.keys, key)
	return ok
}

// Len returns the number of keys with history.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.keys)
}
