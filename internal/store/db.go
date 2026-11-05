// Package store is the keyspace: a map of key → Entry split into shards, each
// with its own mutex, so commands on different keys run in parallel on all CPU
// cores. (Redis runs commands on a single thread.)
//
// Locking rule: the command dispatcher locks the shards of a command's keys
// (DB.Lock) or all shards (DB.LockAll) before calling a handler. Methods in
// this file assume the caller already holds the right lock. Methods in
// shard.go that say "self-locking" take locks themselves.
package store

import (
	"errors"
	"sync/atomic"
)

var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// Event reports a change to a key. Op is the lowercase command name for
// writes ("set", "lpush"...), or "expired", "evicted", "del" for internal
// deletions. Op "flushall" has an empty Key and means every key changed.
// ClientID is the client that made the change (0 = server itself).
type Event struct {
	Key      string
	Op       string
	ClientID int64
}

type DB struct {
	shards    []*shard
	mask      uint32
	listeners atomic.Pointer[[]func(Event)]
	count     atomic.Int64
	used      atomic.Int64
	dirty     atomic.Int64

	Hits, Misses, Expired, Evicted atomic.Int64

	// CompressThreshold is read when strings are created (see NewStringValue).
	CompressThreshold atomic.Int64
}

// New creates a DB with n shards (rounded up to a power of two).
func New(n int) *DB {
	size := 1
	for size < n {
		size <<= 1
	}
	db := &DB{shards: make([]*shard, size), mask: uint32(size - 1)}
	for i := range db.shards {
		db.shards[i] = newShard()
	}
	db.listeners.Store(&[]func(Event){})
	return db
}

// Subscribe registers a listener for key events. Listeners run synchronously,
// often while shard locks are held, so they must be fast and must never lock
// DB shards themselves.
func (db *DB) Subscribe(fn func(Event)) {
	for {
		old := db.listeners.Load()
		next := append(append([]func(Event){}, *old...), fn)
		if db.listeners.CompareAndSwap(old, &next) {
			return
		}
	}
}

// Emit sends an event to all listeners.
func (db *DB) Emit(ev Event) {
	for _, fn := range *db.listeners.Load() {
		fn(ev)
	}
}

// NewStringValue creates a string entry honouring the compression threshold.
func (db *DB) NewStringValue(b []byte) *Entry {
	return NewEntry(KindString, NewString(b, int(db.CompressThreshold.Load())))
}

// Get looks up key for a command: expired keys are removed, LRU/LFU info and
// hit/miss stats are updated. Returns nil if missing.
func (db *DB) Get(key string) *Entry {
	e := db.lookup(key)
	if e == nil {
		db.Misses.Add(1)
		return nil
	}
	db.Hits.Add(1)
	e.touch(nowMs())
	return e
}

// Peek looks up key without touching access info or stats.
func (db *DB) Peek(key string) *Entry { return db.lookup(key) }

// GetKind is Get plus a type check. Returns (nil, nil) if the key is missing.
func (db *DB) GetKind(key string, kind Kind) (*Entry, error) {
	e := db.Get(key)
	if e != nil && e.Kind != kind {
		return nil, ErrWrongType
	}
	return e, nil
}

// Put inserts or replaces key. The entry's ExpireAt is kept as given.
func (db *DB) Put(key string, e *Entry) {
	s := db.shardOf(key)
	if old, ok := s.data[key]; ok {
		db.used.Add(-old.size)
	} else {
		db.count.Add(1)
	}
	e.size = entrySize(key, e)
	db.used.Add(e.size)
	if e.access == 0 {
		e.touch(nowMs())
	}
	s.data[key] = e
	if e.ExpireAt > 0 {
		s.volatile[key] = struct{}{}
	} else {
		delete(s.volatile, key)
	}
	db.dirty.Add(1)
}

// Delete removes key. It does not emit an event.
func (db *DB) Delete(key string) bool {
	s := db.shardOf(key)
	e, ok := s.data[key]
	if !ok {
		return false
	}
	delete(s.data, key)
	delete(s.volatile, key)
	db.used.Add(-e.size)
	db.count.Add(-1)
	db.dirty.Add(1)
	return true
}

// Updated must be called after a value is modified in place (LPUSH, HSET...).
// It refreshes memory accounting and deletes containers that became empty.
func (db *DB) Updated(key string) {
	s := db.shardOf(key)
	e, ok := s.data[key]
	if !ok {
		return
	}
	if isEmpty(e) {
		db.Delete(key)
		return
	}
	size := entrySize(key, e)
	db.used.Add(size - e.size)
	e.size = size
	db.dirty.Add(1)
}

// SetExpire sets an absolute expiry in unix ms (0 removes it). Returns false if key is missing.
func (db *DB) SetExpire(key string, atMs int64) bool {
	s := db.shardOf(key)
	e, ok := s.data[key]
	if !ok {
		return false
	}
	e.ExpireAt = atMs
	if atMs > 0 {
		s.volatile[key] = struct{}{}
	} else {
		delete(s.volatile, key)
	}
	db.dirty.Add(1)
	return true
}

// Len returns the number of keys (may include expired keys not yet removed).
func (db *DB) Len() int64 { return db.count.Load() }

// UsedMemory is the estimated bytes used by keys and values.
func (db *DB) UsedMemory() int64 { return db.used.Load() }

// Dirty returns the number of changes since the last ResetDirty (for save rules).
func (db *DB) Dirty() int64       { return db.dirty.Load() }
func (db *DB) ResetDirty(n int64) { db.dirty.Add(-n) }

// --- require LockAll ---

// ForEach visits every live key until fn returns false. Requires LockAll.
func (db *DB) ForEach(fn func(key string, e *Entry) bool) {
	now := nowMs()
	for _, s := range db.shards {
		for k, e := range s.data {
			if e.ExpireAt > 0 && e.ExpireAt <= now {
				continue
			}
			if !fn(k, e) {
				return
			}
		}
	}
}

// Scan iterates shard by shard. The cursor is the next shard index; 0 starts
// and a returned 0 means done. Every key present for the whole scan is
// returned at least once. Requires LockAll.
func (db *DB) Scan(cursor uint64, count int, match func(key string, e *Entry) bool) (uint64, []string) {
	if count <= 0 {
		count = 10
	}
	keys := []string{}
	now := nowMs()
	i := int(cursor)
	for ; i < len(db.shards) && len(keys) < count; i++ {
		for k, e := range db.shards[i].data {
			if e.ExpireAt > 0 && e.ExpireAt <= now {
				continue
			}
			if match == nil || match(k, e) {
				keys = append(keys, k)
			}
		}
	}
	if i >= len(db.shards) {
		return 0, keys
	}
	return uint64(i), keys
}

// Flush deletes every key. Requires LockAll. Emits a "flushall" event.
func (db *DB) Flush() {
	for _, s := range db.shards {
		s.data = map[string]*Entry{}
		s.volatile = map[string]struct{}{}
	}
	db.count.Store(0)
	db.used.Store(0)
	db.dirty.Add(1)
	db.Emit(Event{Op: "flushall"})
}

// --- internals ---

func (db *DB) shardIndex(key string) uint32 {
	// FNV-1a
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h & db.mask
}

func (db *DB) shardOf(key string) *shard { return db.shards[db.shardIndex(key)] }

func (db *DB) lookup(key string) *Entry {
	s := db.shardOf(key)
	e, ok := s.data[key]
	if !ok {
		return nil
	}
	if e.ExpireAt > 0 && e.ExpireAt <= nowMs() {
		db.Delete(key)
		db.Expired.Add(1)
		db.Emit(Event{Key: key, Op: "expired"})
		return nil
	}
	return e
}

const entryOverhead = 96 // map slot + Entry struct + key header, roughly

func entrySize(key string, e *Entry) int64 {
	return int64(len(key)) + entryOverhead + e.Value.MemSize()
}

func isEmpty(e *Entry) bool {
	switch e.Kind {
	case KindList:
		return e.List().Len() == 0
	case KindHash:
		return e.Hash().Len() == 0
	case KindSet:
		return e.Set().Len() == 0
	case KindZSet:
		return e.ZSet().Len() == 0
	}
	return false
}
