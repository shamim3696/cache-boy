// Package tags keeps a two-way index between keys and tags, so every key
// carrying a tag can be found (and deleted) in one call.
//
// Keys are indexed by their stored (namespaced) name; tags are global. A db
// listener drops keys from the index when they are deleted, expire, are
// evicted, or are left missing by any write; FLUSHALL clears the index.
//
// Lock order: shard locks (held by the dispatcher or the db listener caller)
// are always taken before Index.mu, never after.
package tags

import (
	"sort"
	"sync"

	"cache-boy/internal/store"
)

type set = map[string]struct{}

type Index struct {
	db    *store.DB
	mu    sync.RWMutex
	byTag map[string]set // tag → keys
	byKey map[string]set // key → tags
}

func New(db *store.DB) *Index {
	x := &Index{db: db, byTag: map[string]set{}, byKey: map[string]set{}}
	db.Subscribe(x.onEvent)
	return x
}

// Tag attaches tags to key and returns how many were new. The caller must
// hold the key's shard lock and the key must exist.
func (x *Index) Tag(key string, tags ...string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	n := 0
	for _, t := range tags {
		ks := x.byKey[key]
		if ks == nil {
			ks = set{}
			x.byKey[key] = ks
		}
		if _, ok := ks[t]; ok {
			continue
		}
		ks[t] = struct{}{}
		keys := x.byTag[t]
		if keys == nil {
			keys = set{}
			x.byTag[t] = keys
		}
		keys[key] = struct{}{}
		n++
	}
	return n
}

// Untag removes tags from key and returns how many were removed.
func (x *Index) Untag(key string, tags ...string) int {
	x.mu.Lock()
	defer x.mu.Unlock()
	ks := x.byKey[key]
	if ks == nil {
		return 0
	}
	n := 0
	for _, t := range tags {
		if _, ok := ks[t]; !ok {
			continue
		}
		delete(ks, t)
		x.unlinkTag(t, key)
		n++
	}
	if len(ks) == 0 {
		delete(x.byKey, key)
	}
	return n
}

// RemoveKey forgets every tag of key.
func (x *Index) RemoveKey(key string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.removeKeyLocked(key)
}

// Keys returns the (stored) keys carrying tag, sorted.
func (x *Index) Keys(tag string) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return sorted(x.byTag[tag])
}

// KeysOf returns the union of the keys carrying any of tags, sorted.
func (x *Index) KeysOf(tags ...string) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	u := set{}
	for _, t := range tags {
		for k := range x.byTag[t] {
			u[k] = struct{}{}
		}
	}
	return sorted(u)
}

// Of returns the tags of key, sorted.
func (x *Index) Of(key string) []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return sorted(x.byKey[key])
}

// List returns every tag in use, sorted.
func (x *Index) List() []string {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := make([]string, 0, len(x.byTag))
	for t := range x.byTag {
		out = append(out, t)
	}
	sort.Strings(out)
	return out
}

// Has reports whether key has any tag.
func (x *Index) Has(key string) bool {
	x.mu.RLock()
	defer x.mu.RUnlock()
	return len(x.byKey[key]) > 0
}

// Clear drops the whole index.
func (x *Index) Clear() {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.byTag = map[string]set{}
	x.byKey = map[string]set{}
}

// Dump emits TAG.ADD for every tagged key (after the keys themselves are
// restored, TAG.ADD re-attaches the tags).
func (x *Index) Dump(emit func(args []string)) {
	x.mu.RLock()
	keys := make([]string, 0, len(x.byKey))
	for k := range x.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	cmds := make([][]string, 0, len(keys))
	for _, k := range keys {
		cmds = append(cmds, append([]string{"TAG.ADD", k}, sorted(x.byKey[k])...))
	}
	x.mu.RUnlock()
	for _, c := range cmds {
		emit(c)
	}
}

func (x *Index) onEvent(ev store.Event) {
	switch ev.Op {
	case "flushall":
		x.Clear()
		return
	case "expired", "evicted", "del":
		if x.Has(ev.Key) {
			x.RemoveKey(ev.Key)
		}
		return
	}
	if !x.Has(ev.Key) {
		return
	}
	// The shard lock of ev.Key is held by our caller, so Peek is safe. It
	// may emit "expired" (re-entering onEvent), so Index.mu must not be held.
	if x.db.Peek(ev.Key) == nil {
		x.RemoveKey(ev.Key)
	}
}

func (x *Index) removeKeyLocked(key string) {
	for t := range x.byKey[key] {
		x.unlinkTag(t, key)
	}
	delete(x.byKey, key)
}

func (x *Index) unlinkTag(t, key string) {
	if keys := x.byTag[t]; keys != nil {
		delete(keys, key)
		if len(keys) == 0 {
			delete(x.byTag, t)
		}
	}
}

func sorted(s set) []string {
	out := make([]string, 0, len(s))
	for k := range s {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
