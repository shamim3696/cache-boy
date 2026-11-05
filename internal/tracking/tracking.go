// Package tracking implements server-assisted client-side caching
// (CLIENT TRACKING). The server remembers which keys each client has read;
// when one of them changes, it pushes an "invalidate" message so the client
// can drop its local copy. With BCAST, clients instead get invalidations for
// every key matching their prefixes, without the server remembering reads.
package tracking

import (
	"strings"
	"sync"
	"sync/atomic"

	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

type Client interface {
	Push(v resp.Value) bool
	ClientID() int64
}

type Options struct {
	BCast    bool
	Prefixes []string
	NoLoop   bool // don't notify a client about its own writes
}

type state struct {
	c    Client
	opts Options
}

type Table struct {
	mu      sync.Mutex
	clients map[int64]*state
	keys    map[string]map[int64]struct{} // key → clients that read it
	n       atomic.Int64                  // len(clients), read without the lock on every write
}

func New(db *store.DB) *Table {
	t := &Table{clients: map[int64]*state{}, keys: map[string]map[int64]struct{}{}}
	db.Subscribe(t.onEvent)
	return t
}

func (t *Table) Enable(c Client, opts Options) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.clients[c.ClientID()] = &state{c, opts}
	t.n.Store(int64(len(t.clients)))
}

// Disable stops tracking for c. Remembered keys are cleaned lazily.
func (t *Table) Disable(c Client) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.clients, c.ClientID())
	t.n.Store(int64(len(t.clients)))
}

func (t *Table) Enabled(c Client) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.clients[c.ClientID()]
	return ok
}

// Remember records that c read keys (called after read commands).
func (t *Table) Remember(c Client, keys []string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	st, ok := t.clients[c.ClientID()]
	if !ok || st.opts.BCast {
		return
	}
	for _, k := range keys {
		if t.keys[k] == nil {
			t.keys[k] = map[int64]struct{}{}
		}
		t.keys[k][c.ClientID()] = struct{}{}
	}
}

// TrackedKeys returns how many keys are being tracked (for INFO).
func (t *Table) TrackedKeys() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.keys)
}

func (t *Table) onEvent(ev store.Event) {
	if t.n.Load() == 0 { // fast path: every write lands here
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.clients) == 0 {
		return
	}
	if ev.Op == "flushall" {
		for _, st := range t.clients {
			st.c.Push(resp.Invalidate(nil))
		}
		clear(t.keys)
		return
	}
	msg := resp.Invalidate([]string{ev.Key})
	for id := range t.keys[ev.Key] {
		if st, ok := t.clients[id]; ok && !(st.opts.NoLoop && id == ev.ClientID) {
			st.c.Push(msg)
		}
	}
	delete(t.keys, ev.Key) // each read is invalidated once, as in Redis
	for id, st := range t.clients {
		if !st.opts.BCast || (st.opts.NoLoop && id == ev.ClientID) {
			continue
		}
		if len(st.opts.Prefixes) == 0 {
			st.c.Push(msg)
			continue
		}
		for _, p := range st.opts.Prefixes {
			if strings.HasPrefix(ev.Key, p) {
				st.c.Push(msg)
				break
			}
		}
	}
}
