// Package pubsub implements PUBLISH/SUBSCRIBE with exact channels and glob patterns.
package pubsub

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Subscriber receives messages. Push must not block.
type Subscriber interface {
	Push(v resp.Value) bool
	ClientID() int64
}

type subState struct {
	channels map[string]struct{}
	patterns map[string]struct{}
}

func (s *subState) count() int { return len(s.channels) + len(s.patterns) }

type Hub struct {
	mu       sync.RWMutex
	channels map[string]map[Subscriber]struct{}
	patterns map[string]map[Subscriber]struct{}
	subs     map[Subscriber]*subState
	active   atomic.Int64 // len(channels)+len(patterns), for lock-free HasSubscribers
}

func New() *Hub {
	return &Hub{
		channels: map[string]map[Subscriber]struct{}{},
		patterns: map[string]map[Subscriber]struct{}{},
		subs:     map[Subscriber]*subState{},
	}
}

// Subscribe adds channels and returns the "subscribe" confirmation replies.
func (h *Hub) Subscribe(s Subscriber, channels ...string) []resp.Value {
	return h.add(s, channels, false)
}

// PSubscribe adds patterns and returns the "psubscribe" confirmation replies.
func (h *Hub) PSubscribe(s Subscriber, patterns ...string) []resp.Value {
	return h.add(s, patterns, true)
}

// Unsubscribe removes channels (all if none given).
func (h *Hub) Unsubscribe(s Subscriber, channels ...string) []resp.Value {
	return h.remove(s, channels, false)
}

// PUnsubscribe removes patterns (all if none given).
func (h *Hub) PUnsubscribe(s Subscriber, patterns ...string) []resp.Value {
	return h.remove(s, patterns, true)
}

// Count returns how many channels + patterns s is subscribed to.
func (h *Hub) Count(s Subscriber) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if st, ok := h.subs[s]; ok {
		return st.count()
	}
	return 0
}

// RemoveAll drops every subscription of s (on disconnect).
func (h *Hub) RemoveAll(s Subscriber) {
	h.remove(s, nil, false)
	h.remove(s, nil, true)
}

// Publish delivers msg and returns the number of receivers.
func (h *Hub) Publish(channel, msg string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	n := 0
	if subs, ok := h.channels[channel]; ok {
		m := resp.Push(resp.Bulk("message"), resp.Bulk(channel), resp.Bulk(msg))
		for s := range subs {
			s.Push(m)
			n++
		}
	}
	for p, subs := range h.patterns {
		if !matchPattern(p, channel) {
			continue
		}
		m := resp.Push(resp.Bulk("pmessage"), resp.Bulk(p), resp.Bulk(channel), resp.Bulk(msg))
		for s := range subs {
			s.Push(m)
			n++
		}
	}
	return n
}

// HasSubscribers is a lock-free check used to skip building events nobody listens to.
func (h *Hub) HasSubscribers() bool { return h.active.Load() > 0 }

// Channels lists active channels matching pattern ("" = all).
func (h *Hub) Channels(pattern string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := []string{}
	for c := range h.channels {
		if pattern == "" || matchPattern(pattern, c) {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// NumSub returns the subscriber count of a channel.
func (h *Hub) NumSub(channel string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.channels[channel])
}

// NumPat returns the number of distinct patterns.
func (h *Hub) NumPat() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.patterns)
}

func (h *Hub) add(s Subscriber, names []string, pattern bool) []resp.Value {
	h.mu.Lock()
	defer h.unlock()
	st := h.subs[s]
	if st == nil {
		st = &subState{channels: map[string]struct{}{}, patterns: map[string]struct{}{}}
		h.subs[s] = st
	}
	index, mine, kind := h.channels, st.channels, "subscribe"
	if pattern {
		index, mine, kind = h.patterns, st.patterns, "psubscribe"
	}
	out := make([]resp.Value, 0, len(names))
	for _, n := range names {
		if _, ok := mine[n]; !ok {
			mine[n] = struct{}{}
			if index[n] == nil {
				index[n] = map[Subscriber]struct{}{}
			}
			index[n][s] = struct{}{}
		}
		out = append(out, resp.Push(resp.Bulk(kind), resp.Bulk(n), resp.Int(int64(st.count()))))
	}
	return out
}

func (h *Hub) remove(s Subscriber, names []string, pattern bool) []resp.Value {
	h.mu.Lock()
	defer h.unlock()
	kind := "unsubscribe"
	if pattern {
		kind = "punsubscribe"
	}
	st := h.subs[s]
	if st == nil {
		if names == nil {
			return []resp.Value{resp.Push(resp.Bulk(kind), resp.Null(), resp.Int(0))}
		}
		out := make([]resp.Value, len(names))
		for i, n := range names {
			out[i] = resp.Push(resp.Bulk(kind), resp.Bulk(n), resp.Int(0))
		}
		return out
	}
	index, mine := h.channels, st.channels
	if pattern {
		index, mine = h.patterns, st.patterns
	}
	if names == nil {
		for n := range mine {
			names = append(names, n)
		}
		sort.Strings(names)
	}
	out := make([]resp.Value, 0, len(names))
	for _, n := range names {
		if _, ok := mine[n]; ok {
			delete(mine, n)
			delete(index[n], s)
			if len(index[n]) == 0 {
				delete(index, n)
			}
		}
		out = append(out, resp.Push(resp.Bulk(kind), resp.Bulk(n), resp.Int(int64(st.count()))))
	}
	if len(out) == 0 {
		out = append(out, resp.Push(resp.Bulk(kind), resp.Null(), resp.Int(int64(st.count()))))
	}
	if st.count() == 0 {
		delete(h.subs, s)
	}
	return out
}

// unlock publishes the subscription count before releasing the write lock,
// so the per-write fast path never sees a stale zero.
func (h *Hub) unlock() {
	h.active.Store(int64(len(h.channels) + len(h.patterns)))
	h.mu.Unlock()
}
