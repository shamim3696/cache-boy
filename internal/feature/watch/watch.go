// Package watch streams key changes to clients (KEYWATCH) and to HTTP
// endpoints (webhooks).
//
// KEYWATCH subscribers register glob patterns scoped to their namespace. On
// every db event whose key matches, they receive the push
// ["keyevent", key, op] with the namespace removed from key. Each event is
// also published on the pub/sub channel "__key__:<stored key>" (message: op)
// when anyone is subscribed to pub/sub at all.
//
// Webhooks get a JSON POST {"key","op","ts"} for each matching event. Events
// go through a bounded queue (dropped when full) to worker goroutines that
// try up to 3 times with exponential backoff.
//
// The db listener runs under the event key's shard lock: it only matches
// patterns, pushes without blocking and enqueues without blocking.
package watch

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shamim3696/cache-boy/internal/feature/namespace"
	"github.com/shamim3696/cache-boy/internal/glob"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/pubsub"
	"github.com/shamim3696/cache-boy/internal/store"
)

// Subscriber receives key events. Push must not block.
type Subscriber interface {
	Push(v resp.Value) bool
	ClientID() int64
}

type pattern struct {
	ns, glob string
}

type watcher struct {
	sub      Subscriber
	patterns map[pattern]struct{}
}

const (
	queueSize   = 10000
	workers     = 4
	attempts    = 3
	httpTimeout = 5 * time.Second
)

// Webhook is a registered HTTP endpoint.
type Webhook struct {
	NS        string // namespace the pattern applies to ("" = default)
	ID        string
	Pattern   string
	URL       string
	Secret    string
	delivered atomic.Int64
	failed    atomic.Int64
}

func (w *Webhook) Delivered() int64 { return w.delivered.Load() }
func (w *Webhook) Failed() int64    { return w.failed.Load() }

type delivery struct {
	hook *Webhook
	body []byte
}

type Hub struct {
	ps *pubsub.Hub

	mu       sync.RWMutex
	patterns map[pattern]map[int64]Subscriber
	clients  map[int64]*watcher
	hooks    map[string]*Webhook // ns + "\x00" + id
	active   atomic.Int64        // len(patterns)+len(hooks), read on every write

	queue     chan delivery
	stop      chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup
	client    *http.Client
	gate      atomic.Pointer[func() bool]

	Dropped atomic.Int64 // events dropped because the queue was full

	// backoff before retry n (1-based). Tests may shorten it.
	Backoff func(n int) time.Duration
}

func New(db *store.DB, ps *pubsub.Hub) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	h := &Hub{
		ps:       ps,
		patterns: map[pattern]map[int64]Subscriber{},
		clients:  map[int64]*watcher{},
		hooks:    map[string]*Webhook{},
		queue:    make(chan delivery, queueSize),
		stop:     make(chan struct{}),
		ctx:      ctx,
		cancel:   cancel,
		client:   &http.Client{Timeout: httpTimeout},
		Backoff:  func(n int) time.Duration { return time.Duration(100<<(n-1)) * time.Millisecond },
	}
	if db != nil {
		db.Subscribe(h.Notify)
	}
	return h
}

// SetDeliveryGate installs a check run before each webhook delivery; when
// it returns false (e.g. this node is a replica) the event is not sent.
func (h *Hub) SetDeliveryGate(fn func() bool) { h.gate.Store(&fn) }

// Close stops webhook workers. Pending deliveries are abandoned.
func (h *Hub) Close() {
	h.closeOnce.Do(func() {
		close(h.stop)
		h.cancel()
		h.wg.Wait()
	})
}

// --- KEYWATCH ---

// Watch subscribes s to patterns in namespace ns and returns the
// confirmations ["keywatch", pattern, count].
func (h *Hub) Watch(s Subscriber, ns string, globs ...string) []resp.Value {
	h.mu.Lock()
	defer h.unlock()
	id := s.ClientID()
	w := h.clients[id]
	if w == nil {
		w = &watcher{sub: s, patterns: map[pattern]struct{}{}}
		h.clients[id] = w
	}
	out := make([]resp.Value, 0, len(globs))
	for _, g := range globs {
		p := pattern{ns, g}
		if _, ok := w.patterns[p]; !ok {
			w.patterns[p] = struct{}{}
			if h.patterns[p] == nil {
				h.patterns[p] = map[int64]Subscriber{}
			}
			h.patterns[p][id] = s
		}
		out = append(out, resp.Push(resp.Bulk("keywatch"), resp.Bulk(g), resp.Int(int64(len(w.patterns)))))
	}
	return out
}

// Unwatch removes patterns of s in namespace ns (all of them if none given)
// and returns the confirmations ["keyunwatch", pattern, count].
func (h *Hub) Unwatch(s Subscriber, ns string, globs ...string) []resp.Value {
	h.mu.Lock()
	defer h.unlock()
	id := s.ClientID()
	w := h.clients[id]
	if len(globs) == 0 && w != nil {
		for p := range w.patterns {
			if p.ns == ns {
				globs = append(globs, p.glob)
			}
		}
		sort.Strings(globs)
	}
	count := func() int64 {
		if w == nil {
			return 0
		}
		return int64(len(w.patterns))
	}
	out := make([]resp.Value, 0, len(globs))
	for _, g := range globs {
		p := pattern{ns, g}
		if w != nil {
			if _, ok := w.patterns[p]; ok {
				delete(w.patterns, p)
				h.unlinkLocked(p, id)
			}
		}
		out = append(out, resp.Push(resp.Bulk("keyunwatch"), resp.Bulk(g), resp.Int(count())))
	}
	if len(out) == 0 {
		out = append(out, resp.Push(resp.Bulk("keyunwatch"), resp.Null(), resp.Int(count())))
	}
	if w != nil && len(w.patterns) == 0 {
		delete(h.clients, id)
	}
	return out
}

// RemoveClient drops every pattern of a client (call on disconnect).
func (h *Hub) RemoveClient(id int64) {
	h.mu.Lock()
	defer h.unlock()
	w := h.clients[id]
	if w == nil {
		return
	}
	for p := range w.patterns {
		h.unlinkLocked(p, id)
	}
	delete(h.clients, id)
}

// Count returns how many patterns client id watches.
func (h *Hub) Count(id int64) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	if w := h.clients[id]; w != nil {
		return len(w.patterns)
	}
	return 0
}

func (h *Hub) unlinkLocked(p pattern, id int64) {
	if subs := h.patterns[p]; subs != nil {
		delete(subs, id)
		if len(subs) == 0 {
			delete(h.patterns, p)
		}
	}
}

// --- webhooks ---

func hookKey(ns, id string) string { return ns + "\x00" + id }

// AddWebhook registers (or replaces) a webhook.
func (h *Hub) AddWebhook(ns, id, pat, url, secret string) {
	h.startOnce.Do(func() {
		for i := 0; i < workers; i++ {
			h.wg.Add(1)
			go h.worker()
		}
	})
	h.mu.Lock()
	defer h.unlock()
	h.hooks[hookKey(ns, id)] = &Webhook{NS: ns, ID: id, Pattern: pat, URL: url, Secret: secret}
}

// DelWebhook removes a webhook. Returns true if it existed.
func (h *Hub) DelWebhook(ns, id string) bool {
	h.mu.Lock()
	defer h.unlock()
	k := hookKey(ns, id)
	_, ok := h.hooks[k]
	delete(h.hooks, k)
	return ok
}

// Webhooks returns the webhooks of namespace ns sorted by id.
func (h *Hub) Webhooks(ns string) []*Webhook {
	h.mu.RLock()
	defer h.mu.RUnlock()
	var out []*Webhook
	for _, w := range h.hooks {
		if w.NS == ns {
			out = append(out, w)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// Dump emits WEBHOOK.ADD for every webhook. Webhooks of a named namespace
// carry the internal option _NS.
func (h *Hub) Dump(emit func(args []string)) {
	h.mu.RLock()
	hooks := make([]*Webhook, 0, len(h.hooks))
	for _, w := range h.hooks {
		hooks = append(hooks, w)
	}
	h.mu.RUnlock()
	sort.Slice(hooks, func(i, j int) bool {
		if hooks[i].NS != hooks[j].NS {
			return hooks[i].NS < hooks[j].NS
		}
		return hooks[i].ID < hooks[j].ID
	})
	for _, w := range hooks {
		emit(AddArgs(w.NS, w.ID, w.Pattern, w.URL, w.Secret))
	}
}

// AddArgs builds the WEBHOOK.ADD command that recreates a webhook.
func AddArgs(ns, id, pat, url, secret string) []string {
	args := []string{"WEBHOOK.ADD", id, pat, url}
	if secret != "" {
		args = append(args, "SECRET", secret)
	}
	if ns != "" {
		args = append(args, "_NS", ns)
	}
	return args
}

// Sign returns the X-CacheBoy-Signature header value for body.
func Sign(secret string, body []byte) string {
	m := hmac.New(sha256.New, []byte(secret))
	m.Write(body)
	return "sha256=" + hex.EncodeToString(m.Sum(nil))
}

func (h *Hub) worker() {
	defer h.wg.Done()
	for {
		select {
		case <-h.stop:
			return
		case d := <-h.queue:
			h.deliver(d)
		}
	}
}

func (h *Hub) deliver(d delivery) {
	if g := h.gate.Load(); g != nil && !(*g)() {
		return
	}
	for n := 1; n <= attempts; n++ {
		if n > 1 {
			t := time.NewTimer(h.Backoff(n - 1))
			select {
			case <-t.C:
			case <-h.stop:
				t.Stop()
				return
			}
		}
		if h.post(d) {
			d.hook.delivered.Add(1)
			return
		}
		select {
		case <-h.stop:
			return
		default:
		}
	}
	d.hook.failed.Add(1)
}

func (h *Hub) post(d delivery) bool {
	req, err := http.NewRequestWithContext(h.ctx, http.MethodPost, d.hook.URL, bytes.NewReader(d.body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "cache-boy-webhook")
	if d.hook.Secret != "" {
		req.Header.Set("X-CacheBoy-Signature", Sign(d.hook.Secret, d.body))
	}
	res, err := h.client.Do(req)
	if err != nil {
		return false
	}
	res.Body.Close()
	return res.StatusCode >= 200 && res.StatusCode < 300
}

// --- event dispatch ---

// Notify handles one db event (it is the db listener; exported for tests).
func (h *Hub) Notify(ev store.Event) {
	switch ev.Op {
	case "flushall", "cache.release":
		return
	}
	if h.ps != nil && h.ps.HasSubscribers() {
		h.ps.Publish("__key__:"+ev.Key, ev.Op)
	}
	if h.active.Load() == 0 {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if len(h.patterns) == 0 && len(h.hooks) == 0 {
		return
	}
	ns := namespace.Of(ev.Key)
	user, ok := namespace.Strip(ns, ev.Key)
	if !ok {
		return
	}
	var msg resp.Value
	built := false
	for p, subs := range h.patterns {
		if p.ns != ns || !glob.Match(p.glob, user) {
			continue
		}
		if !built {
			msg = resp.Push(resp.Bulk("keyevent"), resp.Bulk(user), resp.Bulk(ev.Op))
			built = true
		}
		for _, s := range subs {
			s.Push(msg)
		}
	}
	var body []byte
	for _, w := range h.hooks {
		if w.NS != ns || !glob.Match(w.Pattern, user) {
			continue
		}
		if body == nil {
			body, _ = json.Marshal(struct {
				Key string `json:"key"`
				Op  string `json:"op"`
				Ts  int64  `json:"ts"`
			}{user, ev.Op, time.Now().UnixMilli()})
		}
		select {
		case h.queue <- delivery{hook: w, body: body}:
		default:
			h.Dropped.Add(1)
		}
	}
}

// unlock publishes the subscription count before releasing the write lock,
// so the per-write fast path never sees a stale zero.
func (h *Hub) unlock() {
	h.active.Store(int64(len(h.patterns) + len(h.hooks)))
	h.mu.Unlock()
}
