package command

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"cache-boy/internal/feature/watch"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

func TestFeatureKeyWatch(t *testing.T) {
	e := newTestEngine(t)
	w := e.NewClient("tcp", "watcher")
	c := e.NewClient("tcp", "writer")

	v := do(e, w, "KEYWATCH", "user:*", "cfg")
	if len(v.Elems) != 2 || !ftEq(ftStrs(v.Elems[1]), []string{"keywatch", "cfg", "2"}) {
		t.Fatalf("KEYWATCH reply %v", v)
	}
	do(e, c, "_ft.set", "user:1", "a")
	p := ftPush(t, w, time.Second)
	if p.Type != resp.TypePush || !ftEq(ftStrs(p), []string{"keyevent", "user:1", "_ft.set"}) {
		t.Fatalf("push %v", p)
	}
	do(e, c, "_ft.set", "other", "a")
	ftNoPush(t, w, 20*time.Millisecond)
	do(e, c, "_ft.del", "user:1")
	if p := ftPush(t, w, time.Second); !ftEq(ftStrs(p), []string{"keyevent", "user:1", "_ft.del"}) {
		t.Fatalf("push %v", p)
	}
	// Expiry is reported as "expired".
	do(e, c, "_ft.set", "cfg", "x", "PX", "10")
	ftPush(t, w, time.Second)
	time.Sleep(20 * time.Millisecond)
	do(e, c, "_ft.get", "cfg")
	if p := ftPush(t, w, time.Second); !ftEq(ftStrs(p), []string{"keyevent", "cfg", "expired"}) {
		t.Fatalf("push %v", p)
	}

	v = do(e, w, "KEYUNWATCH", "user:*")
	if !ftEq(ftStrs(v.Elems[0]), []string{"keyunwatch", "user:*", "1"}) {
		t.Fatalf("KEYUNWATCH reply %v", v)
	}
	do(e, c, "_ft.set", "user:2", "a")
	ftNoPush(t, w, 20*time.Millisecond)
	v = do(e, w, "KEYUNWATCH")
	if !ftEq(ftStrs(v.Elems[0]), []string{"keyunwatch", "cfg", "0"}) {
		t.Fatalf("KEYUNWATCH all %v", v)
	}
	do(e, w, "KEYWATCH", "*")
	e.Watch.RemoveClient(w.ID)
	do(e, c, "_ft.set", "z", "a")
	ftNoPush(t, w, 20*time.Millisecond)
	if e.Watch.Count(w.ID) != 0 {
		t.Fatal("RemoveClient left patterns")
	}
}

func TestFeatureKeyWatchNamespaces(t *testing.T) {
	e := newTestEngine(t)
	w := e.NewClient("tcp", "watcher")
	mustStr(t, e, w, "OK", "NAMESPACE", "USE", "app")
	do(e, w, "KEYWATCH", "*")
	c := e.NewClient("tcp", "writer")
	do(e, c, "_ft.set", "k", "v") // default namespace: not visible
	ftNoPush(t, w, 20*time.Millisecond)
	mustStr(t, e, c, "OK", "NAMESPACE", "USE", "app")
	do(e, c, "_ft.set", "k", "v")
	if p := ftPush(t, w, time.Second); !ftEq(ftStrs(p), []string{"keyevent", "k", "_ft.set"}) {
		t.Fatalf("push %v", p)
	}
}

type ftHookServer struct {
	*httptest.Server
	mu     sync.Mutex
	bodies [][]byte
	sigs   []string
	fail   bool
	hits   int
}

func ftNewHookServer(t *testing.T, fail bool) *ftHookServer {
	h := &ftHookServer{fail: fail}
	h.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		h.mu.Lock()
		h.hits++
		h.bodies = append(h.bodies, body)
		h.sigs = append(h.sigs, r.Header.Get("X-CacheBoy-Signature"))
		h.mu.Unlock()
		if h.fail {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	t.Cleanup(h.Close)
	return h
}

func (h *ftHookServer) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.hits
}

func TestFeatureWebhooks(t *testing.T) {
	e := newTestEngine(t)
	e.Watch.Backoff = func(int) time.Duration { return time.Millisecond }
	rec := &ftRecorder{}
	e.AddPropagator(rec)
	c := e.NewClient("tcp", "admin")
	ok := ftNewHookServer(t, false)
	bad := ftNewHookServer(t, true)

	mustStr(t, e, c, "OK", "WEBHOOK.ADD", "h1", "user:*", ok.URL, "SECRET", "s3cret")
	mustStr(t, e, c, "OK", "WEBHOOK.ADD", "h2", "user:*", bad.URL)
	mustErr(t, e, c, "WEBHOOK.ADD", "h3", "*", "ftp://example.com")
	mustErr(t, e, c, "WEBHOOK.ADD", "h3", "*", ok.URL, "SECRET")

	do(e, c, "_ft.set", "user:7", "v")
	do(e, c, "_ft.set", "nomatch", "v")
	ftEventually(t, 3*time.Second, func() bool { return ok.count() == 1 && bad.count() == 3 }, "webhook deliveries")

	ok.mu.Lock()
	body, sig := ok.bodies[0], ok.sigs[0]
	ok.mu.Unlock()
	var ev struct {
		Key string `json:"key"`
		Op  string `json:"op"`
		Ts  int64  `json:"ts"`
	}
	if err := json.Unmarshal(body, &ev); err != nil || ev.Key != "user:7" || ev.Op != "_ft.set" || ev.Ts == 0 {
		t.Fatalf("body %s (%v)", body, err)
	}
	if sig != watch.Sign("s3cret", body) || sig[:7] != "sha256=" {
		t.Fatalf("signature %q", sig)
	}
	bad.mu.Lock()
	if bad.sigs[0] != "" {
		t.Fatalf("unexpected signature without SECRET: %q", bad.sigs[0])
	}
	bad.mu.Unlock()

	ftEventually(t, time.Second, func() bool {
		v := do(e, c, "WEBHOOK.LIST")
		return len(v.Elems) == 2 &&
			ftEq(ftStrs(v.Elems[0]), []string{"h1", "user:*", ok.URL, "1", "0"}) &&
			ftEq(ftStrs(v.Elems[1]), []string{"h2", "user:*", bad.URL, "0", "1"})
	}, "WEBHOOK.LIST counters")

	// Dump and propagation recreate the webhooks.
	var dumped [][]string
	e.Watch.Dump(func(a []string) { dumped = append(dumped, a) })
	if len(dumped) != 2 || !ftEq(dumped[0], []string{"WEBHOOK.ADD", "h1", "user:*", ok.URL, "SECRET", "s3cret"}) {
		t.Fatalf("dump %q", dumped)
	}
	e2 := newTestEngine(t)
	var hookCmds [][]string // only the webhook commands: replayed writes would trigger deliveries
	for _, cmd := range rec.all() {
		if cmd[0] == "WEBHOOK.ADD" || cmd[0] == "WEBHOOK.DEL" {
			hookCmds = append(hookCmds, cmd)
		}
	}
	ftReplay(t, e2, hookCmds)
	if v := do(e2, e2.NewClient("tcp", "x"), "WEBHOOK.LIST"); len(v.Elems) != 2 {
		t.Fatalf("replicated webhooks %v", v)
	}

	mustStr(t, e, c, "1", "WEBHOOK.DEL", "h2")
	mustStr(t, e, c, "0", "WEBHOOK.DEL", "h2")
	if v := do(e, c, "WEBHOOK.LIST"); len(v.Elems) != 1 {
		t.Fatalf("WEBHOOK.LIST after DEL %v", v)
	}

	// Namespaced webhooks only see their namespace and keep it through Dump.
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	mustStr(t, e, app, "OK", "WEBHOOK.ADD", "h1", "*", ok.URL)
	if v := do(e, app, "WEBHOOK.LIST"); len(v.Elems) != 1 || v.Elems[0].Elems[1].Str != "*" {
		t.Fatalf("namespaced WEBHOOK.LIST %v", v)
	}
	do(e, app, "_ft.set", "inside", "v")
	ftEventually(t, 3*time.Second, func() bool { return ok.count() == 2 }, "namespaced delivery")
	ok.mu.Lock()
	body = ok.bodies[1]
	ok.mu.Unlock()
	if err := json.Unmarshal(body, &ev); err != nil || ev.Key != "inside" {
		t.Fatalf("namespaced body %s", body)
	}
	dumped = nil
	e.Watch.Dump(func(a []string) { dumped = append(dumped, a) })
	if len(dumped) != 2 || !ftEq(dumped[1], []string{"WEBHOOK.ADD", "h1", "*", ok.URL, "_NS", "app"}) {
		t.Fatalf("dump %q", dumped)
	}
}

func TestFeatureWebhookQueueFull(t *testing.T) {
	// Deliveries never block writers: with a stalled endpoint, events beyond
	// the bounded queue are dropped.
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	defer srv.Close()
	defer close(block)
	h := watch.New(nil, nil)
	defer h.Close()
	h.AddWebhook("", "slow", "*", srv.URL, "")
	start := time.Now()
	for i := 0; i < 20000; i++ {
		h.Notify(store.Event{Key: "k", Op: "set"})
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("enqueueing blocked")
	}
	if h.Dropped.Load() == 0 {
		t.Fatal("expected dropped events")
	}
}
