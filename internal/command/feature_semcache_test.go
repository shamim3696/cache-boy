package command

import (
	"sync"
	"testing"
)

type scRecorder struct {
	mu   sync.Mutex
	cmds [][]string
}

func (r *scRecorder) Propagate(args []string) {
	r.mu.Lock()
	r.cmds = append(r.cmds, append([]string(nil), args...))
	r.mu.Unlock()
}

func TestSemanticCache(t *testing.T) {
	e := newTestEngine(t)
	rec := &scRecorder{}
	e.AddPropagator(rec)
	c := e.NewClient("tcp", "test")

	// Miss on an unknown cache.
	if v := do(e, c, "SCACHE.GET", "llm", "[1,0,0]"); v.Elems[0].Str != "miss" {
		t.Fatalf("empty cache: %v", v)
	}
	do(e, c, "SCACHE.SET", "llm", "[1,0,0]", "Paris", "PROMPT", "capital of france?", "ID", "a")
	do(e, c, "SCACHE.SET", "llm", "[0,1,0]", "4", "ID", "b")

	// Similar vector → hit; dissimilar → miss with the best score reported.
	v := do(e, c, "SCACHE.GET", "llm", "[0.98,0.1,0]")
	if v.Elems[0].Str != "hit" || v.Elems[1].Str != "Paris" || v.Elems[3].Str != "a" {
		t.Fatalf("similar prompt: %v", v)
	}
	v = do(e, c, "SCACHE.GET", "llm", "[0.7,0.7,0]")
	if v.Elems[0].Str != "miss" || v.Elems[2].Float < 0.6 {
		t.Fatalf("below threshold: %v", v)
	}
	// A lower per-request threshold turns it into a hit.
	if v = do(e, c, "SCACHE.GET", "llm", "[0.7,0.7,0]", "THRESHOLD", "0.5"); v.Elems[0].Str != "hit" {
		t.Fatalf("custom threshold: %v", v)
	}
	// Exact prompt short-circuits even with an unrelated vector.
	if v = do(e, c, "SCACHE.GET", "llm", "[0,0,1]", "PROMPT", "capital of france?"); v.Elems[1].Str != "Paris" {
		t.Fatalf("exact prompt: %v", v)
	}
	mustErr(t, e, c, "SCACHE.SET", "llm", "[1,0]", "x") // wrong dimension
	mustErr(t, e, c, "SCACHE.GET", "llm", "not-a-vector")

	info := do(e, c, "SCACHE.INFO", "llm")
	if info.Elems[1].Int != 2 || info.Elems[3].Int != 3 {
		t.Fatalf("info: %v", info)
	}

	// Expiry: an entry with a past PXAT is never returned.
	do(e, c, "SCACHE.SET", "llm", "[0,0,1]", "old", "ID", "c", "PXAT", "1")
	if v = do(e, c, "SCACHE.GET", "llm", "[0,0,1]"); v.Elems[0].Str != "miss" {
		t.Fatalf("expired entry returned: %v", v)
	}

	// LRU eviction over MAXENTRIES: "b" was never read, so it goes first.
	mustStr(t, e, c, "OK", "SCACHE.CONFIG", "llm", "MAXENTRIES", "2")
	do(e, c, "SCACHE.SET", "llm", "[0,0.5,0.5]", "new", "ID", "d")
	if v = do(e, c, "SCACHE.GET", "llm", "[0,1,0]", "THRESHOLD", "0.99"); v.Elems[0].Str != "miss" {
		t.Fatalf("LRU victim still present: %v", v)
	}

	// Namespaces isolate caches.
	ns := e.NewClient("tcp", "ns")
	do(e, ns, "NAMESPACE", "USE", "tenant2")
	if v = do(e, ns, "SCACHE.GET", "llm", "[1,0,0]"); v.Elems[0].Str != "miss" {
		t.Fatalf("namespace leak: %v", v)
	}

	// Replaying what was propagated rebuilds the same cache elsewhere.
	e2 := newTestEngine(t)
	r := e2.InternalClient("replay", true)
	for _, cmd := range rec.cmds {
		if v := e2.Execute(r, cmd); v.IsError() {
			t.Fatalf("replay %v: %s", cmd, v.Str)
		}
	}
	c2 := e2.NewClient("tcp", "test")
	if v = do(e2, c2, "SCACHE.GET", "llm", "[0.98,0.1,0]"); v.Elems[1].Str != "Paris" {
		t.Fatalf("replayed cache: %v", v)
	}
	if info = do(e2, c2, "SCACHE.INFO", "llm"); info.Elems[1].Int != 2 {
		t.Fatalf("replayed entries: %v", info)
	}

	mustStr(t, e, c, "2", "SCACHE.DROP", "llm")
	mustErr(t, e, c, "SCACHE.INFO", "llm")
}

func TestSemanticCacheDump(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	do(e, c, "SCACHE.CONFIG", "faq", "THRESHOLD", "0.8")
	do(e, c, "SCACHE.SET", "faq", "[1,2,3]", "answer", "PROMPT", "q", "EX", "100")

	var cmds [][]string
	unlock := e.DB.LockAll()
	e.DumpModules(func(args []string) { cmds = append(cmds, args) })
	unlock()

	e2 := newTestEngine(t)
	r := e2.InternalClient("load", true)
	for _, cmd := range cmds {
		if v := e2.Execute(r, cmd); v.IsError() {
			t.Fatalf("%v: %s", cmd, v.Str)
		}
	}
	c2 := e2.NewClient("tcp", "test")
	if v := do(e2, c2, "SCACHE.GET", "faq", "[1,2,3.1]"); v.Elems[1].Str != "answer" {
		t.Fatalf("dumped cache: %v", v)
	}
	if info := do(e2, c2, "SCACHE.INFO", "faq"); info.Elems[5].Float != float64(float32(0.8)) {
		t.Fatalf("threshold not restored: %v", info)
	}
}
