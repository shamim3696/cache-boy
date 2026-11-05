package command

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFeatureRateLimit(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")

	ftArr(t, e, c, []string{"1", "2", "0", "334"}, "RATELIMIT", "api", "3", "1s")
	for _, want := range []string{"1", "0"} {
		got := ftStrs(do(e, c, "RATELIMIT", "api", "3", "1000"))
		if got[0] != "1" || got[1] != want || got[2] != "0" {
			t.Fatalf("got %q, want remaining %s", got, want)
		}
	}
	v := do(e, c, "RATELIMIT", "api", "3", "1s")
	if got := ftStrs(v); got[0] != "0" || got[1] != "0" || got[2] == "0" {
		t.Fatalf("4th request: %q", got)
	}
	// PEEK consumes nothing.
	for i := 0; i < 3; i++ {
		if got := ftStrs(do(e, c, "RATELIMIT.PEEK", "api", "3", "1s")); got[0] != "0" {
			t.Fatalf("peek: %q", got)
		}
	}
	mustStr(t, e, c, "1", "RATELIMIT.RESET", "api")
	mustStr(t, e, c, "0", "RATELIMIT.RESET", "api")
	if got := ftStrs(do(e, c, "RATELIMIT.PEEK", "api", "3", "1s")); got[0] != "1" || got[1] != "3" {
		t.Fatalf("peek after reset: %q", got)
	}
	// COST.
	ftArr(t, e, c, []string{"1", "0", "0", "1000"}, "RATELIMIT", "bulk", "10", "1s", "COST", "10")
	if got := ftStrs(do(e, c, "RATELIMIT", "bulk", "10", "1s", "COST", "1")); got[0] != "0" {
		t.Fatalf("over budget: %q", got)
	}
	// Recovers after the emission interval.
	ftArr(t, e, c, []string{"1", "0", "0", "50"}, "RATELIMIT", "fast", "1", "50ms")
	time.Sleep(60 * time.Millisecond)
	if got := ftStrs(do(e, c, "RATELIMIT", "fast", "1", "50ms")); got[0] != "1" {
		t.Fatalf("after interval: %q", got)
	}

	mustErr(t, e, c, "RATELIMIT", "x", "0", "1s")
	mustErr(t, e, c, "RATELIMIT", "x", "1", "soon")
	mustErr(t, e, c, "RATELIMIT", "x", "1", "1s", "COST")
	mustErr(t, e, c, "RATELIMIT", "x", "1", "1s", "BOGUS", "1")

	// Namespaces isolate rate-limit keys.
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	if got := ftStrs(do(e, app, "RATELIMIT", "fast", "1", "1h")); got[0] != "1" {
		t.Fatalf("namespaced: %q", got)
	}
}

func TestFeatureRateLimitConcurrent(t *testing.T) {
	e := newTestEngine(t)
	var allowed atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.NewClient("tcp", "g")
			for i := 0; i < 10; i++ {
				if v := do(e, c, "RATELIMIT", "shared", "100", "1h"); v.Elems[0].Int == 1 {
					allowed.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	if n := allowed.Load(); n != 100 {
		t.Fatalf("allowed %d, want 100", n)
	}
}

func TestFeatureDuration(t *testing.T) {
	for in, want := range map[string]time.Duration{
		"250": 250 * time.Millisecond, "500ms": 500 * time.Millisecond, "10s": 10 * time.Second,
		"5m": 5 * time.Minute, "1h": time.Hour, "1.5s": 1500 * time.Millisecond, "0": 0,
	} {
		if got, ok := featureDuration(in); !ok || got != want {
			t.Errorf("%q: got %v %v, want %v", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "-1", "abc", "10d", "-5s"} {
		if _, ok := featureDuration(in); ok {
			t.Errorf("%q: expected failure", in)
		}
	}
}
