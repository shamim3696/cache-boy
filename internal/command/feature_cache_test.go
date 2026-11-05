package command

import (
	"sync"
	"testing"
	"time"
)

func TestFeatureCacheGet(t *testing.T) {
	e := newTestEngine(t)
	a := e.NewClient("tcp", "a")
	b := e.NewClient("tcp", "b")

	ftArr(t, e, a, []string{"compute", "<nil>"}, "CACHE.GET", "k")
	// Someone else is computing: b times out after WAIT.
	start := time.Now()
	ftArr(t, e, b, []string{"timeout", "<nil>"}, "CACHE.GET", "k", "WAIT", "50")
	if time.Since(start) < 45*time.Millisecond {
		t.Fatal("WAIT returned too early")
	}
	ftArr(t, e, b, []string{"timeout", "<nil>"}, "CACHE.GET", "k", "WAIT", "0")
	do(e, a, "_ft.set", "k", "v")
	ftArr(t, e, b, []string{"hit", "v"}, "CACHE.GET", "k")

	// RELEASE wakes a waiter, which takes over the compute lock.
	ftArr(t, e, a, []string{"compute", "<nil>"}, "CACHE.GET", "r")
	go func() {
		time.Sleep(30 * time.Millisecond)
		do(e, a, "CACHE.RELEASE", "r")
	}()
	start = time.Now()
	ftArr(t, e, b, []string{"compute", "<nil>"}, "CACHE.GET", "r", "WAIT", "2000")
	if time.Since(start) > time.Second {
		t.Fatal("RELEASE didn't wake the waiter")
	}
	mustStr(t, e, b, "1", "CACHE.RELEASE", "r")
	mustStr(t, e, b, "0", "CACHE.RELEASE", "r")

	// An expired compute lock is taken over.
	ftArr(t, e, a, []string{"compute", "<nil>"}, "CACHE.GET", "x", "LOCK", "30")
	time.Sleep(40 * time.Millisecond)
	ftArr(t, e, b, []string{"compute", "<nil>"}, "CACHE.GET", "x")

	do(e, a, "_ft.hset", "h", "f", "v")
	mustErr(t, e, a, "CACHE.GET", "h")
	mustErr(t, e, a, "CACHE.GET", "k", "LOCK", "0")
	mustErr(t, e, a, "CACHE.GET", "k", "WAIT")
	mustErr(t, e, a, "CACHE.GET", "k", "BOGUS", "1")

	// Inside MULTI there's no waiting.
	do(e, a, "CACHE.GET", "m")
	mustStr(t, e, b, "OK", "MULTI")
	mustStr(t, e, b, "QUEUED", "CACHE.GET", "m")
	if v := do(e, b, "EXEC"); !ftEq(ftStrs(v.Elems[0]), []string{"timeout", "<nil>"}) {
		t.Fatalf("EXEC: %v", v)
	}
}

func TestFeatureCacheGetStampede(t *testing.T) {
	e := newTestEngine(t)
	const clients = 20
	var mu sync.Mutex
	statuses := map[string]int{}
	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.NewClient("tcp", "c")
			v := do(e, c, "CACHE.GET", "hot", "WAIT", "5000")
			status := v.Elems[0].Str
			if status == "compute" {
				time.Sleep(100 * time.Millisecond) // the expensive query
				do(e, c, "_ft.set", "hot", "fresh")
			} else if status == "hit" && v.Elems[1].Str != "fresh" {
				t.Errorf("hit with %q", v.Elems[1].Str)
			}
			mu.Lock()
			statuses[status]++
			mu.Unlock()
		}()
	}
	wg.Wait()
	if statuses["compute"] != 1 || statuses["hit"] != clients-1 {
		t.Fatalf("statuses %v", statuses)
	}
}
