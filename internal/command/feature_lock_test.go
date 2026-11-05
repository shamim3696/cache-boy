package command

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFeatureLock(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "a")
	d := e.NewClient("tcp", "b")

	v := do(e, c, "LOCK", "res", "10s", "OWNER", "worker-1")
	if v.IsError() || v.IsNull() {
		t.Fatalf("LOCK: %v", v)
	}
	tok := v.Int
	if v := do(e, d, "LOCK", "res", "10s"); !v.IsNull() {
		t.Fatalf("second LOCK: %v", v)
	}
	info := ftMap(t, do(e, d, "LOCK.INFO", "res"))
	if info["token"] != strconv.FormatInt(tok, 10) || info["owner"] != "worker-1" {
		t.Fatalf("LOCK.INFO: %v", info)
	}
	if ttl, _ := strconv.Atoi(info["ttl_ms"]); ttl <= 9000 || ttl > 10000 {
		t.Fatalf("ttl_ms %v", info["ttl_ms"])
	}
	mustStr(t, e, d, "0", "UNLOCK", "res", strconv.FormatInt(tok+1, 10))
	mustStr(t, e, c, "1", "LOCK.EXTEND", "res", strconv.FormatInt(tok, 10), "1m")
	mustStr(t, e, c, "0", "LOCK.EXTEND", "res", "1", "1m")
	mustStr(t, e, c, "1", "UNLOCK", "res", strconv.FormatInt(tok, 10))
	mustStr(t, e, c, "0", "UNLOCK", "res", strconv.FormatInt(tok, 10))
	if v := do(e, c, "LOCK.INFO", "res"); !v.IsNull() {
		t.Fatalf("LOCK.INFO after unlock: %v", v)
	}

	// Expiry, and tokens keep increasing.
	v = do(e, c, "LOCK", "short", "30ms")
	t1 := v.Int
	time.Sleep(50 * time.Millisecond)
	if v := do(e, c, "LOCK.INFO", "short"); !v.IsNull() {
		t.Fatalf("expired lock still held: %v", v)
	}
	v = do(e, d, "LOCK", "short", "1s")
	if v.IsNull() || v.Int <= t1 {
		t.Fatalf("token %v not above %d", v, t1)
	}
	mustStr(t, e, c, "0", "UNLOCK", "short", strconv.FormatInt(t1, 10))

	// WAIT: acquired once the holder releases.
	v = do(e, c, "LOCK", "w", "10s")
	holder := v.Int
	go func() {
		time.Sleep(50 * time.Millisecond)
		do(e, c, "UNLOCK", "w", strconv.FormatInt(holder, 10))
	}()
	start := time.Now()
	v = do(e, d, "LOCK", "w", "10s", "WAIT", "2000")
	if v.IsNull() || v.Int <= holder || time.Since(start) > time.Second {
		t.Fatalf("LOCK WAIT: %v after %v", v, time.Since(start))
	}
	// WAIT also wakes on expiry, and times out.
	do(e, c, "LOCK", "x", "40ms")
	if v := do(e, d, "LOCK", "x", "1s", "WAIT", "500"); v.IsNull() {
		t.Fatal("LOCK WAIT should get the lock once it expires")
	}
	start = time.Now()
	if v := do(e, c, "LOCK", "x", "1s", "WAIT", "30"); !v.IsNull() {
		t.Fatalf("LOCK WAIT timeout: %v", v)
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Fatal("LOCK WAIT returned too early")
	}

	// Namespaces isolate lock names.
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	if v := do(e, app, "LOCK", "x", "1s"); v.IsNull() {
		t.Fatal("namespaced lock should be free")
	}

	mustErr(t, e, c, "LOCK", "y", "0")
	mustErr(t, e, c, "LOCK", "y", "1s", "WAIT")
	mustErr(t, e, c, "UNLOCK", "y", "abc")
}

func TestFeatureLockMutualExclusion(t *testing.T) {
	e := newTestEngine(t)
	var inside, maxInside atomic.Int32
	var mu sync.Mutex
	var tokens []int64
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.NewClient("tcp", "g")
			for i := 0; i < 4; i++ {
				v := do(e, c, "LOCK", "mutex", "5s", "WAIT", "10000")
				if v.IsNull() || v.IsError() {
					t.Errorf("LOCK failed: %v", v)
					return
				}
				n := inside.Add(1)
				if n > maxInside.Load() {
					maxInside.Store(n)
				}
				mu.Lock()
				tokens = append(tokens, v.Int)
				mu.Unlock()
				time.Sleep(100 * time.Microsecond)
				inside.Add(-1)
				if r := do(e, c, "UNLOCK", "mutex", strconv.FormatInt(v.Int, 10)); r.Int != 1 {
					t.Errorf("UNLOCK: %v", r)
				}
			}
		}()
	}
	wg.Wait()
	if maxInside.Load() != 1 {
		t.Fatalf("%d holders at once", maxInside.Load())
	}
	if len(tokens) != 200 {
		t.Fatalf("%d acquisitions", len(tokens))
	}
	for i := 1; i < len(tokens); i++ {
		if tokens[i] <= tokens[i-1] {
			t.Fatalf("tokens not increasing in acquisition order: %d then %d", tokens[i-1], tokens[i])
		}
	}
}
