package command

import (
	"testing"
	"time"
)

func TestFeatureTags(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	rec := &ftRecorder{}
	e.AddPropagator(rec)

	mustStr(t, e, c, "OK", "_ft.set", "a", "1")
	mustStr(t, e, c, "OK", "_ft.set", "b", "2")
	mustStr(t, e, c, "2", "TAG.ADD", "a", "t1", "t2")
	mustStr(t, e, c, "1", "TAG.ADD", "a", "t1", "t3")
	mustStr(t, e, c, "1", "TAG.ADD", "b", "t1")
	mustStr(t, e, c, "0", "TAG.ADD", "missing", "t1")
	ftArr(t, e, c, []string{"a", "b"}, "TAG.KEYS", "t1")
	ftArr(t, e, c, []string{"t1", "t2", "t3"}, "TAG.OF", "a")
	ftArr(t, e, c, []string{"t1", "t2", "t3"}, "TAG.LIST")
	mustStr(t, e, c, "1", "TAG.REMOVE", "a", "t3", "nope")
	ftArr(t, e, c, []string{"t1", "t2"}, "TAG.LIST")

	// Deleting a key drops it from the index.
	mustStr(t, e, c, "1", "_ft.del", "b")
	ftArr(t, e, c, []string{"a"}, "TAG.KEYS", "t1")
	ftArr(t, e, c, []string{}, "TAG.OF", "b")

	// Expired keys disappear too.
	mustStr(t, e, c, "OK", "_ft.set", "tmp", "x", "PX", "20")
	mustStr(t, e, c, "1", "TAG.ADD", "tmp", "t1")
	time.Sleep(40 * time.Millisecond)
	ftArr(t, e, c, []string{"a"}, "TAG.KEYS", "t1")
	if e.Tags.Has("tmp") {
		t.Fatal("expired key still tagged")
	}

	// Dump recreates the tags.
	var dumped [][]string
	e.Tags.Dump(func(args []string) { dumped = append(dumped, args) })
	if len(dumped) != 1 || !ftEq(dumped[0], []string{"TAG.ADD", "a", "t1", "t2"}) {
		t.Fatalf("dump: %q", dumped)
	}

	// Invalidate deletes and propagates DEL for each key.
	mustStr(t, e, c, "OK", "_ft.set", "c", "3")
	mustStr(t, e, c, "1", "TAG.ADD", "c", "t2")
	before := len(rec.all())
	mustStr(t, e, c, "2", "TAG.INVALIDATE", "t2", "unknown")
	mustStr(t, e, c, "", "_ft.get", "a")
	mustStr(t, e, c, "", "_ft.get", "c")
	ftArr(t, e, c, []string{}, "TAG.LIST")
	got := rec.all()[before:]
	if len(got) != 2 || !ftEq(got[0], []string{"DEL", "a"}) || !ftEq(got[1], []string{"DEL", "c"}) {
		t.Fatalf("propagated: %q", got)
	}
	mustStr(t, e, c, "0", "TAG.INVALIDATE", "t2")
}

func TestFeatureTagsNamespacesAndFlush(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "default")
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")

	mustStr(t, e, c, "OK", "_ft.set", "k", "1")
	mustStr(t, e, app, "OK", "_ft.set", "k", "2")
	mustStr(t, e, c, "1", "TAG.ADD", "k", "shared")
	mustStr(t, e, app, "1", "TAG.ADD", "k", "shared")
	ftArr(t, e, c, []string{"k"}, "TAG.KEYS", "shared")
	ftArr(t, e, app, []string{"k"}, "TAG.KEYS", "shared")
	if n := len(e.Tags.Keys("shared")); n != 2 {
		t.Fatalf("index has %d keys", n)
	}

	// Invalidation is limited to the caller's namespace.
	mustStr(t, e, app, "1", "TAG.INVALIDATE", "shared")
	mustStr(t, e, c, "1", "_ft.get", "k")
	mustStr(t, e, app, "", "_ft.get", "k")

	mustStr(t, e, c, "OK", "FLUSHALL")
	ftArr(t, e, c, []string{}, "TAG.LIST")
}

func TestFeatureTagsMultiAndConcurrency(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	// TAG.KEYS inside MULTI (all shards already locked) must not deadlock.
	mustStr(t, e, c, "OK", "_ft.set", "m", "1")
	mustStr(t, e, c, "1", "TAG.ADD", "m", "t")
	mustStr(t, e, c, "OK", "MULTI")
	mustStr(t, e, c, "QUEUED", "TAG.KEYS", "t")
	if v := do(e, c, "EXEC"); len(v.Elems) != 1 || !ftEq(ftStrs(v.Elems[0]), []string{"m"}) {
		t.Fatalf("EXEC: %v", v)
	}

	done := make(chan struct{})
	for g := 0; g < 8; g++ {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			cl := e.NewClient("tcp", "g")
			for i := 0; i < 200; i++ {
				k := string(rune('a'+g)) + "-" + string(rune('a'+i%26))
				do(e, cl, "_ft.set", k, "v")
				do(e, cl, "TAG.ADD", k, "hot")
				if i%3 == 0 {
					do(e, cl, "_ft.del", k)
				}
				if i%50 == 0 {
					do(e, cl, "TAG.INVALIDATE", "hot")
				}
				do(e, cl, "TAG.KEYS", "hot")
			}
		}(g)
	}
	for g := 0; g < 8; g++ {
		<-done
	}
	// Every indexed key must exist.
	for _, k := range e.Tags.Keys("hot") {
		if v := do(e, c, "_ft.get", k); v.IsNull() {
			t.Fatalf("indexed key %q does not exist", k)
		}
	}
}
