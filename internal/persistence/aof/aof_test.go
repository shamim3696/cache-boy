package aof_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/persistence/aof"
	"github.com/shamim3696/cache-boy/internal/persistence/snapshot"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

// Test-only write commands (the real data-type commands live elsewhere).
func init() {
	command.Register(
		&command.Spec{Name: "aoftset", Arity: 3, Flags: command.FlagWrite | command.FlagDenyOOM, FirstKey: 1, LastKey: 1, Group: "test",
			Handler: func(x *command.Ctx) resp.Value {
				x.DB().Put(x.Arg(1), x.DB().NewStringValue([]byte(x.Arg(2))))
				return resp.OK
			}},
		&command.Spec{Name: "aoftdel", Arity: 2, Flags: command.FlagWrite, FirstKey: 1, LastKey: 1, Group: "test",
			Handler: func(x *command.Ctx) resp.Value {
				if !x.DB().Delete(x.Arg(1)) {
					x.NoChange()
				}
				return resp.OK
			}},
	)
}

func newEngine(t *testing.T) *command.Engine {
	t.Helper()
	e := command.New(config.NewStore(config.Default()))
	t.Cleanup(e.Stop)
	return e
}

func strings(db *store.DB) map[string]string {
	unlock := db.LockAll()
	defer unlock()
	out := map[string]string{}
	db.ForEach(func(k string, e *store.Entry) bool {
		out[k] = string(e.Str().Bytes())
		return true
	})
	return out
}

func run(t *testing.T, e *command.Engine, c *command.Client, args ...string) {
	t.Helper()
	if v := e.Execute(c, args); v.IsError() {
		t.Fatalf("%v: %s", args, v.Str)
	}
}

func load(t *testing.T, path string) (*command.Engine, aof.LoadResult) {
	t.Helper()
	e := newEngine(t)
	e.SetLoading(true)
	res, err := aof.Load(path, e)
	e.SetLoading(false)
	if err != nil {
		t.Fatal(err)
	}
	return e, res
}

func TestAppendAndLoad(t *testing.T) {
	for _, policy := range []string{"always", "everysec", "no"} {
		t.Run(policy, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "appendonly.aof")
			e := newEngine(t)
			a, err := aof.Open(path, policy)
			if err != nil {
				t.Fatal(err)
			}
			e.AddPropagator(a)
			c := e.InternalClient("test", false)
			run(t, e, c, "aoftset", "a", "1")
			run(t, e, c, "aoftset", "b", "two words\r\n")
			run(t, e, c, "aoftset", "a", "3")
			run(t, e, c, "aoftdel", "b")
			run(t, e, c, "aoftdel", "missing") // NoChange: not logged
			run(t, e, c, "aoftset", "c", "")
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			got, res := load(t, path)
			if want := strings(e.DB); !reflect.DeepEqual(strings(got.DB), want) {
				t.Fatalf("loaded %v, want %v", strings(got.DB), want)
			}
			if res.Commands != 5 || res.Truncated != 0 || res.Preamble {
				t.Fatalf("load result %+v", res)
			}
		})
	}
}

func TestTruncatedTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	var b []byte
	b = resp.AppendCommand(b, []string{"aoftset", "k1", "v1"})
	b = resp.AppendCommand(b, []string{"aoftset", "k2", "v2"})
	good := len(b)
	for _, tail := range []string{"*3\r\n$7\r\naoftset\r\n$2\r\nk3\r\n$2\r\nv", "*3\r\n$7", "*", "*3\r\n$7\r\naoftset\r\n$2\r\nk3\r\n"} {
		if err := os.WriteFile(path, append(append([]byte(nil), b...), tail...), 0o644); err != nil {
			t.Fatal(err)
		}
		e, res := load(t, path)
		if res.Truncated != int64(len(tail)) {
			t.Fatalf("tail %q: truncated %d bytes, want %d", tail, res.Truncated, len(tail))
		}
		if got := strings(e.DB); !reflect.DeepEqual(got, map[string]string{"k1": "v1", "k2": "v2"}) {
			t.Fatalf("tail %q: data %v", tail, got)
		}
		if st, _ := os.Stat(path); st.Size() != int64(good) {
			t.Fatalf("file not truncated: %d bytes, want %d", st.Size(), good)
		}
	}

	// An unterminated MULTI is dropped as a whole.
	m := append([]byte(nil), b...)
	m = resp.AppendCommand(m, []string{"MULTI"})
	m = resp.AppendCommand(m, []string{"aoftset", "k9", "v9"})
	os.WriteFile(path, m, 0o644)
	e, res := load(t, path)
	if res.Truncated == 0 || len(strings(e.DB)) != 2 {
		t.Fatalf("open MULTI: %+v %v", res, strings(e.DB))
	}

	// Garbage in the middle is an error, not a silent truncation.
	bad := append([]byte(nil), b[:good/2]...)
	bad = append(bad, "?garbage\r\n"...)
	bad = append(bad, b...)
	os.WriteFile(path, bad, 0o644)
	e2 := newEngine(t)
	if _, err := aof.Load(path, e2); err == nil {
		t.Fatal("corrupt AOF loaded without error")
	}
}

func TestRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	e := newEngine(t)
	a, err := aof.Open(path, "everysec")
	if err != nil {
		t.Fatal(err)
	}
	e.AddPropagator(a)
	c := e.InternalClient("test", false)
	for i := range 2000 {
		run(t, e, c, "aoftset", fmt.Sprintf("k%d", i%50), fmt.Sprintf("v%d", i))
	}
	a.Flush(true)
	before := a.Size()

	// Keep writing from other goroutines while the rewrite runs.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.InternalClient("w", false)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				e.Execute(c, []string{"aoftset", fmt.Sprintf("g%d-%d", g, i%20), fmt.Sprint(i)})
			}
		}()
	}
	if err := a.Rewrite(e); err != nil {
		t.Fatal(err)
	}
	close(stop)
	wg.Wait()
	if !a.LastRewriteOK() || a.Rewriting() {
		t.Fatal("rewrite status")
	}
	run(t, e, c, "aoftset", "after", "rewrite")
	run(t, e, c, "aoftdel", "k1")
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	data, _ := os.ReadFile(path)
	if !bytes.HasPrefix(data, []byte(snapshot.Magic)) {
		t.Fatal("rewritten AOF has no snapshot preamble")
	}
	if int64(len(data)) >= before {
		t.Logf("note: rewritten size %d vs %d before", len(data), before)
	}
	got, res := load(t, path)
	if !res.Preamble || res.Truncated != 0 {
		t.Fatalf("load result %+v", res)
	}
	if want := strings(e.DB); !reflect.DeepEqual(strings(got.DB), want) {
		t.Fatalf("rewritten AOF loads %d keys, want %d", len(strings(got.DB)), len(want))
	}
	if _, err := os.Stat(path + ".rewrite.tmp"); !os.IsNotExist(err) {
		t.Fatal("temp file left behind")
	}
}

func TestNeedsRewrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "appendonly.aof")
	a, err := aof.Open(path, "no")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if a.NeedsRewrite(100, 10) {
		t.Fatal("empty file needs no rewrite")
	}
	for range 10 {
		a.Propagate([]string{"aoftset", "k", "v"})
	}
	if !a.NeedsRewrite(100, 10) {
		t.Fatal("grown file should be rewritten")
	}
	if a.NeedsRewrite(0, 10) || a.NeedsRewrite(100, 1<<20) {
		t.Fatal("rewrite rule ignores its limits")
	}
}
