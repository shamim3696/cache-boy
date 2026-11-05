package command

import (
	"sync"
	"testing"
	"time"

	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

// Test-only data commands (the real SET/GET live in another file group).
// Names start with "_ft." so they can't collide with real commands.
func init() {
	Register(
		&Spec{Name: "_ft.set", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "test", FirstKey: 1, LastKey: 1,
			Summary: "test: _ft.set key value [PX ms]", Handler: func(x *Ctx) resp.Value {
				e := x.DB().NewStringValue([]byte(x.Arg(2)))
				if x.NArgs() == 5 && x.ArgLower(3) == "px" {
					ms, ok := x.Int(4)
					if !ok {
						return ErrNotInt
					}
					e.ExpireAt = x.Now() + ms
				}
				x.DB().Put(x.Keys[0], e)
				return resp.OK
			}},
		&Spec{Name: "_ft.get", Arity: 2, Flags: FlagReadOnly, Group: "test", FirstKey: 1, LastKey: 1,
			Summary: "test: _ft.get key", Handler: func(x *Ctx) resp.Value {
				e, err := x.DB().GetKind(x.Keys[0], store.KindString)
				if err != nil {
					return ErrWrongTyp
				}
				if e == nil {
					return resp.Null()
				}
				return resp.Bytes(e.Str().Bytes())
			}},
		&Spec{Name: "_ft.del", Arity: 2, Flags: FlagWrite, Group: "test", FirstKey: 1, LastKey: 1,
			Summary: "test: _ft.del key", Handler: func(x *Ctx) resp.Value {
				if x.DB().Peek(x.Keys[0]) == nil || !x.DB().Delete(x.Keys[0]) {
					x.NoChange()
					return resp.Int(0)
				}
				return resp.Int(1)
			}},
		&Spec{Name: "_ft.hset", Arity: 4, Flags: FlagWrite, Group: "test", FirstKey: 1, LastKey: 1,
			Summary: "test: _ft.hset key field value (creates a hash)", Handler: func(x *Ctx) resp.Value {
				h := store.NewHash()
				h.Set(x.Arg(2), x.Arg(3))
				x.DB().Put(x.Keys[0], store.NewEntry(store.KindHash, h))
				return resp.Int(1)
			}},
	)
}

// ftRecorder is a Propagator that keeps every propagated command.
type ftRecorder struct {
	mu   sync.Mutex
	cmds [][]string
}

func (r *ftRecorder) Propagate(args []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds = append(r.cmds, append([]string(nil), args...))
}

func (r *ftRecorder) all() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][]string(nil), r.cmds...)
}

// replay runs cmds on e as the replication link would.
func ftReplay(t *testing.T, e *Engine, cmds [][]string) {
	t.Helper()
	c := e.InternalClient("master", true)
	for _, cmd := range cmds {
		if v := e.Execute(c, append([]string(nil), cmd...)); v.IsError() {
			t.Fatalf("replay %q: %s", cmd, v.Str)
		}
	}
}

// strs flattens an array reply into strings (nulls become "<nil>").
func ftStrs(v resp.Value) []string {
	out := make([]string, len(v.Elems))
	for i, e := range v.Elems {
		if e.IsNull() {
			out[i] = "<nil>"
		} else {
			out[i] = e.String()
		}
	}
	return out
}

func ftEq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// mustArr runs a command and checks the flattened array reply.
func ftArr(t *testing.T, e *Engine, c *Client, want []string, args ...string) {
	t.Helper()
	v := do(e, c, args...)
	if v.IsError() {
		t.Fatalf("%v: error %s", args, v.Str)
	}
	if got := ftStrs(v); !ftEq(got, want) {
		t.Fatalf("%v: got %q, want %q", args, got, want)
	}
}

// mapOf converts a map reply to a Go map of strings.
func ftMap(t *testing.T, v resp.Value) map[string]string {
	t.Helper()
	if v.Type != resp.TypeMap {
		t.Fatalf("expected map, got %c %v", v.Type, v)
	}
	m := map[string]string{}
	for i := 0; i+1 < len(v.Elems); i += 2 {
		m[v.Elems[i].String()] = v.Elems[i+1].String()
	}
	return m
}

// nextPush waits for the next push sent to c.
func ftPush(t *testing.T, c *Client, timeout time.Duration) resp.Value {
	t.Helper()
	select {
	case v := <-c.Out():
		return v
	case <-time.After(timeout):
		t.Fatalf("no push received within %v", timeout)
	}
	return resp.Value{}
}

// noPush fails if c receives a push within d.
func ftNoPush(t *testing.T, c *Client, d time.Duration) {
	t.Helper()
	select {
	case v := <-c.Out():
		t.Fatalf("unexpected push %v", v)
	case <-time.After(d):
	}
}

// eventually polls cond until it is true or timeout passes.
func ftEventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", msg)
}
