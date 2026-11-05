package command

import (
	"testing"

	"cache-boy/internal/config"
	"cache-boy/internal/protocol/resp"
)

// newTestEngine returns an engine with default config (no persistence).
func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e := New(config.NewStore(config.Default()))
	t.Cleanup(e.Stop)
	return e
}

// do runs a command as client c and returns the reply.
func do(e *Engine, c *Client, args ...string) resp.Value {
	return e.Execute(c, args)
}

// mustStr runs a command and fails unless the reply's string form equals want.
func mustStr(t *testing.T, e *Engine, c *Client, want string, args ...string) {
	t.Helper()
	v := do(e, c, args...)
	if v.IsError() || v.String() != want {
		t.Fatalf("%v: got %q (type %c), want %q", args, v.String(), v.Type, want)
	}
}

// mustErr runs a command and fails unless it returns an error.
func mustErr(t *testing.T, e *Engine, c *Client, args ...string) {
	t.Helper()
	if v := do(e, c, args...); !v.IsError() {
		t.Fatalf("%v: expected error, got %q", args, v.String())
	}
}

func TestCoreCommands(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	mustStr(t, e, c, "PONG", "PING")
	mustStr(t, e, c, "hi", "ECHO", "hi")
	mustErr(t, e, c, "NOSUCHCMD")
	mustErr(t, e, c, "ECHO")
	mustStr(t, e, c, "OK", "MULTI")
	mustStr(t, e, c, "QUEUED", "PING")
	if v := do(e, c, "EXEC"); len(v.Elems) != 1 || v.Elems[0].Str != "PONG" {
		t.Fatalf("EXEC: %v", v)
	}
	mustStr(t, e, c, "OK", "NAMESPACE", "USE", "app1")
	mustStr(t, e, c, "app1", "NAMESPACE", "CURRENT")
}

func TestAuth(t *testing.T) {
	e := newTestEngine(t)
	if err := e.SetConfig("requirepass", "secret"); err != nil {
		t.Fatal(err)
	}
	c := e.NewClient("tcp", "test")
	mustErr(t, e, c, "PING")
	mustErr(t, e, c, "AUTH", "wrong")
	mustStr(t, e, c, "OK", "AUTH", "secret")
	mustStr(t, e, c, "PONG", "PING")
	mustStr(t, e, c, "OK", "ACL", "SETUSER", "reader", "on", ">pw", "~public:*", "+@read", "+ping")
	r := e.NewClient("tcp", "test2")
	mustStr(t, e, r, "OK", "AUTH", "reader", "pw")
	mustStr(t, e, r, "PONG", "PING")
	mustErr(t, e, r, "FLUSHALL")
}
