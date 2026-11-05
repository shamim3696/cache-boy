package command_test

// Helpers for the infrastructure tests (persistence, replication, cluster).
// They live in the external test package because those packages import
// package command.

import (
	"testing"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/config"
	"cache-boy/internal/protocol/resp"
)

// Test-only data commands: the real ones are implemented elsewhere.
func init() {
	command.Register(
		&command.Spec{Name: "infratset", Arity: 3, Flags: command.FlagWrite | command.FlagDenyOOM, FirstKey: 1, LastKey: 1, Group: "test",
			Handler: func(x *command.Ctx) resp.Value {
				x.DB().Put(x.Arg(1), x.DB().NewStringValue([]byte(x.Arg(2))))
				return resp.OK
			}},
		&command.Spec{Name: "infratget", Arity: 2, Flags: command.FlagReadOnly, FirstKey: 1, LastKey: 1, Group: "test",
			Handler: func(x *command.Ctx) resp.Value {
				e := x.DB().Get(x.Arg(1))
				if e == nil {
					return resp.Null()
				}
				return resp.Bytes(e.Str().Bytes())
			}},
	)
}

func infraEngine(t *testing.T, mod func(*config.Config)) *command.Engine {
	t.Helper()
	cfg := config.Default()
	cfg.Dir = t.TempDir()
	if mod != nil {
		mod(cfg)
	}
	e := command.New(config.NewStore(cfg))
	t.Cleanup(e.Stop)
	return e
}

func exec(e *command.Engine, c *command.Client, args ...string) resp.Value {
	return e.Execute(c, args)
}

func mustOK(t *testing.T, e *command.Engine, c *command.Client, args ...string) resp.Value {
	t.Helper()
	v := e.Execute(c, args)
	if v.IsError() {
		t.Fatalf("%v: %s", args, v.Str)
	}
	return v
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func infoField(e *command.Engine, section, key string) string {
	for _, s := range e.InfoSections() {
		if s.Name != section {
			continue
		}
		for _, kv := range s.Fields {
			if kv[0] == key {
				return kv[1]
			}
		}
	}
	return ""
}
