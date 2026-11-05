package scripting

import (
	"strings"
	"testing"
	"time"

	"cache-boy/internal/protocol/resp"
)

func TestRunConversionsAndCall(t *testing.T) {
	s := New()
	var calls [][]string
	env := Env{
		Keys: []string{"k1"},
		Argv: []string{"a1", "a2"},
		Call: func(args []string) resp.Value {
			calls = append(calls, args)
			switch args[0] {
			case "map":
				return resp.Map(resp.Bulk("f"), resp.Int(7))
			case "fail":
				return resp.Err("WRONGTYPE nope")
			}
			return resp.Array(resp.Bulk("x"), resp.Int(2), resp.Null())
		},
	}
	v := s.Eval("var r = cb.call('any', KEYS[0], 1.5, 3, true); return [r[0], r[1] + 1, r[2], ARGV[1]]", env)
	if len(v.Elems) != 4 || v.Elems[0].Str != "x" || v.Elems[1].Int != 3 || !v.Elems[2].IsNull() || v.Elems[3].Str != "a2" {
		t.Fatalf("got %v", v)
	}
	if want := []string{"any", "k1", "1.5", "3", "1"}; strings.Join(calls[0], ",") != strings.Join(want, ",") {
		t.Fatalf("call args %q", calls[0])
	}
	if v := s.Eval("return cb.call('map').f", env); v.Int != 7 {
		t.Fatalf("map: %v", v)
	}
	if v := s.Eval("return cb.call('fail')", env); v.Str != "WRONGTYPE nope" {
		t.Fatalf("error passthrough: %v", v)
	}
	if v := s.Eval("return {a: 1}", env); v.Type != resp.TypeMap || len(v.Elems) != 2 {
		t.Fatalf("object: %v", v)
	}
	env.Timeout = 20 * time.Millisecond
	if v := s.Eval("for(;;){}", env); !v.IsError() || !strings.Contains(v.Str, "timed out") {
		t.Fatalf("timeout: %v", v)
	}
	if s.Len() == 0 || !s.Exists(SHA1("return {a: 1}")) {
		t.Fatal("scripts not cached")
	}
}
