package command

import (
	"strconv"
	"strings"
	"sync"
	"testing"

	"cache-boy/internal/feature/scripting"
	"cache-boy/internal/protocol/resp"
)

func TestFeatureEval(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	rec := &ftRecorder{}
	e.AddPropagator(rec)

	mustStr(t, e, c, "OK", "EVAL", "return cb.call('_ft.set', KEYS[0], ARGV[0])", "1", "k", "hello")
	mustStr(t, e, c, "hello", "EVAL", "return cb.call('_ft.get', KEYS[0])", "1", "k")
	if got := rec.all(); len(got) != 1 || !ftEq(got[0], []string{"_ft.set", "k", "hello"}) {
		t.Fatalf("propagated %q (EVAL itself must not be)", got)
	}

	// Return value conversions.
	for _, tc := range []struct {
		script string
		typ    resp.Type
		want   string
	}{
		{"return 'str'", resp.TypeBulk, "str"},
		{"return 42", resp.TypeInteger, "42"},
		{"return 3 * 2", resp.TypeInteger, "6"},
		{"return 1.5", resp.TypeBulk, "1.5"},
		{"return true", resp.TypeInteger, "1"},
		{"return false", resp.TypeNull, ""},
		{"return null", resp.TypeNull, ""},
		{"", resp.TypeNull, ""},
		{"return {ok: 'FINE'}", resp.TypeSimple, "FINE"},
		{"return {err: 'MYERR custom'}", resp.TypeError, "MYERR custom"},
		{"return {err: 'oops'}", resp.TypeError, "ERR oops"},
		{"return ARGV.length + KEYS.length", resp.TypeInteger, "0"},
		{"return typeof cb.call('_ft.get', 'nope')", resp.TypeBulk, "object"},
	} {
		v := do(e, c, "EVAL", tc.script, "0")
		if v.Type != tc.typ || v.String() != tc.want {
			t.Errorf("%q: got %c %q, want %c %q", tc.script, v.Type, v.String(), tc.typ, tc.want)
		}
	}
	v := do(e, c, "EVAL", "return [1, 'a', [2, null], true]", "0")
	if v.Type != resp.TypeArray || len(v.Elems) != 4 || v.Elems[0].Int != 1 || v.Elems[1].Str != "a" ||
		len(v.Elems[2].Elems) != 2 || !v.Elems[2].Elems[1].IsNull() || v.Elems[3].Int != 1 {
		t.Fatalf("array: %v", v)
	}
	// Replies converted to JS: integers are numbers, arrays are arrays.
	mustStr(t, e, c, "number", "EVAL", "return typeof cb.call('TAG.ADD', 'k', 't')", "0")
	mustStr(t, e, c, "1", "EVAL", "return cb.call('TAG.LIST').length", "0")
	mustStr(t, e, c, "3", "EVAL", "return cb.call('ECHO', 3)", "0")

	// Errors: cb.call throws the original error, cb.pcall returns it.
	do(e, c, "_ft.hset", "h", "f", "v")
	if v := do(e, c, "EVAL", "return cb.call('_ft.get', 'h')", "0"); !v.IsError() || !strings.HasPrefix(v.Str, "WRONGTYPE") {
		t.Fatalf("call error: %v", v)
	}
	mustStr(t, e, c, "WRONGTYPE", "EVAL", "return cb.pcall('_ft.get', 'h').err.split(' ')[0]", "0")
	mustStr(t, e, c, "caught", "EVAL", "try { cb.call('_ft.get', 'h') } catch (e) { return 'caught' }", "0")
	if v := do(e, c, "EVAL", "throw new Error('boom')", "0"); !v.IsError() || !strings.Contains(v.Str, "boom") {
		t.Fatalf("throw: %v", v)
	}
	if v := do(e, c, "EVAL", "return (", "0"); !v.IsError() || !strings.Contains(v.Str, "compiling") {
		t.Fatalf("syntax error: %v", v)
	}
	if v := do(e, c, "EVAL", "return cb.call('EVAL', 'return 1', '0')", "0"); !v.IsError() || !strings.Contains(v.Str, "not allowed") {
		t.Fatalf("nested EVAL: %v", v)
	}
	if v := do(e, c, "EVAL", "return cb.call('MULTI')", "0"); !v.IsError() {
		t.Fatalf("MULTI in script: %v", v)
	}
	mustErr(t, e, c, "EVAL", "return 1", "2", "onlyone")
	mustErr(t, e, c, "EVAL", "return 1", "-1")
	mustErr(t, e, c, "EVAL", "return 1", "x")

	// Timeout.
	mustStr(t, e, c, "OK", "CONFIG", "SET", "script-timeout", "100")
	if v := do(e, c, "EVAL", "while (true) {}", "0"); !v.IsError() || !strings.Contains(v.Str, "timed out") {
		t.Fatalf("timeout: %v", v)
	}
	mustStr(t, e, c, "PONG", "PING") // the server is still usable
}

func TestFeatureEvalSHA(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	src := "return ARGV[0] + ':' + KEYS[0]"
	sha := scripting.SHA1(src)
	if v := do(e, c, "EVALSHA", sha, "1", "k", "a"); !v.IsError() || !strings.HasPrefix(v.Str, "NOSCRIPT") {
		t.Fatalf("EVALSHA unknown: %v", v)
	}
	mustStr(t, e, c, sha, "SCRIPT", "LOAD", src)
	mustStr(t, e, c, "a:k", "EVALSHA", sha, "1", "k", "a")
	mustStr(t, e, c, "a:k", "EVALSHA", strings.ToUpper(sha), "1", "k", "a")
	ftArr(t, e, c, []string{"1", "0"}, "SCRIPT", "EXISTS", sha, "0000")
	mustStr(t, e, c, "OK", "SCRIPT", "FLUSH")
	ftArr(t, e, c, []string{"0"}, "SCRIPT", "EXISTS", sha)
	mustErr(t, e, c, "SCRIPT", "LOAD", "return (")
	mustErr(t, e, c, "SCRIPT", "BOGUS")
	// EVAL caches the script too.
	do(e, c, "EVAL", src, "1", "k", "b")
	ftArr(t, e, c, []string{"1"}, "SCRIPT", "EXISTS", sha)
}

func TestFeatureEvalNamespaces(t *testing.T) {
	e := newTestEngine(t)
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	mustStr(t, e, app, "k", "EVAL", "cb.call('_ft.set', KEYS[0], 'v'); return KEYS[0]", "1", "k")
	mustStr(t, e, app, "v", "_ft.get", "k")
	c := e.NewClient("tcp", "default")
	mustStr(t, e, c, "", "_ft.get", "k")
}

func TestFeatureEvalAtomic(t *testing.T) {
	e := newTestEngine(t)
	const goroutines, iterations = 10, 50
	script := `
		var n = Number(cb.call('_ft.get', KEYS[0]) || 0);
		cb.call('_ft.set', KEYS[0], String(n + 1));
		return n + 1;`
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.NewClient("tcp", "g")
			for i := 0; i < iterations; i++ {
				if v := do(e, c, "EVAL", script, "1", "counter"); v.IsError() {
					t.Errorf("EVAL: %s", v.Str)
					return
				}
				// Plain writes racing with scripts on another key.
				do(e, c, "_ft.set", "other"+strconv.Itoa(i), "x")
			}
		}()
	}
	wg.Wait()
	mustStr(t, e, e.NewClient("tcp", "check"), strconv.Itoa(goroutines*iterations), "_ft.get", "counter")
}
