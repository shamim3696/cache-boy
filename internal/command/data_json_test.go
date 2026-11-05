package command

import "testing"

func TestJSONCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"JSON.GET j", "nil"},
		{`JSON.SET j $ {"a":1,"b":[1,2],"c":{"d":"x"}}`, "OK"},
		{"JSON.GET j", `{"a":1,"b":[1,2],"c":{"d":"x"}}`},
		{"JSON.GET j $", `{"a":1,"b":[1,2],"c":{"d":"x"}}`},
		{"JSON.GET j $.a", "[1]"},
		{"JSON.GET j .c.d", `["x"]`},
		{"JSON.GET j $.b[*]", "[1,2]"},
		{"JSON.GET j $.b[-1]", "[2]"},
		{`JSON.GET j $["c"]`, `[{"d":"x"}]`},
		{"JSON.GET j $.*", `[1,[1,2],{"d":"x"}]`},
		{"JSON.GET j $.nope", "[]"},
		{"JSON.GET j $.a $.c.d", `{"$.a":[1],"$.c.d":["x"]}`},
		{"JSON.GET j $[", "!ERR invalid JSON path"},

		{"JSON.SET j $.e 5", "OK"},
		{"JSON.SET j $.a 2 NX", "nil"},
		{"JSON.SET j $.a 2 XX", "OK"},
		{"JSON.SET j $.zz 1 XX", "nil"},
		{"JSON.SET j $.f true NX", "OK"},
		{"JSON.SET j $ 1 NX", "nil"},
		{"JSON.SET j $.x.y.z 1", "nil"},
		{"JSON.SET j $.a 1 BOGUS", "!ERR syntax error"},
		{"JSON.SET j $.a {bad", "!ERR invalid JSON"},
		{"JSON.SET new $.a 1", "!ERR new objects must be created at the root"},
		{"JSON.SET new $ 1 XX", "nil"},
		{"EXISTS new", "0"},
		{"JSON.SET new $ [1,2] NX", "OK"},
		{"JSON.GET new", "[1,2]"},

		{"JSON.TYPE j", "[object]"},
		{"JSON.TYPE j $.a", "[integer]"},
		{"JSON.TYPE j $.b", "[array]"},
		{"JSON.TYPE j $.c.d", "[string]"},
		{"JSON.TYPE j $.f", "[boolean]"},
		{"JSON.TYPE j $.nope", "[]"},
		{"JSON.TYPE nope", "nil"},

		{"JSON.NUMINCRBY j $.a 1.5", "[3.5]"},
		{"JSON.NUMINCRBY j $.a -0.5", "[3]"},
		{"JSON.TYPE j $.a", "[integer]"},
		{"JSON.NUMINCRBY j $.c 1", "!ERR wrong JSON type"},
		{"JSON.NUMINCRBY j $.nope 1", "!ERR path does not exist"},
		{"JSON.NUMINCRBY j $.a x", "!ERR value is not a valid float"},
		{"JSON.NUMINCRBY nope $.a 1", "!ERR could not perform this operation on a key that doesn't exist"},

		{`JSON.ARRAPPEND j $.b 3 "x"`, "[4]"},
		{"JSON.GET j $.b", `[[1,2,3,"x"]]`},
		{"JSON.ARRAPPEND j $.a 1", "!ERR wrong JSON type"},
		{"JSON.ARRAPPEND j $.b {bad", "!ERR invalid JSON"},
		{"JSON.ARRAPPEND nope $ 1", "!ERR could not perform"},
		{"JSON.ARRLEN j $.b", "[4]"},
		{"JSON.ARRLEN j $.*", "[nil 4 nil nil nil]"},
		{"JSON.ARRLEN nope", "nil"},
		{"JSON.ARRLEN new", "[2]"},
		{"JSON.OBJKEYS j", "[[a b c e f]]"},
		{"JSON.OBJKEYS j $.c", "[[d]]"},
		{"JSON.OBJKEYS j $.a", "[nil]"},

		{"JSON.DEL j $.b[0]", "1"},
		{"JSON.GET j $.b", `[[2,3,"x"]]`},
		{"JSON.DEL j $.b[*]", "3"},
		{"JSON.GET j $.b", "[[]]"},
		{"JSON.DEL j $.nope", "0"},
		{"JSON.DEL j $.c.d", "1"},
		{"JSON.GET j", `{"a":3,"b":[],"c":{},"e":5,"f":true}`},
		{"JSON.DEL nope", "0"},
		{"JSON.DEL j $", "1"},
		{"EXISTS j", "0"},

		{`JSON.SET m1 $ {"a":1}`, "OK"},
		{`JSON.SET m2 $ {"a":"two"}`, "OK"},
		{"SET str v", "OK"},
		{"JSON.MGET m1 m2 nope str $.a", `[[1] ["two"] nil nil]`},
		{"JSON.MGET m1 $", `[{"a":1}]`},
		{"JSON.GET str", "!WRONGTYPE"},
		{"JSON.SET str $ 1", "!WRONGTYPE"},
		{"JSON.DEL str", "!WRONGTYPE"},
		{"GET m1", "!WRONGTYPE"},
		{"TYPE m1", "json"},
	})
}

func TestJSONPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	mustStr(t, e, c, "OK", "JSON.SET", "j", "$", `{"a":1}`)
	if v := do(e, c, "JSON.SET", "j", "$.a", "2", "NX"); !v.IsNull() {
		t.Fatalf("JSON.SET NX: %s", render(v))
	}
	mustStr(t, e, c, "0", "JSON.DEL", "j", "$.nope")
	if got := rec.take(); len(got) != 1 || got[0] != `JSON.SET j $ {"a":1}` {
		t.Fatalf("propagated %q", got)
	}
}
