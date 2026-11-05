package command

import (
	"testing"
)

func TestHashCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"HSET h a 1 b 2", "2"},
		{"HSET h a 10 c 3", "1"},
		{"HSET h a", "!ERR wrong number of arguments"},
		{"HMSET h d 4", "OK"},
		{"HGET h a", "10"},
		{"HGET h nope", "nil"},
		{"HGET nope a", "nil"},
		{"HMGET h a nope d", "[10 nil 4]"},
		{"HMGET nope a", "[nil]"},
		{"HLEN h", "4"},
		{"HLEN nope", "0"},
		{"HEXISTS h a", "1"},
		{"HEXISTS h z", "0"},
		{"HGETALL h", "{a:10 b:2 c:3 d:4}"},
		{"HGETALL nope", "{}"},
		{"HKEYS h", "[a b c d]"},
		{"HVALS h", "[10 2 3 4]"},
		{"HSTRLEN h a", "2"},
		{"HSTRLEN h nope", "0"},
		{"HSETNX h a x", "0"},
		{"HSETNX h e 5", "1"},
		{"HDEL h a nope", "1"},
		{"HDEL h nope", "0"},
		{"HDEL h b c d e", "4"},
		{"EXISTS h", "0"},

		{"HINCRBY n f 5", "5"},
		{"HINCRBY n f -10", "-5"},
		{"HINCRBY n f x", "!ERR value is not an integer"},
		{"HSET n s abc big 9223372036854775807", "2"},
		{"HINCRBY n s 1", "!ERR hash value is not an integer"},
		{"HINCRBY n big 1", "!ERR increment or decrement would overflow"},
		{"HINCRBYFLOAT n g 1.5", "1.5"},
		{"HINCRBYFLOAT n g 0.1", "1.6"},
		{"HINCRBYFLOAT n s 1", "!ERR hash value is not a float"},
		{"HINCRBYFLOAT n g abc", "!ERR value is not a valid float"},
		{"HINCRBYFLOAT n g inf", "!ERR increment would produce NaN or Infinity"},
		{"HINCRBY fresh f x", "!ERR value is not an integer"},
		{"HINCRBYFLOAT fresh2 f a", "!ERR value is not a valid float"},
		{"EXISTS fresh fresh2", "0"},

		{"SET str v", "OK"},
		{"HSET str a b", "!WRONGTYPE"},
		{"HGET str a", "!WRONGTYPE"},
		{"HGETALL str", "!WRONGTYPE"},
		{"HINCRBY str a 1", "!WRONGTYPE"},
		{"HSCAN str 0", "!WRONGTYPE"},
		{"HRANDFIELD str", "!WRONGTYPE"},
		{"GET str", "v"},
	})
}

func TestHashRandAndScan(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"HSET h a 1 b 2 c 3 ab 4", "4"},
		{"HSCAN h 0", "[0 [a 1 ab 4 b 2 c 3]]"},
		{"HSCAN h 0 MATCH a*", "[0 [a 1 ab 4]]"},
		{"HSCAN h 0 MATCH a* COUNT 100 NOVALUES", "[0 [a ab]]"},
		{"HSCAN h 0 COUNT x", "!ERR value is not an integer"},
		{"HSCAN h x", "!ERR invalid cursor"},
		{"HSCAN h 0 TYPE string", "!ERR syntax error"},
		{"HSCAN nope 0", "[0 []]"},
		{"HRANDFIELD nope", "nil"},
		{"HRANDFIELD nope 3", "[]"},
		{"HRANDFIELD h 0", "[]"},
		{"HRANDFIELD h 1 BOGUS", "!ERR syntax error"},
		{"HRANDFIELD h x", "!ERR value is not an integer"},
	})
	fields := map[string]string{"a": "1", "b": "2", "c": "3", "ab": "4"}
	if v := do(e, c, "HRANDFIELD", "h"); fields[v.Str] == "" {
		t.Fatalf("HRANDFIELD: %q", v.Str)
	}
	v := do(e, c, "HRANDFIELD", "h", "10")
	seen := map[string]bool{}
	for _, f := range v.Elems {
		seen[f.Str] = true
	}
	if len(v.Elems) != 4 || len(seen) != 4 {
		t.Fatalf("HRANDFIELD 10: %s", render(v))
	}
	if v := do(e, c, "HRANDFIELD", "h", "-10"); len(v.Elems) != 10 {
		t.Fatalf("HRANDFIELD -10: %s", render(v))
	}
	v = do(e, c, "HRANDFIELD", "h", "2", "WITHVALUES")
	if len(v.Elems) != 4 || fields[v.Elems[0].Str] != v.Elems[1].Str || fields[v.Elems[2].Str] != v.Elems[3].Str {
		t.Fatalf("HRANDFIELD WITHVALUES: %s", render(v))
	}
}

func TestHashPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	mustStr(t, e, c, "0.3", "HINCRBYFLOAT", "h", "f", "0.3")
	mustStr(t, e, c, "0", "HDEL", "h", "nope")
	mustStr(t, e, c, "0", "HSETNX", "h", "f", "x")
	if got := rec.take(); len(got) != 1 || got[0] != "HSET h f 0.3" {
		t.Fatalf("propagated %q", got)
	}
}
