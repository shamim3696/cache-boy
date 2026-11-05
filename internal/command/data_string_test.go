package command

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestStringCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"GET k", "nil"},
		{"SET k v", "OK"},
		{"GET k", "v"},
		{"SET k v2 NX", "nil"},
		{"SET k v2 XX", "OK"},
		{"SET n v XX", "nil"},
		{"EXISTS n", "0"},
		{"SET k v3 GET", "v2"},
		{"SET new v GET", "nil"},
		{"SET k v4 NX GET", "v3"},
		{"GET k", "v3"},
		{"SET k v NX XX", "!ERR syntax error"},
		{"SET k v EX 10 PX 100", "!ERR syntax error"},
		{"SET k v EX 10 KEEPTTL", "!ERR syntax error"},
		{"SET k v EX 0", "!ERR invalid expire time in 'set' command"},
		{"SET k v EX abc", "!ERR value is not an integer"},
		{"SET k v EX", "!ERR syntax error"},
		{"SET k v BOGUS", "!ERR syntax error"},
		{"SET k v TAGS", "!ERR syntax error"},
		{"SET k v TAGS t1 t2", "OK"},
		{"SET k v EX 100", "OK"},
		{"TTL k", "100"},
		{"SET k v2 KEEPTTL", "OK"},
		{"TTL k", "100"},
		{"SET k v3", "OK"},
		{"TTL k", "-1"},
		{"SET k v PXAT 99999999999999", "OK"},
		{"PEXPIRETIME k", "99999999999999"},

		{"RPUSH list a", "1"},
		{"GET list", "!WRONGTYPE"},
		{"SET list v GET", "!WRONGTYPE"},
		{"SET list v", "OK"},
		{"GET list", "v"},

		{"SETNX nx 1", "1"},
		{"SETNX nx 2", "0"},
		{"GET nx", "1"},
		{"SETEX se 100 v", "OK"},
		{"TTL se", "100"},
		{"PSETEX pse 100000 v", "OK"},
		{"TTL pse", "100"},
		{"SETEX se 0 v", "!ERR invalid expire time in 'setex' command"},
		{"PSETEX se -5 v", "!ERR invalid expire time in 'psetex' command"},
		{"GETSET se new", "v"},
		{"TTL se", "-1"},
		{"GETSET none x", "nil"},
		{"GETDEL se", "new"},
		{"GETDEL se", "nil"},
		{"EXISTS se", "0"},

		{"SET ge v", "OK"},
		{"GETEX ge", "v"},
		{"GETEX ge EX 100", "v"},
		{"TTL ge", "100"},
		{"GETEX ge PERSIST", "v"},
		{"TTL ge", "-1"},
		{"GETEX ge EX 10 PERSIST", "!ERR syntax error"},
		{"GETEX missing EX 10", "nil"},
		{"GETEX ge PXAT 1", "v"},
		{"EXISTS ge", "0"},

		{"MSET a 1 b 2 c 3", "OK"},
		{"MSET a 1 b", "!ERR wrong number of arguments"},
		{"MGET a b nope list c", "[1 2 nil v 3]"},
		{"RPUSH rl x", "1"},
		{"MGET a rl", "[1 nil]"},
		{"MSETNX a 9 z 9", "0"},
		{"GET z", "nil"},
		{"MSETNX y 1 z 2", "1"},
		{"MGET y z", "[1 2]"},

		{"APPEND ap Hello", "5"},
		{"APPEND ap World", "10"},
		{"GET ap", "HelloWorld"},
		{"STRLEN ap", "10"},
		{"STRLEN nope", "0"},
		{"APPEND rl x", "!WRONGTYPE"},
		{"STRLEN rl", "!WRONGTYPE"},

		{"GETRANGE ap 0 4", "Hello"},
		{"GETRANGE ap -5 -1", "World"},
		{"GETRANGE ap 5 100", "World"},
		{"GETRANGE ap 8 2", ""},
		{"GETRANGE ap -1 -5", ""},
		{"GETRANGE ap -100 1", "He"},
		{"GETRANGE nope 0 10", ""},
		{"SUBSTR ap 0 0", "H"},
		{"GETRANGE ap a 1", "!ERR value is not an integer"},

		{"SETRANGE sr 5 hi", "7"},
		{"STRLEN sr", "7"},
		{"SETRANGE ap 0 J", "10"},
		{"GET ap", "JelloWorld"},
		{"SETRANGE ap 10 !", "11"},
		{"GET ap", "JelloWorld!"},
		{"SETRANGE ap -1 x", "!ERR offset is out of range"},
		{"SETRANGE empty 0 ", "!ERR wrong number"},
		{"SETRANGE ap 536870912 x", "!ERR string exceeds maximum allowed size"},
	})
	mustStr(t, e, c, "11", "SETRANGE", "ap", "3", "")
	mustStr(t, e, c, "0", "SETRANGE", "nokey", "3", "")
	mustStr(t, e, c, "0", "EXISTS", "nokey")
	if v := do(e, c, "GET", "sr"); v.Str != "\x00\x00\x00\x00\x00hi" {
		t.Fatalf("SETRANGE padding: %q", v.Str)
	}
}

func TestIncrDecr(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"INCR n", "1"},
		{"INCR n", "2"},
		{"INCRBY n 10", "12"},
		{"DECR n", "11"},
		{"DECRBY n 20", "-9"},
		{"INCRBY n abc", "!ERR value is not an integer"},
		{"SET s hello", "OK"},
		{"INCR s", "!ERR value is not an integer"},
		{"SET s 1.5", "OK"},
		{"INCR s", "!ERR value is not an integer"},
		{"SET big 9223372036854775807", "OK"},
		{"INCR big", "!ERR increment or decrement would overflow"},
		{"SET small -9223372036854775808", "OK"},
		{"DECR small", "!ERR increment or decrement would overflow"},
		{"DECRBY n -9223372036854775808", "!ERR decrement would overflow"},
		{"RPUSH l a", "1"},
		{"INCR l", "!WRONGTYPE"},
		{"SET t 5 EX 100", "OK"},
		{"INCR t", "6"},
		{"TTL t", "100"},

		{"INCRBYFLOAT f 10.5", "10.5"},
		{"INCRBYFLOAT f 0.1", "10.6"},
		{"INCRBYFLOAT f -5", "5.6"},
		{"SET f 3", "OK"},
		{"INCRBYFLOAT f 1.5e3", "1503"},
		{"INCRBYFLOAT f abc", "!ERR value is not a valid float"},
		{"INCRBYFLOAT f inf", "!ERR increment would produce NaN or Infinity"},
		{"INCRBYFLOAT s 1", "2.5"},
		{"INCRBYFLOAT l 1", "!WRONGTYPE"},
		{"SET word abc", "OK"},
		{"INCRBYFLOAT word 1", "!ERR value is not a valid float"},
	})
}

func TestStringPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	absOf := func(cmd string) int64 {
		f := strings.Fields(cmd)
		n, _ := strconv.ParseInt(f[len(f)-1], 10, 64)
		return n
	}

	now := time.Now().UnixMilli()
	mustStr(t, e, c, "OK", "SET", "k", "v", "EX", "10")
	got := rec.take()
	if len(got) != 1 || !strings.HasPrefix(got[0], "SET k v PXAT ") {
		t.Fatalf("SET EX propagated %q", got)
	}
	if at := absOf(got[0]); at < now+10_000 || at > time.Now().UnixMilli()+10_000 {
		t.Fatalf("SET EX propagated PXAT %d", at)
	}

	mustStr(t, e, c, "OK", "SETEX", "k", "10", "v")
	if got := rec.take(); len(got) != 1 || !strings.HasPrefix(got[0], "SET k v PXAT ") {
		t.Fatalf("SETEX propagated %q", got)
	}

	mustStr(t, e, c, "OK", "SET", "k", "v", "KEEPTTL", "TAGS", "a", "b")
	if got := rec.take(); len(got) != 1 || got[0] != "SET k v KEEPTTL TAGS a b" {
		t.Fatalf("SET KEEPTTL TAGS propagated %q", got)
	}

	if v := do(e, c, "SET", "k", "x", "NX"); !v.IsNull() {
		t.Fatalf("SET NX on existing key: %v", render(v))
	}
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("failed SET NX propagated %q", got)
	}

	mustStr(t, e, c, "1.1", "INCRBYFLOAT", "f", "1.1")
	mustStr(t, e, c, "3.3", "INCRBYFLOAT", "f", "2.2")
	if got := rec.take(); len(got) != 2 || got[0] != "SET f 1.1 KEEPTTL" || got[1] != "SET f 3.3 KEEPTTL" {
		t.Fatalf("INCRBYFLOAT propagated %q", got)
	}

	mustStr(t, e, c, "v", "GETEX", "k", "EX", "50")
	if got := rec.take(); len(got) != 1 || !strings.HasPrefix(got[0], "PEXPIREAT k ") {
		t.Fatalf("GETEX propagated %q", got)
	}
	mustStr(t, e, c, "v", "GETEX", "k")
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("plain GETEX propagated %q", got)
	}
}

func TestStringNamespaces(t *testing.T) {
	e := newTestEngine(t)
	a, b := e.NewClient("tcp", "a"), e.NewClient("tcp", "b")
	mustStr(t, e, a, "OK", "NAMESPACE", "USE", "one")
	mustStr(t, e, a, "OK", "SET", "k", "from-one")
	mustStr(t, e, b, "OK", "SET", "k", "from-default")
	mustStr(t, e, a, "from-one", "GET", "k")
	mustStr(t, e, b, "from-default", "GET", "k")
	runCases(t, e, a, []tc{{"MGET k other", "[from-one nil]"}})
}
