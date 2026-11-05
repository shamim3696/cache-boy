package command

import (
	"strings"
	"testing"

	"cache-boy/internal/protocol/resp"
)

func TestSetCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"SADD s a b c", "3"},
		{"SADD s a d", "1"},
		{"SMEMBERS s", "[a b c d]"},
		{"SMEMBERS nope", "[]"},
		{"SCARD s", "4"},
		{"SCARD nope", "0"},
		{"SISMEMBER s a", "1"},
		{"SISMEMBER s z", "0"},
		{"SISMEMBER nope a", "0"},
		{"SMISMEMBER s a z d", "[1 0 1]"},
		{"SREM s a z", "1"},
		{"SREM s z", "0"},
		{"SREM nope a", "0"},
		{"SREM s b c d", "3"},
		{"EXISTS s", "0"},

		{"SADD a 1 2 3 4", "4"},
		{"SADD b 3 4 5", "3"},
		{"SADD c 4 6", "2"},
		{"SINTER a b", "[3 4]"},
		{"SINTER a b c", "[4]"},
		{"SINTER a nope", "[]"},
		{"SUNION a b", "[1 2 3 4 5]"},
		{"SUNION nope c", "[4 6]"},
		{"SDIFF a b", "[1 2]"},
		{"SDIFF a b c", "[1 2]"},
		{"SDIFF a nope", "[1 2 3 4]"},
		{"SDIFF nope a", "[]"},
		{"SINTERSTORE dst a b", "2"},
		{"SMEMBERS dst", "[3 4]"},
		{"SUNIONSTORE dst a c", "5"},
		{"SMEMBERS dst", "[1 2 3 4 6]"},
		{"SDIFFSTORE dst dst a", "1"},
		{"SMEMBERS dst", "[6]"},
		{"SINTERSTORE dst a nope", "0"},
		{"EXISTS dst", "0"},
		{"SINTERCARD 2 a b", "2"},
		{"SINTERCARD 2 a b LIMIT 1", "1"},
		{"SINTERCARD 2 a b LIMIT 0", "2"},
		{"SINTERCARD 0 a", "!ERR numkeys"},
		{"SINTERCARD 3 a b", "!ERR"},
		{"SINTERCARD 1 a LIMIT -1", "!ERR LIMIT can't be negative"},
		{"SINTERCARD 1 a BOGUS", "!ERR syntax error"},

		{"SMOVE a b 1", "1"},
		{"SISMEMBER a 1", "0"},
		{"SISMEMBER b 1", "1"},
		{"SMOVE a b nope", "0"},
		{"SMOVE a fresh 2", "1"},
		{"SMEMBERS fresh", "[2]"},
		{"SMOVE fresh a 2", "1"},
		{"EXISTS fresh", "0"},
		{"SMOVE a a 2", "1"},

		{"SET str v", "OK"},
		{"SADD str a", "!WRONGTYPE"},
		{"SMEMBERS str", "!WRONGTYPE"},
		{"SINTER a str", "!WRONGTYPE"},
		{"SUNIONSTORE dst a str", "!WRONGTYPE"},
		{"SMOVE a str 2", "!WRONGTYPE"},
		{"SISMEMBER a 2", "1"},
		{"SPOP str", "!WRONGTYPE"},
		{"SSCAN str 0", "!WRONGTYPE"},
		{"SINTERSTORE str a b", "2"},
		{"TYPE str", "set"},
	})
}

func TestSetRandom(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"SADD s a b c d e", "5"},
		{"SRANDMEMBER nope", "nil"},
		{"SRANDMEMBER nope 5", "[]"},
		{"SRANDMEMBER s 0", "[]"},
		{"SRANDMEMBER s 1 2", "!ERR syntax error"},
		{"SPOP nope", "nil"},
		{"SPOP nope 3", "[]"},
		{"SPOP s 0", "[]"},
		{"SPOP s -1", "!ERR value is out of range"},
		{"SSCAN s 0 MATCH [ab]", "[0 [a b]]"},
		{"SSCAN s 0 COUNT 2", "[0 [a b c d e]]"},
	})
	if v := do(e, c, "SRANDMEMBER", "s", "10"); len(v.Elems) != 5 {
		t.Fatalf("SRANDMEMBER 10: %s", render(v))
	}
	if v := do(e, c, "SRANDMEMBER", "s", "-8"); len(v.Elems) != 8 {
		t.Fatalf("SRANDMEMBER -8: %s", render(v))
	}
	mustStr(t, e, c, "5", "SCARD", "s")

	popped := map[string]bool{}
	v := do(e, c, "SPOP", "s")
	popped[v.Str] = true
	v = do(e, c, "SPOP", "s", "3")
	if v.Type != resp.TypeSet || len(v.Elems) != 3 {
		t.Fatalf("SPOP 3: %s", render(v))
	}
	for _, m := range v.Elems {
		popped[m.Str] = true
	}
	v = do(e, c, "SPOP", "s", "10")
	for _, m := range v.Elems {
		popped[m.Str] = true
	}
	if len(popped) != 5 {
		t.Fatalf("SPOP returned %v", popped)
	}
	mustStr(t, e, c, "0", "EXISTS", "s")
}

func TestSetPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	do(e, c, "SADD", "s", "a", "b", "c")
	mustStr(t, e, c, "0", "SADD", "s", "a")
	rec.take()

	m := do(e, c, "SPOP", "s").Str
	if got := rec.take(); len(got) != 1 || got[0] != "SREM s "+m {
		t.Fatalf("SPOP propagated %q (popped %q)", got, m)
	}
	v := do(e, c, "SPOP", "s", "5")
	got := rec.take()
	if len(got) != 1 || !strings.HasPrefix(got[0], "SREM s ") || len(strings.Fields(got[0])) != 2+len(v.Elems) {
		t.Fatalf("SPOP count propagated %q", got)
	}
	do(e, c, "SPOP", "s")
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("SPOP of a missing key propagated %q", got)
	}
}
