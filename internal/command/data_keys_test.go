package command

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestKeyspaceCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"SET a 1", "OK"},
		{"SET b 2", "OK"},
		{"RPUSH l x", "1"},
		{"HSET h f v", "1"},
		{"SADD s m", "1"},
		{"ZADD z 1 m", "1"},
		{"EXISTS a b nope a", "3"},
		{"TOUCH a nope", "1"},
		{"TYPE a", "string"},
		{"TYPE l", "list"},
		{"TYPE h", "hash"},
		{"TYPE s", "set"},
		{"TYPE z", "zset"},
		{"TYPE nope", "none"},
		{"KEYS *", "[a b h l s z]"},
		{"KEYS [ab]", "[a b]"},
		{"KEYS ?", "[a b h l s z]"},
		{"KEYS x*", "[]"},
		{"DEL a nope", "1"},
		{"UNLINK b", "1"},
		{"DEL nope", "0"},
		{"EXISTS a b", "0"},

		{"SET src hello", "OK"},
		{"RENAME src dst", "OK"},
		{"GET dst", "hello"},
		{"EXISTS src", "0"},
		{"RENAME nope x", "!ERR no such key"},
		{"RENAME dst dst", "OK"},
		{"SET other 1", "OK"},
		{"RENAMENX dst other", "0"},
		{"RENAMENX dst fresh", "1"},
		{"GET fresh", "hello"},

		{"COPY fresh cp", "1"},
		{"COPY fresh cp", "0"},
		{"SET fresh changed", "OK"},
		{"COPY fresh cp REPLACE", "1"},
		{"GET cp", "changed"},
		{"COPY fresh fresh", "!ERR"},
		{"COPY nope x", "0"},
		{"COPY fresh x BOGUS", "!ERR syntax error"},

		{"OBJECT ENCODING nope", "nil"},
		{"OBJECT REFCOUNT cp", "1"},
		{"OBJECT BOGUS cp", "!ERR"},
	})
}

func TestCopyIsDeep(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"RPUSH l a b", "2"},
		{"HSET h f 1", "1"},
		{"SADD s a", "1"},
		{"ZADD z 1 a", "1"},
		{`JSON.SET j $ {"a":[1]}`, "OK"},
		{"TS.ADD t 1 1.5", "1"},
		{"VADD v id 1 0", "1"},
		{"PEXPIRE l 100000", "1"},
	})
	for _, k := range []string{"l", "h", "s", "z", "j", "t", "v"} {
		mustStr(t, e, c, "1", "COPY", k, k+"2")
		mustStr(t, e, c, render(do(e, c, "TYPE", k)), "TYPE", k+"2")
	}
	if ttl := do(e, c, "PTTL", "l2"); ttl.Int <= 0 {
		t.Fatalf("COPY lost the TTL: %v", ttl.Int)
	}
	runCases(t, e, c, []tc{
		{"RPUSH l2 c", "3"},
		{"LLEN l", "2"},
		{"HSET h2 g 2", "1"},
		{"HLEN h", "1"},
		{"SADD s2 b", "1"},
		{"SCARD s", "1"},
		{"ZADD z2 2 b", "1"},
		{"ZCARD z", "1"},
		{"JSON.ARRAPPEND j2 $.a 2", "[2]"},
		{"JSON.GET j", `{"a":[1]}`},
		{"TS.ADD t2 2 1", "2"},
		{"TS.INFO t", "{totalSamples:1 retentionTime:0 firstTimestamp:1 lastTimestamp:1 labels:{}}"},
		{"VADD v2 id2 0 1", "1"},
		{"VCARD v", "1"},
	})
}

func TestObjectEncoding(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"SET i 12345", "OK"},
		{"SET lead 0123", "OK"},
		{"SET s hello", "OK"},
		{"SET r " + strings.Repeat("x", 45), "OK"},
		{"RPUSH l a", "1"},
		{"HSET h a b", "1"},
		{"SADD st a", "1"},
		{"ZADD z 1 a", "1"},
		{"JSON.SET j $ 1", "OK"},
		{"TS.CREATE t", "OK"},
		{"VADD v a 1", "1"},
		{"OBJECT ENCODING i", "int"},
		{"OBJECT ENCODING lead", "embstr"},
		{"OBJECT ENCODING s", "embstr"},
		{"OBJECT ENCODING r", "raw"},
		{"OBJECT ENCODING l", "quicklist"},
		{"OBJECT ENCODING h", "listpack"},
		{"OBJECT ENCODING st", "listpack"},
		{"OBJECT ENCODING z", "skiplist"},
		{"OBJECT ENCODING j", "json"},
		{"OBJECT ENCODING t", "timeseries"},
		{"OBJECT ENCODING v", "vectorset"},
		{"OBJECT IDLETIME s", "0"},
	})
	e.DB.CompressThreshold.Store(100)
	mustStr(t, e, c, "OK", "SET", "big", strings.Repeat("abc", 1000))
	mustStr(t, e, c, "compressed", "OBJECT", "ENCODING", "big")
	mustStr(t, e, c, strings.Repeat("abc", 1000), "GET", "big")
	mustStr(t, e, c, "3003", "APPEND", "big", "xyz")
	mustStr(t, e, c, "compressed", "OBJECT", "ENCODING", "big")
	if v := do(e, c, "OBJECT", "FREQ", "s"); v.IsError() {
		t.Fatal(v.Str)
	}
}

func TestExpire(t *testing.T) {
	e, c := newDataEngine(t)
	future := strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)
	runCases(t, e, c, []tc{
		{"SET k v", "OK"},
		{"TTL k", "-1"},
		{"PTTL k", "-1"},
		{"EXPIRETIME k", "-1"},
		{"TTL nope", "-2"},
		{"PEXPIRETIME nope", "-2"},
		{"EXPIRE nope 10", "0"},
		{"EXPIRE k 100 XX", "0"},
		{"EXPIRE k 100 GT", "0"},
		{"EXPIRE k 100 NX", "1"},
		{"TTL k", "100"},
		{"EXPIRE k 200 NX", "0"},
		{"EXPIRE k 50 GT", "0"},
		{"EXPIRE k 200 GT", "1"},
		{"EXPIRE k 300 LT", "0"},
		{"EXPIRE k 150 LT", "1"},
		{"TTL k", "150"},
		{"EXPIRE k 10 NX XX", "!ERR NX and XX"},
		{"EXPIRE k 10 GT LT", "!ERR GT and LT"},
		{"EXPIRE k 10 BOGUS", "!ERR Unsupported option"},
		{"EXPIRE k abc", "!ERR value is not an integer"},
		{"EXPIRE k 9223372036854775807", "!ERR invalid expire time"},
		{"PERSIST k", "1"},
		{"PERSIST k", "0"},
		{"TTL k", "-1"},
		{"EXPIRE k 10 LT", "1"}, // no TTL counts as infinite
		{"EXPIREAT k " + future, "1"},
		{"EXPIRETIME k", future},
		{"PEXPIREAT k " + future + "000", "1"},
		{"PEXPIRETIME k", future + "000"},
		{"PEXPIRE k 100000", "1"},
		{"EXISTS k", "1"},
		{"EXPIRE k -1", "1"},
		{"EXISTS k", "0"},
		{"SET k v EX 100", "OK"},
		{"EXPIREAT k 1", "1"},
		{"EXISTS k", "0"},
	})

	mustStr(t, e, c, "OK", "SET", "short", "v", "PX", "30")
	time.Sleep(60 * time.Millisecond)
	runCases(t, e, c, []tc{
		{"GET short", "nil"},
		{"TTL short", "-2"},
		{"EXISTS short", "0"},
		{"DEL short", "0"},
	})

	// RENAME keeps the TTL.
	mustStr(t, e, c, "OK", "SET", "a", "v", "EX", "100")
	mustStr(t, e, c, "OK", "RENAME", "a", "b")
	mustStr(t, e, c, "100", "TTL", "b")
}

func TestExpirePropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	do(e, c, "SET", "k", "v")
	rec.take()

	before := time.Now().UnixMilli()
	mustStr(t, e, c, "1", "EXPIRE", "k", "100")
	got := rec.take()
	if len(got) != 1 || !strings.HasPrefix(got[0], "PEXPIREAT k ") {
		t.Fatalf("EXPIRE propagated %q", got)
	}
	at, _ := strconv.ParseInt(strings.Fields(got[0])[2], 10, 64)
	if at < before+100_000 || at > time.Now().UnixMilli()+100_000 {
		t.Fatalf("PEXPIREAT %d is not now+100s", at)
	}

	mustStr(t, e, c, "0", "EXPIRE", "k", "10", "NX")
	mustStr(t, e, c, "0", "EXPIRE", "missing", "10")
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("no-op EXPIRE propagated %q", got)
	}

	mustStr(t, e, c, "1", "EXPIRE", "k", "0")
	if got := rec.take(); len(got) != 1 || got[0] != "DEL k" {
		t.Fatalf("expire in the past propagated %q", got)
	}

	mustStr(t, e, c, "0", "DEL", "k")
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("DEL of a missing key propagated %q", got)
	}
}

func TestKeysAndScanNamespaces(t *testing.T) {
	e := newTestEngine(t)
	def := e.NewClient("tcp", "def")
	app := e.NewClient("tcp", "app")
	other := e.NewClient("tcp", "other")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	mustStr(t, e, other, "OK", "NAMESPACE", "USE", "other")

	for i := 0; i < 30; i++ {
		n := strconv.Itoa(i)
		do(e, def, "SET", "d"+n, n)
		do(e, app, "SET", "a"+n, n)
		do(e, other, "SET", "o"+n, n)
	}
	do(e, app, "RPUSH", "alist", "x")

	scanAll := func(c *Client, extra ...string) []string {
		var out []string
		cursor := "0"
		for {
			v := do(e, c, append([]string{"SCAN", cursor}, extra...)...)
			if v.IsError() {
				t.Fatal(v.Str)
			}
			cursor = v.Elems[0].Str
			for _, k := range v.Elems[1].Elems {
				out = append(out, k.Str)
			}
			if cursor == "0" {
				return sortStrings(out)
			}
		}
	}

	if got := scanAll(app, "COUNT", "5"); len(got) != 31 || got[0] != "a0" {
		t.Fatalf("SCAN in app: %d keys %v", len(got), got)
	}
	if got := scanAll(app, "MATCH", "a1*"); len(got) != 11 {
		t.Fatalf("SCAN MATCH: %v", got)
	}
	if got := scanAll(app, "TYPE", "list"); len(got) != 1 || got[0] != "alist" {
		t.Fatalf("SCAN TYPE: %v", got)
	}
	for _, k := range scanAll(def) {
		if k[0] != 'd' {
			t.Fatalf("default namespace sees %q", k)
		}
	}
	if v := do(e, app, "KEYS", "*"); len(v.Elems) != 31 {
		t.Fatalf("KEYS in app: %d", len(v.Elems))
	}
	if v := do(e, other, "KEYS", "a*"); len(v.Elems) != 0 {
		t.Fatalf("other sees app keys: %v", render(v))
	}
	if v := do(e, def, "KEYS", "*"); len(v.Elems) != 30 {
		t.Fatalf("KEYS in default: %d", len(v.Elems))
	}
	for i := 0; i < 20; i++ {
		if k := do(e, app, "RANDOMKEY").Str; k[0] != 'a' {
			t.Fatalf("RANDOMKEY in app returned %q", k)
		}
	}
	mustErr(t, e, app, "SCAN", "abc")
	mustErr(t, e, app, "SCAN", "0", "COUNT", "0")
	mustErr(t, e, app, "SCAN", "0", "BOGUS")

	empty := e.NewClient("tcp", "empty")
	mustStr(t, e, empty, "OK", "NAMESPACE", "USE", "empty")
	if v := do(e, empty, "RANDOMKEY"); !v.IsNull() {
		t.Fatalf("RANDOMKEY in empty namespace: %v", render(v))
	}
	mustStr(t, e, app, "0", "EXISTS", "d1")
	mustStr(t, e, app, "1", "DEL", "a1")
	mustStr(t, e, def, "1", "EXISTS", "d1")
}

func TestMultiExecWithDataCommands(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	runCases(t, e, c, []tc{
		{"MULTI", "OK"},
		{"SET k 1 EX 100", "QUEUED"},
		{"INCR k", "QUEUED"},
		{"RPUSH l a b", "QUEUED"},
		{"BLPOP empty 0", "QUEUED"},
		{"LPOP l", "QUEUED"},
		{"SADD s x", "QUEUED"},
		{"ZADD z 1 m", "QUEUED"},
		{"HINCRBYFLOAT h f 1.5", "QUEUED"},
		{"EXEC", "[OK 2 2 nil a 1 1 1.5]"},
		{"TTL k", "100"},
	})
	got := rec.take()
	if len(got) != 7 || !strings.HasPrefix(got[0], "SET k 1 PXAT ") || got[1] != "INCR k" || got[3] != "LPOP l" || got[6] != "HSET h f 1.5" {
		t.Fatalf("EXEC propagated %q", got)
	}

	// WATCH aborts EXEC when a key changes.
	other := e.NewClient("tcp", "other")
	mustStr(t, e, c, "OK", "WATCH", "k")
	mustStr(t, e, other, "3", "INCR", "k")
	mustStr(t, e, c, "OK", "MULTI")
	mustStr(t, e, c, "QUEUED", "INCR", "k")
	if v := do(e, c, "EXEC"); !v.IsNull() {
		t.Fatalf("EXEC after a watched change: %v", render(v))
	}
}
