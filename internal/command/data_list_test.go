package command

import (
	"testing"
	"time"

	"cache-boy/internal/protocol/resp"
)

func TestListCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"LPUSHX l a", "0"},
		{"EXISTS l", "0"},
		{"RPUSH l a b c", "3"},
		{"LPUSH l z y", "5"},
		{"LRANGE l 0 -1", "[y z a b c]"},
		{"RPUSHX l d", "6"},
		{"LPUSHX l x", "7"},
		{"LLEN l", "7"},
		{"LLEN nope", "0"},
		{"LRANGE l -2 -1", "[c d]"},
		{"LRANGE l 5 100", "[c d]"},
		{"LRANGE l 3 1", "[]"},
		{"LRANGE l -100 0", "[x]"},
		{"LRANGE nope 0 -1", "[]"},
		{"LRANGE l a 1", "!ERR value is not an integer"},
		{"LINDEX l 0", "x"},
		{"LINDEX l -1", "d"},
		{"LINDEX l 100", "nil"},
		{"LINDEX nope 0", "nil"},
		{"LSET l 0 X", "OK"},
		{"LSET l -1 D", "OK"},
		{"LSET l 100 v", "!ERR index out of range"},
		{"LSET nope 0 v", "!ERR no such key"},
		{"LRANGE l 0 -1", "[X y z a b c D]"},
		{"LPOP l", "X"},
		{"RPOP l", "D"},
		{"LPOP l 2", "[y z]"},
		{"RPOP l 0", "[]"},
		{"RPOP l 10", "[c b a]"},
		{"EXISTS l", "0"},
		{"LPOP l", "nil"},
		{"LPOP l 2", "nil"},
		{"LPOP l -1", "!ERR value is out of range"},

		{"RPUSH m a b a c a", "5"},
		{"LINSERT m BEFORE c X", "6"},
		{"LINSERT m AFTER c Y", "7"},
		{"LINSERT m AFTER nope Y", "-1"},
		{"LINSERT nope AFTER a b", "0"},
		{"LINSERT m SIDEWAYS a b", "!ERR syntax error"},
		{"LRANGE m 0 -1", "[a b a X c Y a]"},
		{"LPOS m a", "0"},
		{"LPOS m a RANK 2", "2"},
		{"LPOS m a RANK -1", "6"},
		{"LPOS m a RANK -2", "2"},
		{"LPOS m a COUNT 0", "[0 2 6]"},
		{"LPOS m a COUNT 2", "[0 2]"},
		{"LPOS m a RANK -1 COUNT 2", "[6 2]"},
		{"LPOS m a COUNT 0 MAXLEN 3", "[0 2]"},
		{"LPOS m zz", "nil"},
		{"LPOS m zz COUNT 1", "[]"},
		{"LPOS m a RANK 0", "!ERR RANK can't be zero"},
		{"LPOS m a COUNT -1", "!ERR COUNT can't be negative"},
		{"LPOS m a MAXLEN -1", "!ERR MAXLEN can't be negative"},
		{"LPOS m a BOGUS 1", "!ERR syntax error"},
		{"LREM m 1 a", "1"},
		{"LRANGE m 0 -1", "[b a X c Y a]"},
		{"LREM m -1 a", "1"},
		{"LRANGE m 0 -1", "[b a X c Y]"},
		{"LREM m 0 nope", "0"},
		{"LREM m 0 a", "1"},
		{"LTRIM m 1 -2", "OK"},
		{"LRANGE m 0 -1", "[X c]"},
		{"LTRIM m 5 10", "OK"},
		{"EXISTS m", "0"},
		{"LTRIM nope 0 1", "OK"},

		{"SET s v", "OK"},
		{"LPUSH s a", "!WRONGTYPE"},
		{"LPUSHX s a", "!WRONGTYPE"},
		{"LPOP s", "!WRONGTYPE"},
		{"LLEN s", "!WRONGTYPE"},
		{"LRANGE s 0 1", "!WRONGTYPE"},
		{"LPOS s a", "!WRONGTYPE"},
	})
}

func TestListMove(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"RPUSH src a b c", "3"},
		{"LMOVE src dst LEFT RIGHT", "a"},
		{"LMOVE src dst RIGHT LEFT", "c"},
		{"LRANGE dst 0 -1", "[c a]"},
		{"RPOPLPUSH src dst", "b"},
		{"EXISTS src", "0"},
		{"LRANGE dst 0 -1", "[b c a]"},
		{"RPOPLPUSH src dst", "nil"},
		{"LMOVE dst dst LEFT RIGHT", "b"},
		{"LRANGE dst 0 -1", "[c a b]"},
		{"RPUSH one x", "1"},
		{"LMOVE one one LEFT RIGHT", "x"},
		{"LRANGE one 0 -1", "[x]"},
		{"LMOVE dst x UP DOWN", "!ERR syntax error"},
		{"SET str v", "OK"},
		{"LMOVE dst str LEFT LEFT", "!WRONGTYPE"},
		{"LLEN dst", "3"},
		{"LMOVE str dst LEFT LEFT", "!WRONGTYPE"},

		{"RPUSH k1 a b c", "3"},
		{"RPUSH k2 d", "1"},
		{"LMPOP 2 nope k1 LEFT", "[k1 [a]]"},
		{"LMPOP 2 nope k1 RIGHT COUNT 5", "[k1 [c b]]"},
		{"LMPOP 2 k1 k2 LEFT", "[k2 [d]]"},
		{"LMPOP 1 k1 LEFT", "nil"},
		{"LMPOP 0 k1 LEFT", "!ERR numkeys"},
		{"LMPOP 3 k1 LEFT", "!ERR"},
		{"LMPOP 1 k1 MIDDLE", "!ERR syntax error"},
		{"LMPOP 1 k1 LEFT COUNT 0", "!ERR count should be greater than 0"},
		{"LMPOP 1 k1 LEFT COUNT 1 COUNT 2", "!ERR syntax error"},
		{"LMPOP 1 str LEFT", "!WRONGTYPE"},
	})
}

func TestListBlocking(t *testing.T) {
	e, c := newDataEngine(t)
	pusher := e.NewClient("tcp", "pusher")

	runCases(t, e, c, []tc{
		{"RPUSH ready a b", "2"},
		{"BLPOP empty ready 1", "[ready a]"},
		{"BRPOP empty ready 1", "[ready b]"},
		{"BLPOP k -1", "!ERR timeout is negative"},
		{"BLPOP k abc", "!ERR timeout is not a float"},
		{"SET str v", "OK"},
		{"BLPOP str 1", "!WRONGTYPE"},
	})

	// Timeout: returns a null array after roughly the timeout.
	start := time.Now()
	if v := do(e, c, "BLPOP", "empty", "0.1"); !v.IsNull() {
		t.Fatalf("BLPOP timeout: %v", render(v))
	}
	if d := time.Since(start); d < 90*time.Millisecond || d > 2*time.Second {
		t.Fatalf("BLPOP timeout took %v", d)
	}
	mustStr(t, e, c, "0", "EXISTS", "empty")

	wake := func(name string, cmd []string, push []string, want string) {
		t.Helper()
		done := make(chan resp.Value, 1)
		go func() { done <- do(e, c, cmd...) }()
		time.Sleep(50 * time.Millisecond)
		select {
		case v := <-done:
			t.Fatalf("%s returned before the push: %v", name, render(v))
		default:
		}
		do(e, pusher, push...)
		select {
		case v := <-done:
			if got := render(v); got != want {
				t.Fatalf("%s: got %q, want %q", name, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s was not woken by the push", name)
		}
	}
	wake("BLPOP", []string{"BLPOP", "q1", "q2", "0"}, []string{"RPUSH", "q2", "hello"}, "[q2 hello]")
	wake("BRPOP", []string{"BRPOP", "q1", "5"}, []string{"RPUSH", "q1", "a", "b"}, "[q1 b]")
	mustStr(t, e, c, "a", "LPOP", "q1")
	wake("BLMOVE", []string{"BLMOVE", "q3", "dst", "LEFT", "RIGHT", "0"}, []string{"LPUSH", "q3", "m"}, "m")
	wake("BRPOPLPUSH", []string{"BRPOPLPUSH", "q4", "dst", "0"}, []string{"LPUSH", "q4", "n"}, "n")
	mustStr(t, e, c, "2", "LLEN", "dst")
	wake("BLMPOP", []string{"BLMPOP", "0", "2", "q5", "q6", "LEFT", "COUNT", "5"}, []string{"RPUSH", "q6", "a", "b"}, "[q6 [a b]]")

	// A write of the wrong type doesn't satisfy the waiter; it keeps waiting.
	done := make(chan resp.Value, 1)
	go func() { done <- do(e, c, "BLPOP", "q7", "0.3") }()
	time.Sleep(50 * time.Millisecond)
	do(e, pusher, "SET", "q8", "x")
	if v := <-done; !v.IsNull() {
		t.Fatalf("BLPOP woken by an unrelated key: %v", render(v))
	}

	if v := do(e, c, "BLMOVE", "none", "dst", "LEFT", "LEFT", "0.05"); !v.IsNull() {
		t.Fatalf("BLMOVE timeout: %v", render(v))
	}
	if v := do(e, c, "BLMPOP", "0.05", "1", "none", "LEFT"); !v.IsNull() {
		t.Fatalf("BLMPOP timeout: %v", render(v))
	}
}

func TestListBlockingNamespace(t *testing.T) {
	e := newTestEngine(t)
	c, pusher := e.NewClient("tcp", "c"), e.NewClient("tcp", "p")
	mustStr(t, e, c, "OK", "NAMESPACE", "USE", "ns")
	mustStr(t, e, pusher, "OK", "NAMESPACE", "USE", "ns")
	done := make(chan resp.Value, 1)
	go func() { done <- do(e, c, "BLPOP", "jobs", "2") }()
	time.Sleep(50 * time.Millisecond)
	do(e, pusher, "RPUSH", "jobs", "j1")
	if got := render(<-done); got != "[jobs j1]" {
		t.Fatalf("namespaced BLPOP: %q", got)
	}
}

func TestListPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	pusher := e.NewClient("tcp", "pusher")
	rec := &recorder{}
	e.AddPropagator(rec)

	done := make(chan resp.Value, 1)
	go func() { done <- do(e, c, "BLPOP", "q", "0") }()
	time.Sleep(50 * time.Millisecond)
	do(e, pusher, "RPUSH", "q", "a")
	<-done
	got := rec.take()
	if len(got) != 2 || got[0] != "RPUSH q a" || got[1] != "LPOP q" {
		t.Fatalf("BLPOP propagated %q", got)
	}

	do(e, c, "BLPOP", "q", "0.01")
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("timed-out BLPOP propagated %q", got)
	}

	do(e, c, "RPUSH", "q", "a", "b", "c")
	do(e, c, "BRPOPLPUSH", "q", "d", "0")
	do(e, c, "LMPOP", "1", "q", "LEFT", "COUNT", "5")
	do(e, c, "LPOP", "nope")
	got = rec.take()
	want := []string{"RPUSH q a b c", "LMOVE q d RIGHT LEFT", "LPOP q 2"}
	if len(got) != len(want) {
		t.Fatalf("propagated %q, want %q", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("propagated %q, want %q", got, want)
		}
	}
}
