package command

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"cache-boy/internal/protocol/resp"
)

func ftQlen(t *testing.T, e *Engine, c *Client, q string) string {
	t.Helper()
	m := ftMap(t, do(e, c, "QLEN", q))
	return fmt.Sprintf("%s/%s/%s/%s", m["ready"], m["delayed"], m["inflight"], m["dead"])
}

func TestFeatureQueueBasics(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")

	mustStr(t, e, c, "j1", "QPUSH", "jobs", "payload-1", "ID", "j1")
	mustStr(t, e, c, "j1", "QPUSH", "jobs", "other", "ID", "j1") // duplicate ignored
	id2 := do(e, c, "QPUSH", "jobs", "payload-2").Str
	if id2 == "" || id2 == "j1" {
		t.Fatalf("generated id %q", id2)
	}
	if got := ftQlen(t, e, c, "jobs"); got != "2/0/0/0" {
		t.Fatalf("QLEN %s", got)
	}
	ftArr(t, e, c, []string{"j1", "payload-1", "1"}, "QPOP", "jobs")
	ftArr(t, e, c, []string{id2, "payload-2", "1"}, "QPOP", "jobs")
	if v := do(e, c, "QPOP", "jobs"); !v.IsNull() {
		t.Fatalf("empty QPOP: %v", v)
	}
	if got := ftQlen(t, e, c, "jobs"); got != "0/0/2/0" {
		t.Fatalf("QLEN %s", got)
	}
	mustStr(t, e, c, "1", "QACK", "jobs", "j1")
	mustStr(t, e, c, "0", "QACK", "jobs", "j1")
	mustStr(t, e, c, "1", "QNACK", "jobs", id2)
	ftArr(t, e, c, []string{id2, "payload-2", "2"}, "QPOP", "jobs")
	mustStr(t, e, c, "1", "QACK", "jobs", id2)
	if got := ftQlen(t, e, c, "jobs"); got != "0/0/0/0" {
		t.Fatalf("QLEN %s", got)
	}
	mustStr(t, e, c, "j1", "QPUSH", "jobs", "again", "ID", "j1") // acked ids can be reused

	// Delayed jobs.
	do(e, c, "QPUSH", "later", "x", "DELAY", "60", "ID", "d")
	if got := ftQlen(t, e, c, "later"); got != "0/1/0/0" {
		t.Fatalf("QLEN %s", got)
	}
	if v := do(e, c, "QPOP", "later"); !v.IsNull() {
		t.Fatalf("delayed job popped early: %v", v)
	}
	time.Sleep(80 * time.Millisecond)
	ftArr(t, e, c, []string{"d", "x", "1"}, "QPOP", "later")

	// Retries and the dead-letter list.
	do(e, c, "QPUSH", "flaky", "p", "RETRIES", "1", "ID", "f")
	ftArr(t, e, c, []string{"f", "p", "1"}, "QPOP", "flaky")
	mustStr(t, e, c, "1", "QNACK", "flaky", "f", "DELAY", "30")
	if got := ftQlen(t, e, c, "flaky"); got != "0/1/0/0" {
		t.Fatalf("QLEN %s", got)
	}
	time.Sleep(50 * time.Millisecond)
	ftArr(t, e, c, []string{"f", "p", "2"}, "QPOP", "flaky")
	mustStr(t, e, c, "1", "QNACK", "flaky", "f")
	if got := ftQlen(t, e, c, "flaky"); got != "0/0/0/1" {
		t.Fatalf("QLEN %s", got)
	}
	v := do(e, c, "QDEAD", "flaky")
	if len(v.Elems) != 1 || !ftEq(ftStrs(v.Elems[0]), []string{"f", "p", "2"}) {
		t.Fatalf("QDEAD %v", v)
	}
	if v := do(e, c, "QDEAD", "flaky", "0"); len(v.Elems) != 0 {
		t.Fatalf("QDEAD 0: %v", v)
	}
	mustStr(t, e, c, "1", "QRETRYDEAD", "flaky")
	ftArr(t, e, c, []string{"f", "p", "1"}, "QPOP", "flaky")

	mustStr(t, e, c, "1", "QPURGE", "flaky")
	mustStr(t, e, c, "0", "QPURGE", "flaky")
	if got := ftQlen(t, e, c, "flaky"); got != "0/0/0/0" {
		t.Fatalf("QLEN %s", got)
	}

	mustErr(t, e, c, "QPUSH", "q", "p", "DELAY")
	mustErr(t, e, c, "QPUSH", "q", "p", "RETRIES", "-1")
	mustErr(t, e, c, "QPOP", "q", "VISIBILITY", "0")
	mustErr(t, e, c, "QNACK", "q", "id", "DELAY")
}

func TestFeatureQueueVisibilityTimeout(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	do(e, c, "QPUSH", "vt", "work", "ID", "a", "RETRIES", "1")
	ftArr(t, e, c, []string{"a", "work", "1"}, "QPOP", "vt", "VISIBILITY", "40")
	if v := do(e, c, "QPOP", "vt"); !v.IsNull() {
		t.Fatalf("in-flight job redelivered early: %v", v)
	}
	time.Sleep(60 * time.Millisecond)
	ftArr(t, e, c, []string{"a", "work", "2"}, "QPOP", "vt", "VISIBILITY", "40")
	mustStr(t, e, c, "0", "QACK", "vt", "nope")
	// The second timeout exceeds RETRIES: dead.
	time.Sleep(60 * time.Millisecond)
	if got := ftQlen(t, e, c, "vt"); got != "0/0/0/1" {
		t.Fatalf("QLEN %s", got)
	}
	mustStr(t, e, c, "0", "QACK", "vt", "a")
}

func TestFeatureQueueBlocking(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "consumer")
	p := e.NewClient("tcp", "producer")

	go func() {
		time.Sleep(50 * time.Millisecond)
		do(e, p, "QPUSH", "b", "hello", "ID", "x")
	}()
	start := time.Now()
	ftArr(t, e, c, []string{"x", "hello", "1"}, "QPOP", "b", "BLOCK", "2000")
	if d := time.Since(start); d < 40*time.Millisecond || d > time.Second {
		t.Fatalf("BLOCK woke after %v", d)
	}
	// A delayed job wakes a blocked consumer when it becomes due.
	do(e, p, "QPUSH", "b", "soon", "ID", "y", "DELAY", "60")
	ftArr(t, e, c, []string{"y", "soon", "1"}, "QPOP", "b", "BLOCK", "2000")
	// Timeout.
	start = time.Now()
	if v := do(e, c, "QPOP", "b", "BLOCK", "50"); !v.IsNull() {
		t.Fatalf("QPOP BLOCK timeout: %v", v)
	}
	if time.Since(start) < 45*time.Millisecond {
		t.Fatal("BLOCK returned too early")
	}
	// BLOCK 0 waits forever, until the client disconnects.
	done := make(chan resp.Value)
	go func() { done <- do(e, c, "QPOP", "never", "BLOCK", "0") }()
	select {
	case v := <-done:
		t.Fatalf("BLOCK 0 returned %v", v)
	case <-time.After(50 * time.Millisecond):
	}
	c.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("BLOCK 0 not released on disconnect")
	}
	// Inside MULTI, BLOCK doesn't wait.
	m := e.NewClient("tcp", "multi")
	mustStr(t, e, m, "OK", "MULTI")
	mustStr(t, e, m, "QUEUED", "QPOP", "never", "BLOCK", "0")
	if v := do(e, m, "EXEC"); len(v.Elems) != 1 || !v.Elems[0].IsNull() {
		t.Fatalf("EXEC: %v", v)
	}
}

func TestFeatureQueueConcurrentConsumers(t *testing.T) {
	e := newTestEngine(t)
	const jobs, consumers = 300, 20
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for g := 0; g < consumers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := e.NewClient("tcp", "consumer")
			for {
				v := do(e, c, "QPOP", "work", "BLOCK", "300")
				if v.IsNull() {
					return
				}
				mu.Lock()
				seen[v.Elems[1].Str]++
				mu.Unlock()
				if r := do(e, c, "QACK", "work", v.Elems[0].Str); r.Int != 1 {
					t.Errorf("QACK %v", r)
				}
			}
		}()
	}
	p := e.NewClient("tcp", "producer")
	for i := 0; i < jobs; i++ {
		do(e, p, "QPUSH", "work", fmt.Sprint(i))
	}
	wg.Wait()
	if len(seen) != jobs {
		t.Fatalf("received %d distinct jobs, want %d", len(seen), jobs)
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("job %s delivered %d times", k, n)
		}
	}
}

func TestFeatureQueueNamespaces(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "default")
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	do(e, c, "QPUSH", "q", "default-job")
	do(e, app, "QPUSH", "q", "app-job")
	if v := do(e, app, "QPOP", "q"); v.Elems[1].Str != "app-job" {
		t.Fatalf("namespaced QPOP: %v", v)
	}
	if v := do(e, app, "QPOP", "q"); !v.IsNull() {
		t.Fatalf("namespaced QPOP leaked: %v", v)
	}
	if v := do(e, c, "QPOP", "q"); v.Elems[1].Str != "default-job" {
		t.Fatalf("default QPOP: %v", v)
	}
}

func ftDumpQueues(e *Engine) [][]string {
	var out [][]string
	e.Queues.Dump(func(args []string) { out = append(out, args) })
	return out
}

func ftSameDump(t *testing.T, what string, a, b [][]string) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("%s: %d vs %d commands\n%q\n%q", what, len(a), len(b), a, b)
	}
	for i := range a {
		if !ftEq(a[i], b[i]) {
			t.Fatalf("%s: command %d differs:\n%q\n%q", what, i, a[i], b[i])
		}
	}
}

func TestFeatureQueuePropagationReplay(t *testing.T) {
	e := newTestEngine(t)
	rec := &ftRecorder{}
	e.AddPropagator(rec)
	c := e.NewClient("tcp", "test")
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")

	for i := 0; i < 5; i++ {
		do(e, c, "QPUSH", "main", fmt.Sprint("job-", i), "RETRIES", "1")
	}
	do(e, c, "QPUSH", "main", "delayed", "DELAY", "1h", "ID", "dl")
	do(e, app, "QPUSH", "main", "ns-job", "ID", "n1")
	a := do(e, c, "QPOP", "main", "VISIBILITY", "1h")
	b := do(e, c, "QPOP", "main", "VISIBILITY", "1h")
	d := do(e, c, "QPOP", "main", "VISIBILITY", "1h")
	mustStr(t, e, c, "1", "QACK", "main", a.Elems[0].Str)
	mustStr(t, e, c, "1", "QNACK", "main", b.Elems[0].Str, "DELAY", "1h")
	mustStr(t, e, c, "1", "QNACK", "main", d.Elems[0].Str)
	d2 := do(e, c, "QPOP", "main", "VISIBILITY", "1h") // first ready job again
	_ = d2
	// Drive one job to the dead list, retry it, kill another.
	do(e, c, "QPUSH", "dlq", "x", "RETRIES", "0", "ID", "x")
	do(e, c, "QPUSH", "dlq", "y", "RETRIES", "0", "ID", "y")
	do(e, c, "QPOP", "dlq")
	do(e, c, "QNACK", "dlq", "x")
	mustStr(t, e, c, "1", "QRETRYDEAD", "dlq")
	do(e, c, "QPOP", "dlq")
	do(e, c, "QNACK", "dlq", "y")
	do(e, c, "QPUSH", "gone", "z")
	mustStr(t, e, c, "1", "QPURGE", "gone")

	want := ftDumpQueues(e)
	if len(want) == 0 {
		t.Fatal("empty dump")
	}

	// Replaying the propagated stream reproduces the queues.
	e2 := newTestEngine(t)
	ftReplay(t, e2, rec.all())
	ftSameDump(t, "replayed stream", want, ftDumpQueues(e2))
	if got := ftQlen(t, e2, e2.NewClient("tcp", "x"), "main"); got != fmt.Sprint(ftQlen(t, e, c, "main")) {
		t.Fatalf("QLEN after replay %s", got)
	}

	// Replaying the dump (snapshot path) reproduces them too.
	e3 := newTestEngine(t)
	ftReplay(t, e3, want)
	ftSameDump(t, "replayed dump", want, ftDumpQueues(e3))

	// The replica can keep going: the in-flight job can be acked there.
	m := e3.InternalClient("master", true)
	mustStr(t, e3, m, "1", "QACK", "main", d2.Elems[0].Str)
}
