package store

import (
	"strconv"
	"testing"
)

// sweep runs one active-expiry pass at a chosen time.
func sweep(db *DB, budget int, now int64) (expired int, more bool) {
	for _, s := range db.shards {
		s.mu.Lock()
		n, m := db.expireDue(s, budget, now)
		s.mu.Unlock()
		expired += n
		more = more || m
	}
	return expired, more
}

func setExpire(db *DB, key string, at int64) {
	unlock := db.Lock(key)
	defer unlock()
	db.SetExpire(key, at)
}

func TestWheelSkipsStaleEntries(t *testing.T) {
	db := New(4)
	t0 := nowMs()
	put(db, "extended", "v", t0+1000)
	setExpire(db, "extended", t0+5000)
	put(db, "persisted", "v", t0+1000)
	setExpire(db, "persisted", 0)
	put(db, "replaced", "v", t0+1000)
	put(db, "replaced", "v", 0)
	put(db, "deleted", "v", t0+1000)
	unlock := db.Lock("deleted")
	db.Delete("deleted")
	unlock()
	put(db, "due", "v", t0+1000)

	if n, _ := sweep(db, 100, t0+2000); n != 1 || db.Peek("due") != nil {
		t.Fatalf("first sweep expired %d", n)
	}
	if db.Len() != 3 {
		t.Fatalf("len=%d after first sweep", db.Len())
	}
	if n, _ := sweep(db, 100, t0+4000); n != 0 {
		t.Fatalf("extended key expired early (%d)", n)
	}
	if n, _ := sweep(db, 100, t0+5200); n != 1 || db.Len() != 2 {
		t.Fatalf("extended key: expired %d, len %d", n, db.Len())
	}
	if n, _ := sweep(db, 100, t0+100_000); n != 0 || db.Len() != 2 {
		t.Fatalf("late sweep expired %d, len %d", n, db.Len())
	}
	for _, s := range db.shards {
		if s.wheel.n != 0 || len(s.wheel.buckets) != 0 {
			t.Fatalf("wheel not drained: n=%d buckets=%d", s.wheel.n, len(s.wheel.buckets))
		}
	}
}

func TestWheelBudget(t *testing.T) {
	db := New(1)
	t0 := nowMs()
	for i := range 5000 {
		put(db, "k"+strconv.Itoa(i), "v", t0+1000+int64(i%300))
	}
	for i, want := range []int{2000, 2000, 1000} {
		n, more := sweep(db, 2000, t0+2000)
		if n != want || more != (i < 2) {
			t.Fatalf("sweep %d: expired %d more=%v", i, n, more)
		}
	}
	if db.Len() != 0 || db.UsedMemory() != 0 {
		t.Fatalf("len=%d used=%d", db.Len(), db.UsedMemory())
	}
}

func TestWheelSameBucketNotDuplicated(t *testing.T) {
	db := New(1)
	t0 := nowMs()
	at := (t0/wheelTickMs + 10) * wheelTickMs
	put(db, "k", "v", at)
	put(db, "k", "v", at+50) // replaced, same bucket
	setExpire(db, "k", at+10)
	if n := db.shards[0].wheel.n; n != 1 {
		t.Fatalf("wheel entries %d, want 1", n)
	}
	if n, _ := sweep(db, 100, at+wheelTickMs); n != 1 {
		t.Fatalf("expired %d", n)
	}
}

// A sliding TTL moves the key to a new bucket on every request; the stale
// entries it leaves behind must stay bounded.
func TestWheelStaleEntriesBounded(t *testing.T) {
	db := New(1)
	t0 := nowMs()
	put(db, "session", "v", t0+60_000)
	put(db, "other", "v", t0+60_000)
	for i := range 100_000 {
		setExpire(db, "session", t0+60_000+int64(i)*wheelTickMs)
	}
	w := &db.shards[0].wheel
	if w.n > 2*2+wheelSlack+1 {
		t.Fatalf("wheel holds %d entries for 2 keys", w.n)
	}
	if n, _ := sweep(db, 1<<30, t0+60_000+100_000*wheelTickMs); n != 2 {
		t.Fatalf("expired %d", n)
	}
}

// The SetExpire that triggers a rebuild must register the new expiry.
func TestWheelRebuildKeepsNewExpiry(t *testing.T) {
	db := New(1)
	t0 := nowMs()
	put(db, "k", "v", t0+60_000)
	w := &db.shards[0].wheel
	for i := 1; w.n < 2+wheelSlack; i++ {
		setExpire(db, "k", t0+60_000+int64(i)*wheelTickMs)
	}
	last := t0 + 1_000_000
	setExpire(db, "k", last) // rebuilds
	if w.n != 1 {
		t.Fatalf("no rebuild: n=%d", w.n)
	}
	if n, _ := sweep(db, 1<<30, last-1000); n != 0 {
		t.Fatal("expired early")
	}
	if n, _ := sweep(db, 1<<30, last+wheelTickMs); n != 1 {
		t.Fatal("never expired")
	}
}

func TestWheelAfterLongPause(t *testing.T) {
	db := New(1)
	t0 := nowMs()
	put(db, "k", "v", t0+1000)
	// A year later: the cursor must jump instead of walking every tick.
	if n, _ := sweep(db, 100, t0+365*24*3600*1000); n != 1 {
		t.Fatalf("expired %d", n)
	}
}

func TestFlushResetsWheel(t *testing.T) {
	db := New(2)
	t0 := nowMs()
	put(db, "a", "v", t0+1000)
	unlock := db.LockAll()
	db.Flush()
	unlock()
	put(db, "a", "v", 0)
	if n, _ := sweep(db, 100, t0+5000); n != 0 || db.Len() != 1 {
		t.Fatalf("expired %d len %d", n, db.Len())
	}
}
