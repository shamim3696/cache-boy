package store

import (
	"sync"
	"testing"
	"time"
)

func put(db *DB, key, val string, expireAt int64) {
	unlock := db.Lock(key)
	defer unlock()
	e := db.NewStringValue([]byte(val))
	e.ExpireAt = expireAt
	db.Put(key, e)
}

func TestLazyAndActiveExpiry(t *testing.T) {
	db := New(4)
	var events []Event
	var mu sync.Mutex
	db.Subscribe(func(ev Event) { mu.Lock(); events = append(events, ev); mu.Unlock() })

	past := time.Now().Add(-time.Second).UnixMilli()
	put(db, "lazy", "v", past)
	put(db, "active", "v", past)
	put(db, "keep", "v", 0)

	unlock := db.Lock("lazy")
	if db.Get("lazy") != nil {
		t.Fatal("expired key returned")
	}
	unlock()
	if n := db.ActiveExpireCycle(20); n != 1 {
		t.Fatalf("active expiry removed %d keys", n)
	}
	if db.Len() != 1 || db.Expired.Load() != 2 {
		t.Fatalf("len=%d expired=%d", db.Len(), db.Expired.Load())
	}
	if len(events) != 2 || events[0].Op != "expired" {
		t.Fatalf("events: %+v", events)
	}
}

func TestMemoryAccounting(t *testing.T) {
	db := New(4)
	put(db, "a", "abcde", 0)
	used := db.UsedMemory()
	if used <= 5 {
		t.Fatalf("used=%d", used)
	}
	put(db, "a", "abcdefghijklmnopqrstuvwxyz", 0)
	if db.UsedMemory() < used+21 {
		t.Fatalf("replace: %d -> %d", used, db.UsedMemory())
	}
	unlock := db.Lock("a")
	db.Delete("a")
	unlock()
	if db.UsedMemory() != 0 || db.Len() != 0 {
		t.Fatalf("after delete used=%d len=%d", db.UsedMemory(), db.Len())
	}
}

func TestEmptyContainerDeleted(t *testing.T) {
	db := New(1)
	unlock := db.LockAll()
	defer unlock()
	h := NewHash()
	h.Set("f", "v")
	db.Put("h", NewEntry(KindHash, h))
	h.Del("f")
	db.Updated("h")
	if db.Peek("h") != nil {
		t.Fatal("empty hash not deleted")
	}
}

func TestCompression(t *testing.T) {
	db := New(1)
	db.CompressThreshold.Store(100)
	big := make([]byte, 10_000)
	for i := range big {
		big[i] = 'a' + byte(i%3)
	}
	e := db.NewStringValue(big)
	s := e.Str()
	if !s.Compressed() || s.StoredSize() >= len(big) || string(s.Bytes()) != string(big) {
		t.Fatalf("compressed=%v stored=%d", s.Compressed(), s.StoredSize())
	}
	if db.NewStringValue([]byte("short")).Str().Compressed() {
		t.Fatal("small value compressed")
	}
}

func TestLockOrderNoDeadlock(t *testing.T) {
	db := New(16)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); u := db.Lock("a", "b", "c"); u() }()
		go func() { defer wg.Done(); u := db.Lock("c", "b", "a"); u() }()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock")
	}
}
