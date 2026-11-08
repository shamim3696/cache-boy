package expiry

import (
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/store"
)

// 100k keys with a 1 s TTL that are never read again must all be freed by
// active expiry alone, about half a second after they expire.
func TestMassExpiry(t *testing.T) {
	const keys = 100_000
	db := store.New(16)
	var events atomic.Int64
	db.Subscribe(func(ev store.Event) {
		if ev.Op == "expired" {
			events.Add(1)
		}
	})
	start := time.Now()
	at := start.Add(time.Second).UnixMilli()
	unlock := db.LockAll()
	for i := range keys {
		e := db.NewStringValue([]byte("value"))
		e.ExpireAt = at
		db.Put("key:"+strconv.Itoa(i), e)
	}
	unlock()

	stop := make(chan struct{})
	defer close(stop)
	go Run(db, config.NewStore(config.Default()), stop)

	deadline := start.Add(1500 * time.Millisecond)
	for db.Len() > 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	t.Logf("freed %d keys in %v after insertion started (TTL 1s)", keys-db.Len(), time.Since(start).Round(time.Millisecond))
	if db.Len() != 0 || db.UsedMemory() != 0 {
		t.Fatalf("after %v: len=%d used=%d", time.Since(start), db.Len(), db.UsedMemory())
	}
	if db.Expired.Load() != keys || events.Load() != keys {
		t.Fatalf("expired stat %d, events %d", db.Expired.Load(), events.Load())
	}
}
