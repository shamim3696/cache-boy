package eviction

import (
	"fmt"
	"sync"
	"testing"

	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/store"
)

func fill(db *store.DB, n int) {
	unlock := db.LockAll()
	defer unlock()
	for i := 0; i < n; i++ {
		db.Put(fmt.Sprintf("k%d", i), db.NewStringValue(make([]byte, 100)))
	}
}

func TestEvictsUnderLimit(t *testing.T) {
	for _, policy := range []string{"allkeys-lru", "allkeys-lfu", "allkeys-random", "allkeys-tinylfu"} {
		cfg := config.Default()
		cfg.MaxMemoryPolicy = policy
		cs := config.NewStore(cfg)
		db := store.New(8)
		fill(db, 1000)
		limit := db.UsedMemory() / 2
		cs.Set("maxmemory", fmt.Sprint(limit))
		if err := New(db, cs).Free(); err != nil {
			t.Fatalf("%s: %v", policy, err)
		}
		if db.UsedMemory() > limit || db.Evicted.Load() == 0 {
			t.Fatalf("%s: used=%d limit=%d", policy, db.UsedMemory(), limit)
		}
	}
}

func TestNoEviction(t *testing.T) {
	cfg := config.Default()
	cfg.MaxMemory = 1
	db := store.New(4)
	fill(db, 10)
	if err := New(db, config.NewStore(cfg)).Free(); err != ErrOOM {
		t.Fatalf("want OOM, got %v", err)
	}
}

func TestVolatileOnlyEvictsKeysWithTTL(t *testing.T) {
	cfg := config.Default()
	cfg.MaxMemoryPolicy = "volatile-lru"
	cfg.MaxMemory = 1
	db := store.New(4)
	fill(db, 10)
	if err := New(db, config.NewStore(cfg)).Free(); err != ErrOOM {
		t.Fatalf("no volatile keys: want OOM, got %v", err)
	}
	if db.Len() != 10 {
		t.Fatal("persistent keys were evicted")
	}
}

// volatile-tinylfu evicts only keys with a TTL, preferring the rarely used.
func TestVolatileTinyLFU(t *testing.T) {
	cfg := config.Default()
	cfg.MaxMemoryPolicy = "volatile-tinylfu"
	cs := config.NewStore(cfg)
	db := store.New(4)
	fill(db, 100) // persistent
	unlock := db.LockAll()
	for i := range 200 {
		e := db.NewStringValue(make([]byte, 100))
		e.ExpireAt = 1 << 62
		db.Put(fmt.Sprintf("t%d", i), e)
	}
	for range 5 {
		for i := range 20 { // t0..t19 are hot
			db.Get(fmt.Sprintf("t%d", i))
		}
	}
	unlock()
	cs.Set("maxmemory", fmt.Sprint(db.UsedMemory()*2/3))
	if err := New(db, cs).Free(); err != nil {
		t.Fatal(err)
	}
	unlock = db.LockAll()
	defer unlock()
	for i := range 100 {
		if db.Peek(fmt.Sprintf("k%d", i)) == nil {
			t.Fatalf("persistent key k%d evicted", i)
		}
	}
	for i := range 20 {
		if db.Peek(fmt.Sprintf("t%d", i)) == nil {
			t.Fatalf("hot key t%d evicted", i)
		}
	}
}

func TestPoolKeepsBestDistinct(t *testing.T) {
	var p pool
	for i := range 40 {
		p.offer(store.Candidate{Key: fmt.Sprint(i % 25), Access: int64(100 - i)}, lruBetter, 0)
	}
	if len(p.c) != poolSize {
		t.Fatalf("pool size %d", len(p.c))
	}
	seen := map[string]bool{}
	for i, c := range p.c {
		if seen[c.Key] {
			t.Fatalf("duplicate %s", c.Key)
		}
		seen[c.Key] = true
		if i > 0 && lruBetter(c, p.c[i-1], 0) {
			t.Fatalf("pool out of order at %d", i)
		}
	}
	if p.c[0].Access != 61 { // key "14", offered last with the oldest access
		t.Fatalf("best %+v", p.c[0])
	}
}

// Pooled candidates may be gone, or used again, by the time they're chosen.
func TestPoolRechecks(t *testing.T) {
	db := store.New(1)
	fill(db, 3)
	cands := db.Sample(0, 3, false)
	var p pool
	for _, c := range cands {
		c.Access = 0 // looks idle forever
		p.offer(c, lruBetter, 0)
	}
	unlock := db.LockAll()
	db.Delete(cands[0].Key)
	unlock()
	got := p.take(db, lruBetter, 0, false)
	if got == nil || got.Key == cands[0].Key || got.Access == 0 {
		t.Fatalf("took %+v", got)
	}
}

func TestConcurrentFree(t *testing.T) {
	cfg := config.Default()
	cfg.MaxMemoryPolicy = "allkeys-tinylfu"
	cfg.MaxMemory = 200_000
	db := store.New(8)
	ev := New(db, config.NewStore(cfg))
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 2000 {
				if err := ev.Free(); err != nil {
					t.Error(err)
					return
				}
				k := fmt.Sprintf("g%d:%d", g, i)
				unlock := db.Lock(k)
				db.Put(k, db.NewStringValue(make([]byte, 100)))
				db.Get(k)
				unlock()
			}
		})
	}
	wg.Wait()
	if err := ev.Free(); err != nil || db.UsedMemory() > cfg.MaxMemory {
		t.Fatalf("err=%v used=%d", err, db.UsedMemory())
	}
}
