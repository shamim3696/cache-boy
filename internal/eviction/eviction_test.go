package eviction

import (
	"fmt"
	"testing"

	"cache-boy/internal/config"
	"cache-boy/internal/store"
)

func fill(db *store.DB, n int) {
	unlock := db.LockAll()
	defer unlock()
	for i := 0; i < n; i++ {
		db.Put(fmt.Sprintf("k%d", i), db.NewStringValue(make([]byte, 100)))
	}
}

func TestEvictsUnderLimit(t *testing.T) {
	for _, policy := range []string{"allkeys-lru", "allkeys-lfu", "allkeys-random"} {
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
