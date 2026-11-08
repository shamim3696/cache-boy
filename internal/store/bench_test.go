package store

import (
	"strconv"
	"testing"
)

const benchKeys = 1 << 16

func benchDB(b *testing.B) (*DB, []string) {
	db := New(16)
	keys := make([]string, benchKeys)
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		put(db, keys[i], "value", 0)
	}
	b.ResetTimer()
	return db, keys
}

// BenchmarkGet is a hit, the way a command runs it: lock the shard, Get, unlock.
func BenchmarkGet(b *testing.B) {
	db, keys := benchDB(b)
	for i := 0; b.Loop(); i++ {
		k := keys[i&(benchKeys-1)]
		unlock := db.Lock(k)
		db.Get(k)
		unlock()
	}
}

// BenchmarkPut replaces existing keys.
func BenchmarkPut(b *testing.B) {
	db, keys := benchDB(b)
	e := db.NewStringValue([]byte("value"))
	for i := 0; b.Loop(); i++ {
		k := keys[i&(benchKeys-1)]
		unlock := db.Lock(k)
		db.Put(k, e)
		unlock()
	}
}

// BenchmarkPutTTL inserts fresh keys with a TTL, exercising the timing wheel.
func BenchmarkPutTTL(b *testing.B) {
	db, keys := benchDB(b)
	at := nowMs() + 60_000
	for i := 0; b.Loop(); i++ {
		k := keys[i&(benchKeys-1)]
		unlock := db.Lock(k)
		e := db.NewStringValue([]byte("value"))
		e.ExpireAt = at + int64(i&1023)
		db.Put(k, e)
		unlock()
	}
}

func BenchmarkSketchIncrement(b *testing.B) {
	s := newSketch(4096)
	for i := uint32(0); b.Loop(); i++ {
		s.increment(sketchHash(i & 8191))
	}
}

func BenchmarkHashKey(b *testing.B) {
	for b.Loop() {
		hashKey("key:12345")
	}
}
