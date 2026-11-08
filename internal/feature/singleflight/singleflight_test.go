package singleflight

import (
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/store"
)

func TestComputeLock(t *testing.T) {
	db := store.New(4)
	g := New(db)
	if !g.Acquire("k", 1, time.Minute) || g.Acquire("k", 2, time.Minute) || !g.Locked("k") {
		t.Fatal("only the first caller gets the lock")
	}
	db.Emit(store.Event{Key: "other", Op: "set"})
	if !g.Locked("k") {
		t.Fatal("unrelated write cleared the lock")
	}
	db.Emit(store.Event{Key: "k", Op: "set"}) // any write on the key clears it
	if g.Locked("k") || g.Len() != 0 {
		t.Fatal("write did not clear the lock")
	}
	g.Acquire("k", 1, time.Minute)
	if !g.Release("k") || g.Release("k") {
		t.Fatal("release")
	}
	g.Acquire("a", 1, time.Minute)
	g.Acquire("b", 1, time.Minute)
	db.Emit(store.Event{Op: "flushall"})
	if g.Len() != 0 {
		t.Fatal("flushall did not clear locks")
	}
	g.Acquire("e", 1, 10*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	if g.Locked("e") || !g.Acquire("e", 2, time.Minute) {
		t.Fatal("expired lock not taken over")
	}
}
