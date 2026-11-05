package tags

import (
	"strings"
	"testing"

	"cache-boy/internal/store"
)

func TestIndex(t *testing.T) {
	db := store.New(4)
	x := New(db)
	unlock := db.LockAll()
	defer unlock()
	db.Put("a", db.NewStringValue([]byte("1")))
	db.Put("b", db.NewStringValue([]byte("2")))
	if n := x.Tag("a", "t1", "t2", "t1"); n != 2 {
		t.Fatalf("Tag returned %d", n)
	}
	x.Tag("b", "t1")
	if got := strings.Join(x.KeysOf("t1", "t2"), ","); got != "a,b" {
		t.Fatalf("KeysOf %s", got)
	}
	// A write that leaves the key in place keeps its tags.
	db.Emit(store.Event{Key: "a", Op: "set"})
	if !x.Has("a") {
		t.Fatal("tags lost on overwrite")
	}
	// A write after which the key is gone drops it.
	db.Delete("a")
	db.Emit(store.Event{Key: "a", Op: "rename"})
	if x.Has("a") || strings.Join(x.List(), ",") != "t1" {
		t.Fatalf("after delete: %v", x.List())
	}
	db.Emit(store.Event{Key: "b", Op: "evicted"})
	if len(x.List()) != 0 {
		t.Fatalf("after evict: %v", x.List())
	}
	x.Tag("b", "t3")
	db.Emit(store.Event{Op: "flushall"})
	if x.Has("b") || len(x.List()) != 0 {
		t.Fatal("flushall did not clear")
	}
	x.Tag("b", "z", "y")
	if n := x.Untag("b", "y", "nope"); n != 1 || strings.Join(x.Of("b"), ",") != "z" {
		t.Fatalf("Untag %d %v", n, x.Of("b"))
	}
}
