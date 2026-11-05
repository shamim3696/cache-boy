package deque

import (
	"strings"
	"testing"
)

func TestDeque(t *testing.T) {
	d := New()
	for i := 0; i < 100; i++ {
		d.PushBack(string(rune('a' + i%26)))
		d.PushFront("x")
	}
	if d.Len() != 200 {
		t.Fatal(d.Len())
	}
	for i := 0; i < 150; i++ {
		d.PopFront()
	}
	if d.Len() != 50 || d.At(0) != string(rune('a'+50%26)) {
		t.Fatalf("len=%d first=%q", d.Len(), d.At(0))
	}
	if got := strings.Join(d.Range(-3, -1), ""); got != "tuv" {
		t.Fatalf("range: %q", got)
	}
	if len(d.Range(10, 5)) != 0 || len(d.Range(60, 70)) != 0 {
		t.Fatal("empty ranges")
	}
}

func TestRemoveInsertTrim(t *testing.T) {
	d := New()
	for _, s := range []string{"a", "b", "a", "c", "a"} {
		d.PushBack(s)
	}
	if n := d.Remove("a", -2); n != 2 || strings.Join(d.Slice(), "") != "abc" {
		t.Fatalf("remove from tail: %d %v", n, d.Slice())
	}
	d.Insert(1, "z")
	if strings.Join(d.Slice(), "") != "azbc" {
		t.Fatal(d.Slice())
	}
	d.Trim(1, -2)
	if strings.Join(d.Slice(), "") != "zb" {
		t.Fatal(d.Slice())
	}
	if d.MemSize() <= 0 {
		t.Fatal("memsize")
	}
}
