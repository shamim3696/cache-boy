package store

import (
	"strconv"
	"testing"
)

func TestSketchEstimates(t *testing.T) {
	s := newSketch(1000)
	x := func(i int) uint64 { return sketchHash(hashKey("k" + strconv.Itoa(i))) }
	for range 10 {
		for i := range 100 { // keys 0..99 are hot
			s.increment(x(i))
		}
	}
	for i := 1000; i < 2000; i++ { // one-hit wonders
		s.increment(x(i))
	}
	for i := range 100 {
		if e := s.estimate(x(i)); e < 10 {
			t.Fatalf("hot key %d estimated %d", i, e)
		}
	}
	over := 0
	for i := 1000; i < 2000; i++ {
		if s.estimate(x(i)) > 1 {
			over++
		}
	}
	if over > 20 {
		t.Fatalf("%d of 1000 one-hit keys estimated above 1", over)
	}
	if e := s.estimate(x(5000)); e > 1 {
		t.Fatalf("unseen key estimated %d", e)
	}
}

func TestSketchSaturatesAndAges(t *testing.T) {
	s := newSketch(0)
	x := sketchHash(hashKey("hot"))
	for range 100 {
		s.increment(x)
	}
	if e := s.estimate(x); e != 16 {
		t.Fatalf("saturated estimate %d, want 16", e)
	}
	s.age()
	if e := s.estimate(x); e != 7 {
		t.Fatalf("after aging %d, want 7 (15/2, doorkeeper cleared)", e)
	}
	// Aging also happens on its own after period increments.
	for i := 0; ; i++ {
		before := s.additions
		s.increment(sketchHash(uint32(i)))
		if s.additions < before {
			break
		}
		if i > s.period {
			t.Fatal("never aged")
		}
	}
	if e := s.estimate(x); e > 4 { // 3, plus collisions in a deliberately overloaded sketch
		t.Fatalf("after second aging %d", e)
	}
}

func TestSketchGrowsWithShard(t *testing.T) {
	db := New(1)
	unlock := db.LockAll()
	defer unlock()
	for i := range 10_000 {
		db.Put("k"+strconv.Itoa(i), db.NewStringValue([]byte("v")))
	}
	if c := db.shards[0].freq.capacity; c < 10_000 {
		t.Fatalf("sketch capacity %d for 10000 keys", c)
	}
}
