package hll

import (
	"bytes"
	"math"
	"strconv"
	"testing"
)

func addRange(h *HLL, prefix string, from, to int) {
	for i := from; i < to; i++ {
		h.Add([]byte(prefix + strconv.Itoa(i)))
	}
}

func relErr(got int64, want int) float64 {
	return math.Abs(float64(got)-float64(want)) / float64(want)
}

func TestAccuracy(t *testing.T) {
	for _, n := range []int{10, 1000, 100_000, 1_000_000} {
		h := New()
		addRange(h, "el:", 0, n)
		got := h.Count()
		e := relErr(got, n)
		t.Logf("n=%-8d estimate=%-8d error=%.3f%% sparse=%v", n, got, e*100, h.IsSparse())
		if e > 0.02 {
			t.Errorf("n=%d: estimate %d, error %.2f%% > 2%%", n, got, e*100)
		}
	}
}

func TestSparseToDense(t *testing.T) {
	h := New()
	addRange(h, "x", 0, 300)
	if !h.IsSparse() {
		t.Fatal("300 elements should still be sparse")
	}
	sparseCount := h.Count()
	d := h.Clone()
	d.toDense()
	if d.Count() != sparseCount {
		t.Fatalf("dense %d != sparse %d", d.Count(), sparseCount)
	}
	addRange(h, "x", 300, 5000)
	if h.IsSparse() {
		t.Fatal("5000 elements should be dense")
	}
}

func TestAddReportsChange(t *testing.T) {
	h := New()
	if !h.Add([]byte("a")) || h.Add([]byte("a")) {
		t.Fatal("Add should report a change only the first time")
	}
	if h.Count() != 1 {
		t.Fatalf("count %d", h.Count())
	}
}

func TestMerge(t *testing.T) {
	for _, sizes := range [][2]int{{100, 200}, {50_000, 80_000}, {100, 80_000}} {
		a, b, both := New(), New(), New()
		addRange(a, "e", 0, sizes[0])                     // 0..a
		addRange(b, "e", sizes[0]/2, sizes[0]/2+sizes[1]) // overlaps half of a
		addRange(both, "e", 0, sizes[0]/2+sizes[1])
		a.Merge(b)
		// The union sketch is register-for-register the sketch of the union.
		if a.Count() != both.Count() {
			t.Fatalf("%v: merged %d, union %d", sizes, a.Count(), both.Count())
		}
		if e := relErr(a.Count(), sizes[0]/2+sizes[1]); e > 0.02 {
			t.Fatalf("%v: merged error %.2f%%", sizes, e*100)
		}
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 500, 100_000} {
		h := New()
		addRange(h, "r", 0, n)
		b := h.Marshal()
		g, err := Unmarshal(b)
		if err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if g.Count() != h.Count() || g.IsSparse() != h.IsSparse() || !bytes.Equal(g.Marshal(), b) {
			t.Fatalf("n=%d: round trip changed the sketch", n)
		}
		for i := range len(b) {
			if _, err := Unmarshal(b[:i]); err == nil && i != len(b) {
				t.Fatalf("n=%d: truncated encoding (%d bytes) accepted", n, i)
			}
		}
	}
	if _, err := Unmarshal([]byte{0, 1, 5, 60}); err == nil {
		t.Fatal("register value 60 accepted")
	}
}

func TestHashIsPinned(t *testing.T) {
	// Sketches are persisted and replicated as registers, and PFADD is
	// replayed on replicas: the hash must never change.
	if got := murmur64a([]byte("hello"), hashSeed); got != 0xf656f01eecfe400 {
		t.Fatalf("murmur64a(hello) = %#x", got)
	}
}
