package bloom

import (
	"bytes"
	"strconv"
	"testing"
)

// fpRate measures false positives over n items that were never added.
func fpRate(f *Filter, n int) float64 {
	fp := 0
	for i := range n {
		if f.Exists("absent:" + strconv.Itoa(i)) {
			fp++
		}
	}
	return float64(fp) / float64(n)
}

func checkNoFalseNegatives(t *testing.T, f *Filter, n int) {
	t.Helper()
	for i := range n {
		if !f.Exists("item:" + strconv.Itoa(i)) {
			t.Fatalf("false negative for item:%d", i)
		}
	}
}

func TestFalsePositiveRate(t *testing.T) {
	const n = 100_000
	for _, target := range []float64{0.01, 0.001} {
		for _, tc := range []struct {
			name       string
			capacity   uint64
			nonScaling bool
			wantLayers int
		}{
			{"single filter", n, true, 1},
			{"single scaling filter", n, false, 1},
			{"scaled", 1000, false, 7}, // 1000+2000+...+64000 >= 100k
		} {
			f, err := New(target, tc.capacity, 2, tc.nonScaling)
			if err != nil {
				t.Fatal(err)
			}
			for i := range n {
				if _, err := f.Add("item:" + strconv.Itoa(i)); err != nil {
					t.Fatal(err)
				}
			}
			checkNoFalseNegatives(t, f, n)
			got := fpRate(f, 200_000)
			t.Logf("target %.3f %-22s layers=%d fp=%.5f (%.2fx) bytes=%d", target, tc.name, f.NumFilters(), got, got/target, f.MemSize())
			if f.NumFilters() != tc.wantLayers {
				t.Errorf("%s: %d filters, want %d", tc.name, f.NumFilters(), tc.wantLayers)
			}
			if got > target*1.5 {
				t.Errorf("target %v %s: fp rate %v exceeds 1.5x", target, tc.name, got)
			}
		}
	}
}

func TestAddReportsNewAndCount(t *testing.T) {
	f, _ := New(0.01, 100, 2, false)
	if added, _ := f.Add("a"); !added {
		t.Fatal("first add not new")
	}
	if added, _ := f.Add("a"); added {
		t.Fatal("second add reported new")
	}
	if f.Count() != 1 || f.Capacity() != 100 {
		t.Fatalf("count %d capacity %d", f.Count(), f.Capacity())
	}
}

func TestNonScalingFull(t *testing.T) {
	f, _ := New(0.01, 10, 2, true)
	full := false
	for i := range 100 {
		if _, err := f.Add(strconv.Itoa(i)); err == ErrFull {
			full = true
			break
		}
	}
	if !full || f.Count() != 10 {
		t.Fatalf("full=%v count=%d", full, f.Count())
	}
}

func TestMarshalRoundTrip(t *testing.T) {
	f, _ := New(0.01, 50, 3, false)
	for i := range 1000 {
		f.Add("item:" + strconv.Itoa(i))
	}
	b := f.Marshal()
	g, err := Unmarshal(b)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(g.Marshal(), b) || g.Count() != f.Count() || g.NumFilters() != f.NumFilters() || g.Expansion() != 3 {
		t.Fatal("round trip changed the filter")
	}
	checkNoFalseNegatives(t, g, 1000)
	for i := range len(b) {
		if _, err := Unmarshal(b[:i]); err == nil {
			t.Fatalf("truncated encoding (%d bytes) accepted", i)
		}
	}
}

func TestHashIsDeterministic(t *testing.T) {
	// Pinned values: replicas and AOF replays depend on the hash never changing.
	h1, h2 := hash("hello")
	if h1 != 0xe9c562c0fdb23244 || h2 != 0xc6297d203982f46b {
		t.Fatalf("hash(hello) = %#x %#x", h1, h2)
	}
}
