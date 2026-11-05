package skiplist

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
)

// TestAgainstModel compares the skip list with a sorted slice after random operations.
func TestAgainstModel(t *testing.T) {
	z := New()
	model := map[string]float64{}
	for i := 0; i < 5000; i++ {
		m := fmt.Sprintf("m%d", rand.IntN(300))
		if rand.IntN(4) == 0 {
			_, existed := model[m]
			if z.Remove(m) != existed {
				t.Fatal("remove result mismatch")
			}
			delete(model, m)
			continue
		}
		s := float64(rand.IntN(50))
		z.Add(m, s)
		model[m] = s
	}
	sorted := make([]Pair, 0, len(model))
	for m, s := range model {
		sorted = append(sorted, Pair{m, s})
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Score != sorted[j].Score {
			return sorted[i].Score < sorted[j].Score
		}
		return sorted[i].Member < sorted[j].Member
	})
	if z.Len() != len(sorted) {
		t.Fatalf("len %d vs %d", z.Len(), len(sorted))
	}
	all := z.RangeByRank(0, -1, false)
	for i, p := range sorted {
		if all[i] != p {
			t.Fatalf("rank %d: %v vs %v", i, all[i], p)
		}
		if r, _ := z.Rank(p.Member, false); r != i {
			t.Fatalf("Rank(%s) = %d want %d", p.Member, r, i)
		}
		if r, _ := z.Rank(p.Member, true); r != len(sorted)-1-i {
			t.Fatal("reverse rank")
		}
	}
	rev := z.RangeByRank(0, 2, true)
	for i := range rev {
		if rev[i] != sorted[len(sorted)-1-i] {
			t.Fatal("reverse range")
		}
	}
	min, max := Bound{Value: 10}, Bound{Value: 20, Exclusive: true}
	want := 0
	for _, p := range sorted {
		if p.Score >= 10 && p.Score < 20 {
			want++
		}
	}
	if got := len(z.RangeByScore(min, max, false, 0, -1)); got != want {
		t.Fatalf("RangeByScore %d want %d", got, want)
	}
	if got := z.CountByScore(min, max); got != want {
		t.Fatalf("CountByScore %d want %d", got, want)
	}
	if got := len(z.RangeByScore(min, max, true, 0, -1)); got != want {
		t.Fatalf("reverse RangeByScore %d want %d", got, want)
	}
}

func TestPopAndLex(t *testing.T) {
	z := New()
	for _, m := range []string{"d", "a", "c", "b"} {
		z.Add(m, 0)
	}
	got := z.RangeByLex(LexBound{Value: "b"}, LexBound{Value: "d", Exclusive: true}, false, 0, -1)
	if len(got) != 2 || got[0].Member != "b" || got[1].Member != "c" {
		t.Fatal(got)
	}
	if p := z.Pop(1, true); p[0].Member != "d" || z.Len() != 3 {
		t.Fatal(p)
	}
}
