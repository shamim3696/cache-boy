package timeseries

import "testing"

func TestAddRangeAggregate(t *testing.T) {
	s := New(0)
	for _, ts := range []int64{30, 10, 20, 40, 20} { // out of order + duplicate
		s.Add(ts, float64(ts))
	}
	if s.Len() != 4 {
		t.Fatalf("len %d", s.Len())
	}
	got, _ := s.Range(15, 35, "", 0)
	if len(got) != 2 || got[0].TS != 20 || got[1].TS != 30 {
		t.Fatal(got)
	}
	agg, _ := s.Range(0, 100, "sum", 20) // buckets [0,20) [20,40) [40,60)
	if len(agg) != 3 || agg[0].Value != 10 || agg[1].Value != 50 || agg[2].Value != 40 {
		t.Fatal(agg)
	}
	if _, err := s.Range(0, 100, "median", 10); err == nil {
		t.Fatal("unknown aggregation accepted")
	}
}

func TestRetention(t *testing.T) {
	s := New(100)
	for ts := int64(0); ts <= 1000; ts += 50 {
		s.Add(ts, 1)
	}
	if first, _ := s.Range(0, 2000, "", 0); first[0].TS != 900 {
		t.Fatalf("oldest kept sample %d", first[0].TS)
	}
}
