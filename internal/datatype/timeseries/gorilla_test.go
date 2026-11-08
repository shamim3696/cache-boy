package timeseries

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"
)

// model is the previous uncompressed implementation, kept as the reference.
type model struct {
	samples   []Sample
	retention int64
}

func (m *model) add(ts int64, v float64) {
	insert(&m.samples, ts, v)
	if m.retention <= 0 {
		return
	}
	cutoff := m.samples[len(m.samples)-1].TS - m.retention
	i := sort.Search(len(m.samples), func(i int) bool { return m.samples[i].TS >= cutoff })
	m.samples = m.samples[i:]
}

func (m *model) between(from, to int64) []Sample {
	out := []Sample{}
	for _, p := range m.samples {
		if p.TS >= from && p.TS <= to {
			out = append(out, p)
		}
	}
	return out
}

// sameSamples compares value bits: values must round-trip exactly, -0 and
// NaN payloads included.
func sameSamples(a, b []Sample) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].TS != b[i].TS || math.Float64bits(a[i].Value) != math.Float64bits(b[i].Value) {
			return false
		}
	}
	return true
}

func randomValue(r *rand.Rand, prev float64) float64 {
	switch r.IntN(8) {
	case 0:
		return prev
	case 1:
		return math.Float64frombits(r.Uint64())
	case 2:
		return float64(r.IntN(100))
	case 3:
		return math.Copysign(0, -1)
	}
	return prev + r.NormFloat64()
}

func TestModel(t *testing.T) {
	for seed := range uint64(40) {
		r := rand.New(rand.NewPCG(seed, 7))
		var retention int64
		if seed%3 == 0 {
			retention = int64(r.IntN(500_000) + 1)
		}
		s, m := New(retention), &model{retention: retention}
		add := func(ts int64, v float64) {
			s.Add(ts, v)
			m.add(ts, v)
		}
		ts, v := int64(r.IntN(1000))-500, 0.0
		ops := 2000 + r.IntN(3000)
		for op := range ops {
			switch k := r.IntN(100); {
			case k < 75: // append with a jittered interval, sometimes a big gap
				ts += int64(999 + r.IntN(3))
				if r.IntN(50) == 0 {
					ts += int64(r.Uint32())
				}
				v = randomValue(r, v)
				add(ts, v)
			case k < 90: // out of order
				add(ts-int64(r.IntN(400_000)), randomValue(r, v))
			default: // overwrite an existing timestamp
				if len(m.samples) > 0 {
					add(m.samples[r.IntN(len(m.samples))].TS, randomValue(r, v))
				}
			}
			if op%97 != 0 && op != ops-1 {
				continue
			}
			if s.Len() != len(m.samples) {
				t.Fatalf("seed %d op %d: len %d, model %d", seed, op, s.Len(), len(m.samples))
			}
			if !sameSamples(s.Samples(), m.samples) {
				t.Fatalf("seed %d op %d: samples differ", seed, op)
			}
			if len(m.samples) == 0 {
				continue
			}
			if got, ok := s.Latest(); !ok || !sameSamples([]Sample{got}, m.samples[len(m.samples)-1:]) {
				t.Fatalf("seed %d op %d: latest %v", seed, op, got)
			}
			lo := m.samples[r.IntN(len(m.samples))].TS + int64(r.IntN(3)-1)
			hi := lo + int64(r.IntN(600_000))
			if got, _ := s.Range(lo, hi, "", 0); !sameSamples(got, m.between(lo, hi)) {
				t.Fatalf("seed %d op %d: range [%d,%d] differs", seed, op, lo, hi)
			}
			agg, _ := s.Range(math.MinInt64, math.MaxInt64, "count", 60_000)
			total := 0.0
			for _, a := range agg {
				total += a.Value
			}
			if int(total) != len(m.samples) {
				t.Fatalf("seed %d op %d: count aggregation %v, want %d", seed, op, total, len(m.samples))
			}
		}
	}
}

func TestEmptyAndExtremes(t *testing.T) {
	s := New(0)
	if _, ok := s.Latest(); ok || s.Len() != 0 {
		t.Fatal("empty series")
	}
	if got, _ := s.Range(math.MinInt64, math.MaxInt64, "", 0); got == nil || len(got) != 0 {
		t.Fatal("empty range should be an empty, non-nil slice")
	}
	extremes := []Sample{{math.MinInt64, 1}, {-1, math.Inf(-1)}, {0, math.SmallestNonzeroFloat64},
		{math.MaxInt64 - 1, math.MaxFloat64}, {math.MaxInt64, math.Copysign(0, -1)}}
	var want []Sample
	for i := range 3 * chunkSize {
		p := Sample{int64(i)*7919 + 1, float64(i)}
		s.Add(p.TS, p.Value)
		want = append(want, p)
	}
	for _, p := range extremes {
		s.Add(p.TS, p.Value)
		want = append(want, p)
	}
	sort.Slice(want, func(i, j int) bool { return want[i].TS < want[j].TS })
	if !sameSamples(s.Samples(), want) {
		t.Fatal("extreme timestamps and values do not round-trip")
	}
}

// TestBytesPerSample measures a realistic series: one sample a second, the
// value drifting slowly with two decimals, like a temperature sensor.
func TestBytesPerSample(t *testing.T) {
	const n = 100_000
	r := rand.New(rand.NewPCG(1, 2))
	s := New(0)
	var plain []Sample // the previous representation
	ts, v := int64(1_700_000_000_000), 21.5
	for range n {
		ts += 1000
		if r.IntN(10) == 0 {
			v = math.Round((v+r.NormFloat64()*0.05)*100) / 100
		}
		s.Add(ts, v)
		plain = append(plain, Sample{ts, v})
	}
	before := float64(cap(plain)*16+64) / n
	after := float64(s.MemSize()) / n
	t.Logf("bytes/sample, slowly changing sensor: before %.2f, after %.2f (%.1fx smaller)", before, after, before/after)
	if after > 3 {
		t.Errorf("%.2f bytes/sample, want <= 3", after)
	}

	s = New(0)
	for range n {
		ts += 1000
		s.Add(ts, 20+r.Float64())
	}
	t.Logf("bytes/sample, random full-precision values: %.2f", float64(s.MemSize())/n)
}

func BenchmarkAppend(b *testing.B) {
	s := New(0)
	for i := range b.N {
		s.Add(int64(i)*1000, float64(i%100))
	}
}

func BenchmarkAppendWithRetention(b *testing.B) {
	s := New(3_600_000)
	for i := range b.N {
		s.Add(int64(i)*1000, float64(i%100))
	}
}

func BenchmarkOutOfOrder(b *testing.B) {
	s := New(0)
	for i := range 100_000 {
		s.Add(int64(i)*1000, float64(i%100))
	}
	b.ResetTimer()
	for i := range b.N {
		s.Add(int64(i%100_000)*1000+500, 1)
	}
}
