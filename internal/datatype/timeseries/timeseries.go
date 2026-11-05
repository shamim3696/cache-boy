// Package timeseries stores (timestamp, value) samples sorted by time, with
// optional retention and range queries with downsampling.
package timeseries

import (
	"errors"
	"math"
	"sort"
)

type Sample struct {
	TS    int64 // unix milliseconds
	Value float64
}

type Series struct {
	samples   []Sample
	Retention int64 // ms; 0 = keep forever
	Labels    map[string]string
}

var ErrUnknownAgg = errors.New("ERR unknown aggregation; use avg, sum, min, max, count, first, last, range")

func New(retentionMs int64) *Series {
	return &Series{Retention: retentionMs, Labels: map[string]string{}}
}

func (s *Series) Len() int { return len(s.samples) }

func (s *Series) MemSize() int64 { return int64(cap(s.samples))*16 + 64 }

// Add inserts a sample. A sample with an existing timestamp replaces it.
func (s *Series) Add(ts int64, v float64) {
	n := len(s.samples)
	switch {
	case n == 0 || ts > s.samples[n-1].TS:
		s.samples = append(s.samples, Sample{ts, v})
	default:
		i := sort.Search(n, func(i int) bool { return s.samples[i].TS >= ts })
		if i < n && s.samples[i].TS == ts {
			s.samples[i].Value = v
		} else {
			s.samples = append(s.samples, Sample{})
			copy(s.samples[i+1:], s.samples[i:])
			s.samples[i] = Sample{ts, v}
		}
	}
	s.trim()
}

// Latest returns the newest sample.
func (s *Series) Latest() (Sample, bool) {
	if len(s.samples) == 0 {
		return Sample{}, false
	}
	return s.samples[len(s.samples)-1], true
}

// Range returns samples with from <= ts <= to. With agg != "" samples are
// grouped into buckets of bucketMs and aggregated.
func (s *Series) Range(from, to int64, agg string, bucketMs int64) ([]Sample, error) {
	if agg != "" {
		if _, err := aggregate(agg, []Sample{{}}); err != nil {
			return nil, err
		}
	}
	lo := sort.Search(len(s.samples), func(i int) bool { return s.samples[i].TS >= from })
	hi := sort.Search(len(s.samples), func(i int) bool { return s.samples[i].TS > to })
	if lo >= hi {
		return []Sample{}, nil
	}
	raw := s.samples[lo:hi]
	if agg == "" || bucketMs <= 0 {
		return append([]Sample(nil), raw...), nil
	}
	var out []Sample
	for i := 0; i < len(raw); {
		bucket := raw[i].TS - mod(raw[i].TS, bucketMs)
		j := i
		for j < len(raw) && raw[j].TS < bucket+bucketMs {
			j++
		}
		v, err := aggregate(agg, raw[i:j])
		if err != nil {
			return nil, err
		}
		out = append(out, Sample{bucket, v})
		i = j
	}
	return out, nil
}

// Samples returns all samples (used for persistence).
func (s *Series) Samples() []Sample { return s.samples }

func (s *Series) trim() {
	if s.Retention <= 0 || len(s.samples) == 0 {
		return
	}
	cutoff := s.samples[len(s.samples)-1].TS - s.Retention
	i := sort.Search(len(s.samples), func(i int) bool { return s.samples[i].TS >= cutoff })
	if i > 0 {
		s.samples = append(s.samples[:0], s.samples[i:]...)
	}
}

func aggregate(agg string, xs []Sample) (float64, error) {
	switch agg {
	case "avg", "sum":
		sum := 0.0
		for _, x := range xs {
			sum += x.Value
		}
		if agg == "avg" {
			return sum / float64(len(xs)), nil
		}
		return sum, nil
	case "min", "max", "range":
		lo, hi := math.Inf(1), math.Inf(-1)
		for _, x := range xs {
			lo, hi = math.Min(lo, x.Value), math.Max(hi, x.Value)
		}
		switch agg {
		case "min":
			return lo, nil
		case "max":
			return hi, nil
		}
		return hi - lo, nil
	case "count":
		return float64(len(xs)), nil
	case "first":
		return xs[0].Value, nil
	case "last":
		return xs[len(xs)-1].Value, nil
	}
	return 0, ErrUnknownAgg
}

func mod(a, b int64) int64 {
	m := a % b
	if m < 0 {
		m += b
	}
	return m
}
