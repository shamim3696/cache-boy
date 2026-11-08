// Package timeseries stores (timestamp, value) samples sorted by time, with
// optional retention and range queries with downsampling. Samples are kept in
// Gorilla-compressed chunks (see gorilla.go), typically 1-2 bytes per sample
// for a regular series instead of 16.
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

// chunkSize is how many samples the open chunk collects before it is
// compressed. Larger chunks compress slightly better but make out-of-order
// writes and partial reads decode more.
const chunkSize = 256

// chunk is a sealed, Gorilla-compressed run of samples.
type chunk struct {
	data  []byte
	n     int
	first int64  // oldest timestamp
	last  Sample // newest sample, so Latest never decompresses
}

const chunkOverhead = 64 // struct + slice header, for MemSize

func newChunk(samples []Sample) chunk {
	return chunk{
		data:  append([]byte(nil), compress(samples)...), // exact size: MemSize is what we hold
		n:     len(samples),
		first: samples[0].TS,
		last:  samples[len(samples)-1],
	}
}

func (c *chunk) decode(dst []Sample) []Sample { return decompress(dst, c.data, c.n) }

// Series holds samples sorted by timestamp: sealed compressed chunks, oldest
// first, followed by the open chunk, a plain slice that takes appends. Every
// open sample is newer than every sealed one.
//
// Retention is applied lazily to the oldest chunk: samples older than floor
// are hidden from reads and dropped when their whole chunk expires (or the
// chunk is rewritten anyway), so appends never recompress for retention.
type Series struct {
	sealed    []chunk
	open      []Sample
	n         int   // stored samples, including hidden ones in sealed[0]
	floor     int64 // samples older than this have expired
	Retention int64 // ms; 0 = keep forever
	Labels    map[string]string
}

var ErrUnknownAgg = errors.New("ERR unknown aggregation; use avg, sum, min, max, count, first, last, range")

func New(retentionMs int64) *Series {
	return &Series{Retention: retentionMs, Labels: map[string]string{}, floor: math.MinInt64}
}

func (s *Series) Len() int { return s.n - s.expiredHead() }

// expiredHead counts the hidden samples in the oldest chunk.
func (s *Series) expiredHead() int {
	if len(s.sealed) == 0 || s.sealed[0].first >= s.floor {
		return 0
	}
	samples := s.sealed[0].decode(make([]Sample, 0, s.sealed[0].n))
	return sort.Search(len(samples), func(i int) bool { return samples[i].TS >= s.floor })
}

func (s *Series) MemSize() int64 {
	size := int64(cap(s.open))*16 + 64
	for i := range s.sealed {
		size += int64(len(s.sealed[i].data)) + chunkOverhead
	}
	return size
}

// Add inserts a sample. A sample with an existing timestamp replaces it.
// Appends are O(1); an older timestamp decompresses and recompresses the one
// sealed chunk it falls in.
func (s *Series) Add(ts int64, v float64) {
	if ts < s.floor {
		return // already expired: retention would drop it right away
	}
	if len(s.sealed) == 0 || ts > s.sealed[len(s.sealed)-1].last.TS {
		if insert(&s.open, ts, v) {
			s.n++
		}
		if len(s.open) >= chunkSize {
			s.sealed = append(s.sealed, newChunk(s.open))
			s.open = s.open[:0]
		}
	} else {
		s.addSealed(ts, v)
	}
	s.trim()
}

// insert puts (ts, v) into the sorted slice and reports whether it is new.
func insert(xs *[]Sample, ts int64, v float64) bool {
	a := *xs
	n := len(a)
	if n == 0 || ts > a[n-1].TS {
		*xs = append(a, Sample{ts, v})
		return true
	}
	i := sort.Search(n, func(i int) bool { return a[i].TS >= ts })
	if a[i].TS == ts {
		a[i].Value = v
		return false
	}
	a = append(a, Sample{})
	copy(a[i+1:], a[i:])
	a[i] = Sample{ts, v}
	*xs = a
	return true
}

// addSealed writes into the first sealed chunk whose newest sample is not
// older than ts; that chunk is split if it doubles in size.
func (s *Series) addSealed(ts int64, v float64) {
	i := sort.Search(len(s.sealed), func(i int) bool { return s.sealed[i].last.TS >= ts })
	samples := s.sealed[i].decode(make([]Sample, 0, s.sealed[i].n+1))
	if expired := sort.Search(len(samples), func(j int) bool { return samples[j].TS >= s.floor }); expired > 0 {
		samples = samples[expired:]
		s.n -= expired
	}
	if insert(&samples, ts, v) {
		s.n++
	}
	if len(samples) < 2*chunkSize {
		s.sealed[i] = newChunk(samples)
		return
	}
	half := len(samples) / 2
	s.sealed = append(s.sealed, chunk{})
	copy(s.sealed[i+2:], s.sealed[i+1:])
	s.sealed[i], s.sealed[i+1] = newChunk(samples[:half]), newChunk(samples[half:])
}

// Latest returns the newest sample.
func (s *Series) Latest() (Sample, bool) {
	if n := len(s.open); n > 0 {
		return s.open[n-1], true
	}
	if n := len(s.sealed); n > 0 {
		return s.sealed[n-1].last, true
	}
	return Sample{}, false
}

// Range returns samples with from <= ts <= to. With agg != "" samples are
// grouped into buckets of bucketMs and aggregated.
func (s *Series) Range(from, to int64, agg string, bucketMs int64) ([]Sample, error) {
	if agg != "" {
		if _, err := aggregate(agg, []Sample{{}}); err != nil {
			return nil, err
		}
	}
	raw := s.between(from, to)
	if agg == "" || bucketMs <= 0 {
		return raw, nil
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

// between decodes only the chunks that overlap [from, to].
func (s *Series) between(from, to int64) []Sample {
	out := []Sample{}
	from = max(from, s.floor)
	if from > to {
		return out
	}
	for i := sort.Search(len(s.sealed), func(i int) bool { return s.sealed[i].last.TS >= from }); i < len(s.sealed) && s.sealed[i].first <= to; i++ {
		start := len(out)
		out = s.sealed[i].decode(out)
		lo := start + sort.Search(len(out)-start, func(j int) bool { return out[start+j].TS >= from })
		hi := start + sort.Search(len(out)-start, func(j int) bool { return out[start+j].TS > to })
		out = append(out[:start], out[lo:hi]...)
	}
	lo := sort.Search(len(s.open), func(i int) bool { return s.open[i].TS >= from })
	hi := sort.Search(len(s.open), func(i int) bool { return s.open[i].TS > to })
	if lo < hi {
		out = append(out, s.open[lo:hi]...)
	}
	return out
}

// Samples returns a copy of all samples, oldest first (used for persistence).
func (s *Series) Samples() []Sample {
	return s.between(math.MinInt64, math.MaxInt64)
}

// trim expires samples older than Retention before the newest one.
func (s *Series) trim() {
	latest, ok := s.Latest()
	if s.Retention <= 0 || !ok {
		return
	}
	s.floor = max(s.floor, latest.TS-s.Retention)
	drop := sort.Search(len(s.sealed), func(i int) bool { return s.sealed[i].last.TS >= s.floor })
	for _, c := range s.sealed[:drop] {
		s.n -= c.n
	}
	s.sealed = append(s.sealed[:0], s.sealed[drop:]...)
	if len(s.sealed) > 0 {
		return
	}
	if i := sort.Search(len(s.open), func(i int) bool { return s.open[i].TS >= s.floor }); i > 0 {
		s.n -= i
		s.open = append(s.open[:0], s.open[i:]...)
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
