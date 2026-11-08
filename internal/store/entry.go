package store

import (
	"math/rand/v2"
	"time"

	"cache-boy/internal/compression"
	"cache-boy/internal/datatype/bloom"
	"cache-boy/internal/datatype/deque"
	"cache-boy/internal/datatype/hll"
	"cache-boy/internal/datatype/jsondoc"
	"cache-boy/internal/datatype/skiplist"
	"cache-boy/internal/datatype/timeseries"
	"cache-boy/internal/datatype/vector"
)

type Kind uint8

const (
	KindString Kind = iota + 1
	KindList
	KindHash
	KindSet
	KindZSet
	KindJSON
	KindTimeSeries
	KindVector
	KindBloom
	KindHLL
)

func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	case KindSet:
		return "set"
	case KindZSet:
		return "zset"
	case KindJSON:
		return "json"
	case KindTimeSeries:
		return "timeseries"
	case KindVector:
		return "vectorset"
	case KindBloom:
		return "MBbloom--" // RedisBloom's type name, which clients switch on
	case KindHLL:
		return "hyperloglog"
	}
	return "none"
}

// Entry is one key's value plus metadata.
//
// Value holds, depending on Kind:
//
//	KindString     *String
//	KindList       *deque.Deque
//	KindHash       *Hash
//	KindSet        *Set
//	KindZSet       *skiplist.ZSet
//	KindJSON       *jsondoc.Doc
//	KindTimeSeries *timeseries.Series
//	KindVector     *vector.Index
//	KindBloom      *bloom.Filter
//	KindHLL        *hll.HLL
type Entry struct {
	Kind     Kind
	Value    sizer
	ExpireAt int64 // unix ms; 0 = no expiry

	access int64 // last access, unix ms (LRU)
	freq   uint8 // logarithmic access counter (LFU)
	size   int64 // memory accounted for this entry
}

type sizer interface{ MemSize() int64 }

func (e *Entry) Str() *String               { return e.Value.(*String) }
func (e *Entry) List() *deque.Deque         { return e.Value.(*deque.Deque) }
func (e *Entry) Hash() *Hash                { return e.Value.(*Hash) }
func (e *Entry) Set() *Set                  { return e.Value.(*Set) }
func (e *Entry) ZSet() *skiplist.ZSet       { return e.Value.(*skiplist.ZSet) }
func (e *Entry) JSON() *jsondoc.Doc         { return e.Value.(*jsondoc.Doc) }
func (e *Entry) Series() *timeseries.Series { return e.Value.(*timeseries.Series) }
func (e *Entry) Vectors() *vector.Index     { return e.Value.(*vector.Index) }
func (e *Entry) Bloom() *bloom.Filter       { return e.Value.(*bloom.Filter) }
func (e *Entry) HLL() *hll.HLL              { return e.Value.(*hll.HLL) }

// LastAccess returns the last access time in unix ms (for OBJECT IDLETIME).
func (e *Entry) LastAccess() int64 { return e.access }

// Freq returns the LFU counter (for OBJECT FREQ).
func (e *Entry) Freq() uint8 { return e.freq }

func NewEntry(kind Kind, v sizer) *Entry {
	return &Entry{Kind: kind, Value: v, freq: lfuInitVal}
}

const (
	lfuInitVal   = 5
	lfuLogFactor = 10
	lfuDecayMs   = 60_000 // counter halves... decays by 1 per idle minute
)

// touch updates LRU/LFU info on access (Redis' probabilistic log counter).
func (e *Entry) touch(now int64) {
	if e.access > 0 {
		if periods := (now - e.access) / lfuDecayMs; periods > 0 {
			if int64(e.freq) > periods {
				e.freq -= uint8(periods)
			} else {
				e.freq = 0
			}
		}
	}
	if e.freq < 255 {
		base := float64(int(e.freq) - lfuInitVal)
		if base < 0 {
			base = 0
		}
		if rand.Float64() < 1/(base*lfuLogFactor+1) {
			e.freq++
		}
	}
	e.access = now
}

// --- value types that live directly in this package ---

// String is a byte string, transparently compressed when large.
type String struct {
	data       []byte
	compressed bool
	n          int // uncompressed length
}

// NewString stores b, compressing it if threshold > 0 and len(b) >= threshold.
func NewString(b []byte, threshold int) *String {
	s := &String{data: b, n: len(b)}
	if threshold > 0 && len(b) >= threshold {
		if c := compression.Compress(b); c != nil {
			s.data, s.compressed = c, true
		}
	}
	return s
}

// Bytes returns the value. Do not modify the result.
func (s *String) Bytes() []byte {
	if !s.compressed {
		return s.data
	}
	b, err := compression.Decompress(s.data, s.n)
	if err != nil {
		return nil
	}
	return b
}

func (s *String) Len() int         { return s.n }
func (s *String) Compressed() bool { return s.compressed }
func (s *String) StoredSize() int  { return len(s.data) }
func (s *String) MemSize() int64   { return int64(len(s.data)) + 24 }

// Hash is a field → value map with byte accounting.
type Hash struct {
	m     map[string]string
	bytes int64
}

func NewHash() *Hash { return &Hash{m: map[string]string{}} }

// Set returns true if the field is new.
func (h *Hash) Set(field, value string) bool {
	old, exists := h.m[field]
	if exists {
		h.bytes += int64(len(value) - len(old))
	} else {
		h.bytes += int64(len(field) + len(value))
	}
	h.m[field] = value
	return !exists
}

func (h *Hash) Get(field string) (string, bool) { v, ok := h.m[field]; return v, ok }

func (h *Hash) Del(field string) bool {
	old, ok := h.m[field]
	if ok {
		h.bytes -= int64(len(field) + len(old))
		delete(h.m, field)
	}
	return ok
}

func (h *Hash) Len() int               { return len(h.m) }
func (h *Hash) Map() map[string]string { return h.m } // read-only use
func (h *Hash) MemSize() int64         { return h.bytes + int64(len(h.m))*48 + 48 }

// Set is a set of strings with byte accounting.
type Set struct {
	m     map[string]struct{}
	bytes int64
}

func NewSet() *Set { return &Set{m: map[string]struct{}{}} }

// Add returns true if the member is new.
func (s *Set) Add(member string) bool {
	if _, ok := s.m[member]; ok {
		return false
	}
	s.m[member] = struct{}{}
	s.bytes += int64(len(member))
	return true
}

func (s *Set) Remove(member string) bool {
	if _, ok := s.m[member]; !ok {
		return false
	}
	delete(s.m, member)
	s.bytes -= int64(len(member))
	return true
}

func (s *Set) Has(member string) bool { _, ok := s.m[member]; return ok }
func (s *Set) Len() int               { return len(s.m) }

func (s *Set) Members() []string {
	out := make([]string, 0, len(s.m))
	for m := range s.m {
		out = append(out, m)
	}
	return out
}

// Random returns any member (map iteration order is randomised in Go).
func (s *Set) Random() (string, bool) {
	for m := range s.m {
		return m, true
	}
	return "", false
}

func (s *Set) MemSize() int64 { return s.bytes + int64(len(s.m))*32 + 48 }

func nowMs() int64 { return time.Now().UnixMilli() }

// Size returns the bytes accounted for this entry (MEMORY USAGE).
func (e *Entry) Size() int64 { return e.size }
