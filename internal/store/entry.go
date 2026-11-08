package store

import (
	"math/rand/v2"
	"time"

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
	Kind Kind
	freq uint8  // logarithmic access counter (LFU)
	hash uint32 // hashKey of the key, set by Put

	Value    sizer
	ExpireAt int64 // unix ms; 0 = no expiry

	access int64 // last access, unix ms (LRU)
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

// touch updates LRU/LFU info on access (Redis' probabilistic log counter) and
// records the access in the shard's frequency sketch. x is the key's
// sketchHash; the caller holds the shard lock.
func (e *Entry) touch(now int64, f *sketch, x uint64) {
	f.increment(x)
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

func nowMs() int64 { return time.Now().UnixMilli() }

// Size returns the bytes accounted for this entry (MEMORY USAGE).
func (e *Entry) Size() int64 { return e.size }
