// Package bloom implements a scalable Bloom filter (Almeida et al., 2007):
// when the newest sub-filter reaches its capacity a larger one is added with
// a tighter error rate, so the compound false-positive rate stays bounded by
// the requested one however many items are inserted.
package bloom

import (
	"encoding/binary"
	"errors"
	"math"
)

const (
	DefaultErrorRate = 0.01
	DefaultCapacity  = 100
	DefaultExpansion = 2

	// tightening is the error-rate ratio between consecutive sub-filters. The
	// first one gets errRate*(1-tightening), so the geometric series of all of
	// them sums to errRate.
	tightening = 0.5

	// maxBits caps one sub-filter (512 MB) so a huge capacity fails cleanly
	// instead of exhausting memory.
	maxBits = 1 << 32
)

var (
	ErrFull     = errors.New("ERR non scaling filter is full")
	ErrTooLarge = errors.New("ERR filter would be too large")
	ErrCorrupt  = errors.New("bloom: corrupt encoding")
)

// layer is one classic Bloom filter.
type layer struct {
	words    []uint64
	m        uint64 // bits
	k        uint32 // hash functions
	capacity uint64
	count    uint64
}

func newLayer(errRate float64, capacity uint64) (*layer, error) {
	ln2 := math.Ln2
	m := math.Ceil(float64(capacity) * -math.Log(errRate) / (ln2 * ln2))
	if m > maxBits {
		return nil, ErrTooLarge
	}
	mb := max(uint64(m), 64)
	mb = (mb + 63) &^ 63
	k := uint32(max(math.Ceil(-math.Log2(errRate)), 1))
	return &layer{words: make([]uint64, mb/64), m: mb, k: k, capacity: capacity}, nil
}

func (l *layer) has(h1, h2 uint64) bool {
	for i := range uint64(l.k) {
		b := (h1 + i*h2) % l.m
		if l.words[b>>6]&(1<<(b&63)) == 0 {
			return false
		}
	}
	return true
}

func (l *layer) set(h1, h2 uint64) {
	for i := range uint64(l.k) {
		b := (h1 + i*h2) % l.m
		l.words[b>>6] |= 1 << (b & 63)
	}
}

// Filter is a scalable Bloom filter.
type Filter struct {
	layers     []*layer
	errRate    float64
	expansion  uint32
	nonScaling bool
	count      uint64
}

// New returns a filter for capacity items at errRate (0 < errRate < 1). With
// nonScaling the single sub-filter uses errRate as is and Add fails once full.
func New(errRate float64, capacity uint64, expansion uint32, nonScaling bool) (*Filter, error) {
	if !(errRate > 0 && errRate < 1) || capacity == 0 {
		return nil, errors.New("bloom: invalid error rate or capacity")
	}
	f := &Filter{errRate: errRate, expansion: max(expansion, 1), nonScaling: nonScaling}
	first := errRate
	if !nonScaling {
		first *= 1 - tightening
	}
	l, err := newLayer(first, capacity)
	if err != nil {
		return nil, err
	}
	f.layers = []*layer{l}
	return f, nil
}

// hash returns the two hashes for double hashing. FNV-1a followed by two
// different murmur3 finalisers: deterministic across processes and restarts,
// so replicas and AOF replays set exactly the same bits.
func hash(item string) (uint64, uint64) {
	h := uint64(14695981039346656037)
	for i := 0; i < len(item); i++ {
		h ^= uint64(item[i])
		h *= 1099511628211
	}
	return fmix(h), fmix(h^0x9e3779b97f4a7c15) | 1
}

func fmix(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// Exists reports whether item may have been added (false means definitely not).
func (f *Filter) Exists(item string) bool {
	h1, h2 := hash(item)
	return f.has(h1, h2)
}

func (f *Filter) has(h1, h2 uint64) bool {
	for i := len(f.layers) - 1; i >= 0; i-- { // newest first: recent items are likelier
		if f.layers[i].has(h1, h2) {
			return true
		}
	}
	return false
}

// Add inserts item and reports whether it was new (false if it may already
// have been present, as BF.ADD reports it).
func (f *Filter) Add(item string) (bool, error) {
	h1, h2 := hash(item)
	if f.has(h1, h2) {
		return false, nil
	}
	cur := f.layers[len(f.layers)-1]
	if cur.count >= cur.capacity {
		if f.nonScaling {
			return false, ErrFull
		}
		if cur.capacity > math.MaxUint64/uint64(f.expansion) {
			return false, ErrTooLarge
		}
		rate := f.errRate * (1 - tightening) * math.Pow(tightening, float64(len(f.layers)))
		next, err := newLayer(rate, cur.capacity*uint64(f.expansion))
		if err != nil {
			return false, err
		}
		f.layers = append(f.layers, next)
		cur = next
	}
	cur.set(h1, h2)
	cur.count++
	f.count++
	return true, nil
}

// Count is the number of items inserted (BF.CARD).
func (f *Filter) Count() uint64 { return f.count }

// Capacity is the total capacity of all sub-filters.
func (f *Filter) Capacity() uint64 {
	var c uint64
	for _, l := range f.layers {
		c += l.capacity
	}
	return c
}

func (f *Filter) NumFilters() int    { return len(f.layers) }
func (f *Filter) Expansion() uint32  { return f.expansion }
func (f *Filter) NonScaling() bool   { return f.nonScaling }
func (f *Filter) ErrorRate() float64 { return f.errRate }

// Clone returns an independent copy (for COPY).
func (f *Filter) Clone() *Filter {
	g, _ := Unmarshal(f.Marshal())
	return g
}

func (f *Filter) MemSize() int64 { return f.bitBytes() + int64(len(f.layers))*64 + 64 }

func (f *Filter) bitBytes() int64 {
	var n int64
	for _, l := range f.layers {
		n += int64(len(l.words)) * 8
	}
	return n
}

// Marshal encodes the filter: errRate float64 | expansion | flags | nlayers |
// per layer (k, capacity, count, nwords, words little-endian). Integers are
// uvarints.
func (f *Filter) Marshal() []byte {
	b := make([]byte, 0, 32+f.bitBytes()+int64(len(f.layers))*24)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(f.errRate))
	b = binary.AppendUvarint(b, uint64(f.expansion))
	var flags uint64
	if f.nonScaling {
		flags = 1
	}
	b = binary.AppendUvarint(b, flags)
	b = binary.AppendUvarint(b, uint64(len(f.layers)))
	for _, l := range f.layers {
		b = binary.AppendUvarint(b, uint64(l.k))
		b = binary.AppendUvarint(b, l.capacity)
		b = binary.AppendUvarint(b, l.count)
		b = binary.AppendUvarint(b, uint64(len(l.words)))
		for _, w := range l.words {
			b = binary.LittleEndian.AppendUint64(b, w)
		}
	}
	return b
}

// Unmarshal decodes Marshal's output, validating every field.
func Unmarshal(b []byte) (*Filter, error) {
	r := reader{b: b}
	f := &Filter{errRate: math.Float64frombits(r.u64())}
	f.expansion = uint32(r.uvarint())
	f.nonScaling = r.uvarint()&1 != 0
	n := r.uvarint()
	if r.bad || !(f.errRate > 0 && f.errRate < 1) || f.expansion == 0 || n == 0 || n > 64 {
		return nil, ErrCorrupt
	}
	for range n {
		l := &layer{k: uint32(r.uvarint()), capacity: r.uvarint(), count: r.uvarint()}
		nw := r.uvarint()
		if r.bad || nw == 0 || nw > maxBits/64 || nw*8 > uint64(len(r.b)) || l.k == 0 || l.k > 64 {
			return nil, ErrCorrupt
		}
		l.words = make([]uint64, nw)
		for i := range l.words {
			l.words[i] = r.u64()
		}
		l.m = nw * 64
		f.layers = append(f.layers, l)
		f.count += l.count
	}
	if r.bad || len(r.b) != 0 {
		return nil, ErrCorrupt
	}
	return f, nil
}

type reader struct {
	b   []byte
	bad bool
}

func (r *reader) uvarint() uint64 {
	v, n := binary.Uvarint(r.b)
	if n <= 0 {
		r.bad = true
		return 0
	}
	r.b = r.b[n:]
	return v
}

func (r *reader) u64() uint64 {
	if len(r.b) < 8 {
		r.bad = true
		return 0
	}
	v := binary.LittleEndian.Uint64(r.b)
	r.b = r.b[8:]
	return v
}
