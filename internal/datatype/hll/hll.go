// Package hll implements HyperLogLog cardinality estimation the way Redis
// does: 16384 6-bit registers indexed by a MurmurHash64A of the element, and
// Ertl's improved estimator ("New cardinality estimation algorithms for
// HyperLogLog sketches", 2017), whose standard error is about 0.81%.
//
// Small sketches use a sparse representation (sorted index/value pairs) and
// switch to the 12 KB dense one once that would be smaller.
package hll

import (
	"encoding/binary"
	"errors"
	"math"
	"math/bits"
	"sort"
)

const (
	p         = 14
	m         = 1 << p
	q         = 64 - p // bits left for the run of zeros
	maxReg    = q + 1
	denseSize = m*6/8 + 1 // one spare byte so 6-bit reads never run past the end

	// sparseMax is the most sparse entries kept before converting to dense:
	// 4 bytes each, like Redis' default hll-sparse-max-bytes of 3000.
	sparseMax = 750

	hashSeed = 0xadc83b19 // Redis' seed, so register indexes match Redis
)

var ErrCorrupt = errors.New("hll: corrupt encoding")

// HLL is a HyperLogLog sketch. Exactly one of sparse and dense is in use.
type HLL struct {
	sparse []uint32 // idx<<8 | value, sorted by idx, values > 0
	dense  []byte   // packed 6-bit registers, little-endian bit order
	card   int64    // cached Count; -1 when stale
}

func New() *HLL { return &HLL{card: 0} }

// IsSparse reports which representation is in use (OBJECT ENCODING).
func (h *HLL) IsSparse() bool { return h.dense == nil }

func (h *HLL) MemSize() int64 {
	if h.dense != nil {
		return denseSize + 48
	}
	return int64(cap(h.sparse))*4 + 48
}

// Add inserts an element and reports whether a register changed (PFADD's 1).
func (h *HLL) Add(element []byte) bool {
	x := murmur64a(element, hashSeed)
	idx := int(x & (m - 1))
	x >>= p
	x |= 1 << q // guarantees termination: at most q zeros
	return h.setMax(idx, uint8(bits.TrailingZeros64(x)+1))
}

// setMax raises register idx to v if v is larger.
func (h *HLL) setMax(idx int, v uint8) bool {
	if h.dense != nil {
		if getDense(h.dense, idx) >= v {
			return false
		}
		setDense(h.dense, idx, v)
		h.card = -1
		return true
	}
	i := sort.Search(len(h.sparse), func(i int) bool { return int(h.sparse[i]>>8) >= idx })
	e := uint32(idx)<<8 | uint32(v)
	if i < len(h.sparse) && int(h.sparse[i]>>8) == idx {
		if uint8(h.sparse[i]) >= v {
			return false
		}
		h.sparse[i] = e
	} else {
		if len(h.sparse) >= sparseMax {
			h.toDense()
			return h.setMax(idx, v)
		}
		h.sparse = append(h.sparse, 0)
		copy(h.sparse[i+1:], h.sparse[i:])
		h.sparse[i] = e
	}
	h.card = -1
	return true
}

func (h *HLL) toDense() {
	h.dense = make([]byte, denseSize)
	for _, e := range h.sparse {
		setDense(h.dense, int(e>>8), uint8(e))
	}
	h.sparse = nil
}

func getDense(r []byte, idx int) uint8 {
	bit := idx * 6
	b, fb := bit>>3, uint(bit&7)
	return uint8((uint16(r[b])|uint16(r[b+1])<<8)>>fb) & 63
}

func setDense(r []byte, idx int, v uint8) {
	bit := idx * 6
	b, fb := bit>>3, uint(bit&7)
	w := uint16(r[b]) | uint16(r[b+1])<<8
	w = w&^(63<<fb) | uint16(v)<<fb
	r[b], r[b+1] = byte(w), byte(w>>8)
}

// forEach calls fn for every non-zero register.
func (h *HLL) forEach(fn func(idx int, v uint8)) {
	if h.dense == nil {
		for _, e := range h.sparse {
			fn(int(e>>8), uint8(e))
		}
		return
	}
	for i := range m {
		if v := getDense(h.dense, i); v != 0 {
			fn(i, v)
		}
	}
}

// Merge folds other into h (register-wise maximum), i.e. h becomes the union.
func (h *HLL) Merge(other *HLL) {
	other.forEach(func(idx int, v uint8) { h.setMax(idx, v) })
}

// Clone returns an independent copy.
func (h *HLL) Clone() *HLL {
	c := &HLL{card: h.card}
	if h.dense != nil {
		c.dense = append([]byte(nil), h.dense...)
	} else {
		c.sparse = append([]uint32(nil), h.sparse...)
	}
	return c
}

// Count estimates the number of distinct elements added.
func (h *HLL) Count() int64 {
	if h.card >= 0 {
		return h.card
	}
	var histo [maxReg + 1]int
	nonZero := 0
	h.forEach(func(_ int, v uint8) { histo[v]++; nonZero++ })
	histo[0] = m - nonZero
	h.card = estimate(&histo)
	return h.card
}

// estimate is Ertl's improved raw estimator, as in Redis' hllCount.
func estimate(histo *[maxReg + 1]int) int64 {
	const mf = float64(m)
	z := mf * tau(float64(m-histo[q+1])/mf)
	for k := q; k >= 1; k-- {
		z += float64(histo[k])
		z *= 0.5
	}
	z += mf * sigma(float64(histo[0])/mf)
	return int64(math.Round(0.5 / math.Ln2 * mf * mf / z))
}

func sigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		zl := z
		z += x * y
		y += y
		if z == zl {
			return z
		}
	}
}

func tau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		zl := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if z == zl {
			return z / 3
		}
	}
}

// murmur64a is Austin Appleby's MurmurHash64A, the hash Redis uses for HLLs.
func murmur64a(key []byte, seed uint64) uint64 {
	const mul = 0xc6a4a7935bd1e995
	const r = 47
	h := seed ^ uint64(len(key))*mul
	for len(key) >= 8 {
		k := binary.LittleEndian.Uint64(key)
		k *= mul
		k ^= k >> r
		k *= mul
		h ^= k
		h *= mul
		key = key[8:]
	}
	if len(key) > 0 {
		var tail uint64
		for i := len(key) - 1; i >= 0; i-- {
			tail = tail<<8 | uint64(key[i])
		}
		h ^= tail
		h *= mul
	}
	h ^= h >> r
	h *= mul
	h ^= h >> r
	return h
}

// Marshal encodes the sketch: 0 | n uvarint | n × (index delta uvarint, value
// byte) when sparse, or 1 | the packed dense registers.
func (h *HLL) Marshal() []byte {
	if h.dense != nil {
		return append([]byte{1}, h.dense[:denseSize-1]...)
	}
	b := binary.AppendUvarint([]byte{0}, uint64(len(h.sparse)))
	prev := 0
	for _, e := range h.sparse {
		idx := int(e >> 8)
		b = binary.AppendUvarint(b, uint64(idx-prev))
		b = append(b, byte(e))
		prev = idx
	}
	return b
}

// Unmarshal decodes Marshal's output, validating indexes and register values.
func Unmarshal(b []byte) (*HLL, error) {
	if len(b) == 0 {
		return nil, ErrCorrupt
	}
	h := &HLL{card: -1}
	switch b[0] {
	case 1:
		if len(b) != denseSize {
			return nil, ErrCorrupt
		}
		h.dense = make([]byte, denseSize)
		copy(h.dense, b[1:])
		for i := range m {
			if getDense(h.dense, i) > maxReg {
				return nil, ErrCorrupt
			}
		}
		return h, nil
	case 0:
		n, k := binary.Uvarint(b[1:])
		if k <= 0 || n > m {
			return nil, ErrCorrupt
		}
		b = b[1+k:]
		h.sparse = make([]uint32, 0, n)
		idx := -1
		for range n {
			d, k := binary.Uvarint(b)
			if k <= 0 || len(b) < k+1 || (idx >= 0 && d == 0) || d >= m {
				return nil, ErrCorrupt
			}
			idx = max(idx, 0) + int(d)
			v := b[k]
			if idx >= m || v == 0 || v > maxReg {
				return nil, ErrCorrupt
			}
			h.sparse = append(h.sparse, uint32(idx)<<8|uint32(v))
			b = b[k+1:]
		}
		if len(b) != 0 {
			return nil, ErrCorrupt
		}
		return h, nil
	}
	return nil, ErrCorrupt
}
