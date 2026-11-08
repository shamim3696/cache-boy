package timeseries

import (
	"math"
	"math/bits"
)

// Gorilla compression (Pelkonen et al., "Gorilla: A Fast, Scalable,
// In-Memory Time Series Database", VLDB 2015). Timestamps are stored as
// delta-of-deltas, which is a single 0 bit for a regular interval; values are
// XORed with the previous one, so a repeated value costs one bit and a slowly
// changing one only its few meaningful bits.
//
// Differences from the paper: the first sample is stored raw (no block-aligned
// header), the delta-of-delta buckets are plain two's complement ranges
// ([-64, 63] rather than [-63, 64]), and the widest bucket is 64 bits rather
// than 32 because millisecond timestamps may jump arbitrarily.

type bitWriter struct {
	b    []byte
	free uint8 // unused bits in the last byte
}

func (w *bitWriter) bit(v bool) {
	if w.free == 0 {
		w.b = append(w.b, 0)
		w.free = 8
	}
	w.free--
	if v {
		w.b[len(w.b)-1] |= 1 << w.free
	}
}

// bits writes the n low bits of v, most significant first.
func (w *bitWriter) bits(v uint64, n uint8) {
	for n > 0 {
		if w.free == 0 {
			w.b = append(w.b, 0)
			w.free = 8
		}
		take := min(n, w.free)
		chunk := byte(v>>(n-take)) & byte(1<<take-1)
		w.free -= take
		w.b[len(w.b)-1] |= chunk << w.free
		n -= take
	}
}

type bitReader struct {
	b   []byte
	pos uint // bit offset
}

func (r *bitReader) bit() bool {
	v := r.b[r.pos>>3]&(0x80>>(r.pos&7)) != 0
	r.pos++
	return v
}

func (r *bitReader) bits(n uint8) uint64 {
	var v uint64
	for n > 0 {
		off := uint8(r.pos & 7)
		take := min(n, 8-off)
		chunk := uint64(r.b[r.pos>>3]>>(8-off-take)) & (1<<take - 1)
		v = v<<take | chunk
		r.pos += uint(take)
		n -= take
	}
	return v
}

// compress encodes samples (sorted, unique timestamps, at least one).
func compress(samples []Sample) []byte {
	w := &bitWriter{b: make([]byte, 0, len(samples)*2+16)}
	first := samples[0]
	w.bits(uint64(first.TS), 64)
	w.bits(math.Float64bits(first.Value), 64)
	prevTS, prevDelta := first.TS, int64(0)
	prevV := math.Float64bits(first.Value)
	lead, trail := uint8(0xff), uint8(0) // no window yet
	for _, s := range samples[1:] {
		delta := s.TS - prevTS
		dod := delta - prevDelta
		switch {
		case dod == 0:
			w.bit(false)
		case dod >= -64 && dod <= 63:
			w.bits(0b10, 2)
			w.bits(uint64(dod), 7)
		case dod >= -256 && dod <= 255:
			w.bits(0b110, 3)
			w.bits(uint64(dod), 9)
		case dod >= -2048 && dod <= 2047:
			w.bits(0b1110, 4)
			w.bits(uint64(dod), 12)
		default:
			w.bits(0b1111, 4)
			w.bits(uint64(dod), 64)
		}
		prevTS, prevDelta = s.TS, delta

		v := math.Float64bits(s.Value)
		x := v ^ prevV
		prevV = v
		if x == 0 {
			w.bit(false)
			continue
		}
		w.bit(true)
		l, t := uint8(bits.LeadingZeros64(x)), uint8(bits.TrailingZeros64(x))
		l = min(l, 31) // 5-bit field
		if lead != 0xff && l >= lead && t >= trail {
			w.bit(false)
			w.bits(x>>trail, 64-lead-trail)
			continue
		}
		lead, trail = l, t
		sig := 64 - l - t
		w.bit(true)
		w.bits(uint64(l), 5)
		w.bits(uint64(sig&63), 6) // 64 meaningful bits is written as 0
		w.bits(x>>t, sig)
	}
	return w.b
}

// signExtend interprets the n low bits of v as a two's complement number.
func signExtend(v uint64, n uint8) int64 {
	return int64(v<<(64-n)) >> (64 - n)
}

// decompress decodes n samples written by compress, appending to dst.
func decompress(dst []Sample, data []byte, n int) []Sample {
	r := &bitReader{b: data}
	ts := int64(r.bits(64))
	v := r.bits(64)
	dst = append(dst, Sample{ts, math.Float64frombits(v)})
	var delta int64
	var lead, trail uint8
	for i := 1; i < n; i++ {
		var dod int64
		switch {
		case !r.bit():
		case !r.bit():
			dod = signExtend(r.bits(7), 7)
		case !r.bit():
			dod = signExtend(r.bits(9), 9)
		case !r.bit():
			dod = signExtend(r.bits(12), 12)
		default:
			dod = int64(r.bits(64))
		}
		delta += dod
		ts += delta

		if r.bit() {
			if r.bit() {
				lead = uint8(r.bits(5))
				sig := uint8(r.bits(6))
				if sig == 0 {
					sig = 64
				}
				trail = 64 - lead - sig
			}
			v ^= r.bits(64-lead-trail) << trail
		}
		dst = append(dst, Sample{ts, math.Float64frombits(v)})
	}
	return dst
}
