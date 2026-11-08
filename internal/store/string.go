package store

import (
	"strconv"

	"github.com/shamim3696/cache-boy/internal/compression"
)

// String is a byte string. Canonical integers are stored as an int64 and
// large values are transparently compressed. The three encodings share two
// words so a String stays a 32-byte allocation:
//
//	data == nil         int: the value is n
//	data != nil, n == 0 raw bytes
//	data != nil, n > 0  compressed bytes; n is the uncompressed length
type String struct {
	data []byte
	n    int64
}

// embstrMax is the length up to which Redis reports a string as embstr.
const embstrMax = 44

// NewString stores b, as an integer when it is the canonical form of one,
// otherwise compressed if threshold > 0 and len(b) >= threshold.
func NewString(b []byte, threshold int) *String {
	if v, ok := parseInt(b); ok {
		return &String{n: v}
	}
	if b == nil {
		b = []byte{}
	}
	s := &String{data: b}
	if threshold > 0 && len(b) >= threshold {
		if c := compression.Compress(b); c != nil {
			s.data, s.n = c, int64(len(b))
		}
	}
	return s
}

// NewInt returns an int-encoded string.
func NewInt(v int64) *String { return &String{n: v} }

// Bytes returns the value. Do not modify the result.
func (s *String) Bytes() []byte {
	switch {
	case s.data == nil:
		return strconv.AppendInt(make([]byte, 0, 20), s.n, 10)
	case s.n == 0:
		return s.data
	}
	b, err := compression.Decompress(s.data, int(s.n))
	if err != nil {
		return nil
	}
	return b
}

// Int returns the value if it is int-encoded, letting INCR and friends skip
// parsing and formatting.
func (s *String) Int() (int64, bool) { return s.n, s.data == nil }

// SetInt replaces the value with an integer in place. Earlier Bytes results
// stay valid: they are either fresh copies or byte slices that are dropped,
// not overwritten.
func (s *String) SetInt(v int64) { s.data, s.n = nil, v }

func (s *String) Len() int {
	switch {
	case s.data == nil:
		return intLen(s.n)
	case s.n == 0:
		return len(s.data)
	}
	return int(s.n)
}

func (s *String) Compressed() bool { return s.data != nil && s.n > 0 }

// StoredSize is the size of the stored representation.
func (s *String) StoredSize() int {
	if s.data == nil {
		return 8
	}
	return len(s.data)
}

func (s *String) MemSize() int64 { return int64(cap(s.data)) + 32 }

// Encoding is the OBJECT ENCODING name.
func (s *String) Encoding() string {
	switch {
	case s.data == nil:
		return "int"
	case s.n > 0:
		return "compressed"
	case len(s.data) <= embstrMax:
		return "embstr"
	}
	return "raw"
}

// intLen is the length of v formatted in decimal.
func intLen(v int64) int {
	n := 1
	u := uint64(v)
	if v < 0 {
		n++
		u = -u
	}
	for u >= 10 {
		u /= 10
		n++
	}
	return n
}
