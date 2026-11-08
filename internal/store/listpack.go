package store

// Helpers for the packed small encodings of Hash and Set.
//
// A listpack here is a run of entries, each a one-byte length followed by
// that many bytes. One length byte is enough because packed entries are at
// most maxPackedValue bytes long; anything longer converts the container to
// a hashtable first.

const maxPackedValue = 64

// lpEntry returns the entry at off and the offset of the next one.
func lpEntry[T string | []byte](b T, off int) (entry T, next int) {
	n := int(b[off])
	next = off + 1 + n
	return b[off+1 : next], next
}

// lpAppend appends s as a new entry.
func lpAppend(b []byte, s string) []byte {
	b = grow(b, 1+len(s))
	b = append(b, byte(len(s)))
	return append(b, s...)
}

// lpReplace overwrites the entry spanning b[i:j] with s.
func lpReplace(b []byte, i, j int, s string) []byte {
	delta := 1 + len(s) - (j - i)
	switch {
	case delta > 0:
		b = grow(b, delta)
		b = b[:len(b)+delta]
		copy(b[j+delta:], b[j:len(b)-delta])
	case delta < 0:
		copy(b[j+delta:], b[j:])
		b = b[:len(b)+delta]
	}
	b[i] = byte(len(s))
	copy(b[i+1:], s)
	return b
}

// grow makes room for n more bytes. Rather than append's doubling it rounds
// up to roughly the Go allocator's size class: packed values exist to save
// memory, and the occasional copy is cheap at these sizes.
func grow(b []byte, n int) []byte {
	need := len(b) + n
	if need <= cap(b) {
		return b
	}
	var c int
	switch {
	case need <= 32:
		c = (need + 7) &^ 7
	case need <= 256:
		c = (need + 15) &^ 15
	case need <= 512:
		c = (need + 31) &^ 31
	default:
		c = (need + need/8 + 63) &^ 63
	}
	nb := make([]byte, len(b), c)
	copy(nb, b)
	return nb
}

// parseInt parses s if it is the canonical decimal form of an int64, the
// same rule as Redis' string2ll: no sign other than '-', no leading zeros,
// no "-0", no spaces and no overflow. Only such strings may be stored as
// integers, since formatting them back must reproduce the original bytes.
func parseInt[T string | []byte](s T) (int64, bool) {
	n := len(s)
	if n == 0 || n > 20 {
		return 0, false
	}
	if n == 1 && s[0] == '0' {
		return 0, true
	}
	i, neg := 0, s[0] == '-'
	if neg {
		i = 1
	}
	if i == n || s[i] < '1' || s[i] > '9' {
		return 0, false
	}
	var u uint64
	for ; i < n; i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		if u > (1<<64-1)/10 {
			return 0, false
		}
		u *= 10
		d := uint64(c - '0')
		if u > 1<<64-1-d {
			return 0, false
		}
		u += d
	}
	if neg {
		if u > 1<<63 {
			return 0, false
		}
		return int64(-u), true
	}
	if u > 1<<63-1 {
		return 0, false
	}
	return int64(u), true
}
