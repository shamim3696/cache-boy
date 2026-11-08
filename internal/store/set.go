package store

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"strconv"
)

// Set is a set of strings with three encodings, as in Redis:
//
//   - intset: while every member is a canonical int64, a sorted array of
//     2-, 4- or 8-byte little-endian integers (the narrowest width that fits
//     them all), searched by bisection;
//   - listpack: up to 128 members of at most 64 bytes, packed one after
//     another (see listpack.go) and searched linearly;
//   - hashtable: a map, for anything bigger. Sets never convert back.
//
// The packed encodings share one []byte whose first byte says which one it
// is: the integer width, or setListpack. For a listpack the second byte is
// the member count.
type Set struct {
	data []byte // used while big == nil; empty until the first Add
	big  *setTable
}

type setTable struct {
	m     map[string]struct{}
	bytes int64
}

const (
	setListpack           = 1
	setMaxIntsetEntries   = 512
	setMaxListpackEntries = 128
	setListpackHeader     = 2
	setIntsetHeader       = 1
)

func NewSet() *Set { return &Set{} }

func (s *Set) isIntset() bool { return len(s.data) > 0 && s.data[0] != setListpack }

// Add returns true if the member is new.
func (s *Set) Add(member string) bool {
	if s.big == nil && (len(s.data) == 0 || s.isIntset()) {
		if v, ok := parseInt(member); ok {
			if len(s.data) == 0 {
				w := intWidth(v)
				s.data = make([]byte, setIntsetHeader+int(w), 8)
				s.data[0] = w
				putInt(s.data[setIntsetHeader:], w, v)
				return true
			}
			i, found := s.search(v)
			if found {
				return false
			}
			if s.Len() < setMaxIntsetEntries {
				s.insertInt(i, v)
				return true
			}
			s.convert()
		} else if s.Len() < setMaxListpackEntries && len(member) <= maxPackedValue {
			s.toListpack()
		} else {
			s.convert()
		}
	}
	if s.big == nil {
		if s.lpFind(member) >= 0 {
			return false
		}
		if s.Len() < setMaxListpackEntries && len(member) <= maxPackedValue {
			s.data = lpAppend(s.data, member)
			s.data[1]++
			return true
		}
		s.convert()
	}
	t := s.big
	if _, ok := t.m[member]; ok {
		return false
	}
	t.m[member] = struct{}{}
	t.bytes += int64(len(member))
	return true
}

func (s *Set) Remove(member string) bool {
	switch {
	case s.big != nil:
		t := s.big
		if _, ok := t.m[member]; !ok {
			return false
		}
		delete(t.m, member)
		t.bytes -= int64(len(member))
		return true
	case s.isIntset():
		v, ok := parseInt(member)
		if !ok {
			return false
		}
		i, found := s.search(v)
		if found {
			w := int(s.data[0])
			off := setIntsetHeader + i*w
			s.data = append(s.data[:off], s.data[off+w:]...)
		}
		return found
	case len(s.data) > 0:
		i := s.lpFind(member)
		if i < 0 {
			return false
		}
		s.data = append(s.data[:i], s.data[i+1+int(s.data[i]):]...)
		s.data[1]--
		return true
	}
	return false
}

func (s *Set) Has(member string) bool {
	switch {
	case s.big != nil:
		_, ok := s.big.m[member]
		return ok
	case s.isIntset():
		v, ok := parseInt(member)
		if !ok {
			return false
		}
		_, found := s.search(v)
		return found
	case len(s.data) > 0:
		return s.lpFind(member) >= 0
	}
	return false
}

func (s *Set) Len() int {
	switch {
	case s.big != nil:
		return len(s.big.m)
	case s.isIntset():
		return (len(s.data) - setIntsetHeader) / int(s.data[0])
	case len(s.data) > 0:
		return int(s.data[1])
	}
	return 0
}

// Range calls fn for each member until fn returns false. Order is
// unspecified. The set must not be modified during the call.
func (s *Set) Range(fn func(member string) bool) {
	switch {
	case s.big != nil:
		for m := range s.big.m {
			if !fn(m) {
				return
			}
		}
	case s.isIntset():
		for i := range s.Len() {
			if !fn(strconv.FormatInt(s.intAt(i), 10)) {
				return
			}
		}
	default:
		lp := string(s.data) // one allocation rather than one per member
		for i := setListpackHeader; i < len(lp); {
			m, next := lpEntry(lp, i)
			if !fn(m) {
				return
			}
			i = next
		}
	}
}

func (s *Set) Members() []string {
	out := make([]string, 0, s.Len())
	s.Range(func(m string) bool { out = append(out, m); return true })
	return out
}

// Random returns any member.
func (s *Set) Random() (string, bool) {
	n := s.Len()
	switch {
	case n == 0:
		return "", false
	case s.big != nil:
		for m := range s.big.m { // map iteration order is randomised in Go
			return m, true
		}
	case s.isIntset():
		return strconv.FormatInt(s.intAt(rand.IntN(n)), 10), true
	}
	i := setListpackHeader
	for range rand.IntN(n) {
		_, i = lpEntry(s.data, i)
	}
	m, _ := lpEntry(s.data, i)
	return string(m), true
}

func (s *Set) MemSize() int64 {
	if t := s.big; t != nil {
		return t.bytes + int64(len(t.m))*32 + 48 + 32
	}
	return int64(cap(s.data)) + 32
}

// Encoding is the OBJECT ENCODING name.
func (s *Set) Encoding() string {
	switch {
	case s.big != nil:
		return "hashtable"
	case len(s.data) > 0 && s.data[0] == setListpack:
		return "listpack"
	}
	return "intset"
}

// lpFind returns the offset of member's entry, or -1.
func (s *Set) lpFind(member string) int {
	for i := setListpackHeader; i < len(s.data); {
		n := int(s.data[i])
		next := i + 1 + n
		if n == len(member) && string(s.data[i+1:next]) == member {
			return i
		}
		i = next
	}
	return -1
}

func (s *Set) toListpack() {
	lp := grow(nil, setListpackHeader+s.Len()*8)
	lp = append(lp, setListpack, 0)
	var buf [20]byte
	for i := range s.Len() {
		b := strconv.AppendInt(buf[:0], s.intAt(i), 10)
		lp = grow(lp, 1+len(b))
		lp = append(append(lp, byte(len(b))), b...)
		lp[1]++
	}
	s.data = lp
}

func (s *Set) convert() {
	t := &setTable{m: make(map[string]struct{}, s.Len()+1)}
	s.Range(func(m string) bool {
		t.m[m] = struct{}{}
		t.bytes += int64(len(m))
		return true
	})
	s.data, s.big = nil, t
}

// intWidth is the narrowest intset width that holds v.
func intWidth(v int64) byte {
	switch {
	case v >= math.MinInt16 && v <= math.MaxInt16:
		return 2
	case v >= math.MinInt32 && v <= math.MaxInt32:
		return 4
	}
	return 8
}

func putInt(b []byte, w byte, v int64) {
	switch w {
	case 2:
		binary.LittleEndian.PutUint16(b, uint16(v))
	case 4:
		binary.LittleEndian.PutUint32(b, uint32(v))
	default:
		binary.LittleEndian.PutUint64(b, uint64(v))
	}
}

func getInt(b []byte, w byte) int64 {
	switch w {
	case 2:
		return int64(int16(binary.LittleEndian.Uint16(b)))
	case 4:
		return int64(int32(binary.LittleEndian.Uint32(b)))
	}
	return int64(binary.LittleEndian.Uint64(b))
}

func (s *Set) intAt(i int) int64 {
	w := s.data[0]
	return getInt(s.data[setIntsetHeader+i*int(w):], w)
}

// search returns v's index, or where it would be inserted.
func (s *Set) search(v int64) (int, bool) {
	n := s.Len()
	if intWidth(v) > s.data[0] {
		if v < 0 {
			return 0, false
		}
		return n, false
	}
	lo, hi := 0, n
	for lo < hi {
		mid := int(uint(lo+hi) >> 1)
		if s.intAt(mid) < v {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, lo < n && s.intAt(lo) == v
}

// insertInt inserts v at index i, widening every element first if v needs
// more bytes (v is then outside the old range, so i is 0 or Len()).
func (s *Set) insertInt(i int, v int64) {
	w, nw := s.data[0], intWidth(v)
	n := s.Len()
	if nw > w {
		data := grow(nil, setIntsetHeader+(n+1)*int(nw))[:setIntsetHeader+(n+1)*int(nw)]
		data[0] = nw
		for j := range n {
			k := j
			if j >= i {
				k++
			}
			putInt(data[setIntsetHeader+k*int(nw):], nw, s.intAt(j))
		}
		putInt(data[setIntsetHeader+i*int(nw):], nw, v)
		s.data = data
		return
	}
	off := setIntsetHeader + i*int(w)
	s.data = grow(s.data, int(w))
	s.data = s.data[:len(s.data)+int(w)]
	copy(s.data[off+int(w):], s.data[off:])
	putInt(s.data[off:], w, v)
}
