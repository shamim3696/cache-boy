// Package deque is a growable ring buffer of strings used for Redis lists.
// Push/pop at both ends are O(1); index access is O(1).
package deque

const minCap = 8

type Deque struct {
	buf   []string
	head  int // index of first element
	n     int
	bytes int64 // total length of all elements, for memory accounting
}

func New() *Deque { return &Deque{buf: make([]string, minCap)} }

func (d *Deque) Len() int { return d.n }

// MemSize is an estimate of the memory used, in bytes.
func (d *Deque) MemSize() int64 { return int64(len(d.buf))*16 + d.bytes + 32 }

func (d *Deque) PushBack(s string) {
	d.grow()
	d.buf[(d.head+d.n)&(len(d.buf)-1)] = s
	d.n++
	d.bytes += int64(len(s))
}

func (d *Deque) PushFront(s string) {
	d.grow()
	d.head = (d.head - 1) & (len(d.buf) - 1)
	d.buf[d.head] = s
	d.n++
	d.bytes += int64(len(s))
}

func (d *Deque) PopFront() (string, bool) {
	if d.n == 0 {
		return "", false
	}
	s := d.buf[d.head]
	d.buf[d.head] = ""
	d.head = (d.head + 1) & (len(d.buf) - 1)
	d.n--
	d.bytes -= int64(len(s))
	d.shrink()
	return s, true
}

func (d *Deque) PopBack() (string, bool) {
	if d.n == 0 {
		return "", false
	}
	i := (d.head + d.n - 1) & (len(d.buf) - 1)
	s := d.buf[i]
	d.buf[i] = ""
	d.n--
	d.bytes -= int64(len(s))
	d.shrink()
	return s, true
}

// At returns element i (0 = front). i must be in range.
func (d *Deque) At(i int) string { return d.buf[(d.head+i)&(len(d.buf)-1)] }

// Set replaces element i. i must be in range.
func (d *Deque) Set(i int, s string) {
	p := &d.buf[(d.head+i)&(len(d.buf)-1)]
	d.bytes += int64(len(s) - len(*p))
	*p = s
}

// Index converts a Redis index (negative counts from the end) to 0-based; ok is false if out of range.
func (d *Deque) Index(i int) (int, bool) {
	if i < 0 {
		i += d.n
	}
	return i, i >= 0 && i < d.n
}

// Range returns elements start..stop inclusive using Redis LRANGE semantics.
func (d *Deque) Range(start, stop int) []string {
	start, stop, ok := ClampRange(start, stop, d.n)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, stop-start+1)
	for i := start; i <= stop; i++ {
		out = append(out, d.At(i))
	}
	return out
}

// Trim keeps only start..stop (LTRIM semantics).
func (d *Deque) Trim(start, stop int) {
	start, stop, ok := ClampRange(start, stop, d.n)
	if !ok {
		d.Clear()
		return
	}
	kept := d.Range(start, stop)
	d.Clear()
	for _, s := range kept {
		d.PushBack(s)
	}
}

// Insert puts s before position i (0..Len). O(n).
func (d *Deque) Insert(i int, s string) {
	items := d.Slice()
	items = append(items[:i], append([]string{s}, items[i:]...)...)
	d.reset(items)
}

// Remove deletes up to count elements equal to s. count > 0 scans from the
// front, count < 0 from the back, 0 removes all. Returns the number removed.
func (d *Deque) Remove(s string, count int) int {
	items := d.Slice()
	removed := 0
	limit := count
	if limit < 0 {
		limit = -limit
	}
	keep := make([]bool, len(items))
	for i := range keep {
		keep[i] = true
	}
	visit := func(i int) bool {
		if items[i] == s && (limit == 0 || removed < limit) {
			keep[i] = false
			removed++
		}
		return limit == 0 || removed < limit
	}
	if count >= 0 {
		for i := 0; i < len(items) && visit(i); i++ {
		}
	} else {
		for i := len(items) - 1; i >= 0 && visit(i); i-- {
		}
	}
	if removed == 0 {
		return 0
	}
	out := items[:0]
	for i, it := range items {
		if keep[i] {
			out = append(out, it)
		}
	}
	d.reset(out)
	return removed
}

// Find returns the position of the first element equal to s, or -1.
func (d *Deque) Find(s string) int {
	for i := 0; i < d.n; i++ {
		if d.At(i) == s {
			return i
		}
	}
	return -1
}

// Slice returns a copy of all elements, front to back.
func (d *Deque) Slice() []string { return d.Range(0, -1) }

func (d *Deque) Clear() {
	d.buf = make([]string, minCap)
	d.head, d.n, d.bytes = 0, 0, 0
}

func (d *Deque) reset(items []string) {
	d.Clear()
	for _, s := range items {
		d.PushBack(s)
	}
}

func (d *Deque) grow() {
	if d.n < len(d.buf) {
		return
	}
	d.resize(len(d.buf) * 2)
}

func (d *Deque) shrink() {
	if len(d.buf) > minCap && d.n < len(d.buf)/4 {
		d.resize(len(d.buf) / 2)
	}
}

func (d *Deque) resize(size int) {
	nb := make([]string, size)
	for i := 0; i < d.n; i++ {
		nb[i] = d.At(i)
	}
	d.buf, d.head = nb, 0
}

// ClampRange applies Redis start/stop rules (negative = from end, inclusive stop).
func ClampRange(start, stop, n int) (int, int, bool) {
	if start < 0 {
		start += n
	}
	if stop < 0 {
		stop += n
	}
	if start < 0 {
		start = 0
	}
	if stop >= n {
		stop = n - 1
	}
	if start > stop || start >= n {
		return 0, 0, false
	}
	return start, stop, true
}
