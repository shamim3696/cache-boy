package store

// Hash is a field → value map.
//
// Small hashes are a listpack: a single []byte laid out as
// [count][flen][field][vlen][value]... (see listpack.go). That beats a
// []string of fields and values: it is one pointer-free allocation the GC
// never scans, it has no 16-byte header per string, and it doesn't pin the
// caller's argument strings. A linear scan over at most 128 short entries is
// about as fast as hashing. Like Redis, a hash that outgrows the listpack
// converts to a map once and never converts back.
type Hash struct {
	lp  []byte // used while big == nil; empty until the first Set
	big *hashTable
}

type hashTable struct {
	m     map[string]string
	bytes int64
}

const hashMaxListpackEntries = 128

func NewHash() *Hash { return &Hash{} }

// find returns the offsets of field's entry and of its value's entry, or -1.
func (h *Hash) find(field string) (fieldOff, valOff int) {
	for i := 1; i < len(h.lp); {
		n := int(h.lp[i])
		v := i + 1 + n
		if n == len(field) && string(h.lp[i+1:v]) == field {
			return i, v
		}
		i = v + 1 + int(h.lp[v])
	}
	return -1, -1
}

// Set returns true if the field is new.
func (h *Hash) Set(field, value string) bool {
	if h.big == nil && (len(field) > maxPackedValue || len(value) > maxPackedValue) {
		h.convert()
	}
	if h.big != nil {
		return h.big.set(field, value)
	}
	if f, v := h.find(field); f >= 0 {
		end := v + 1 + int(h.lp[v])
		if end-v-1 == len(value) {
			copy(h.lp[v+1:], value)
		} else {
			h.lp = lpReplace(h.lp, v, end, value)
		}
		return false
	}
	if h.Len() == hashMaxListpackEntries {
		h.convert()
		return h.big.set(field, value)
	}
	if len(h.lp) == 0 {
		h.lp = grow(nil, 3+len(field)+len(value))
		h.lp = append(h.lp, 0)
	}
	h.lp = lpAppend(lpAppend(h.lp, field), value)
	h.lp[0]++
	return true
}

func (t *hashTable) set(field, value string) bool {
	old, exists := t.m[field]
	if exists {
		t.bytes += int64(len(value) - len(old))
	} else {
		t.bytes += int64(len(field) + len(value))
	}
	t.m[field] = value
	return !exists
}

func (h *Hash) Get(field string) (string, bool) {
	if h.big != nil {
		v, ok := h.big.m[field]
		return v, ok
	}
	_, v := h.find(field)
	if v < 0 {
		return "", false
	}
	val, _ := lpEntry(h.lp, v)
	return string(val), true
}

func (h *Hash) Del(field string) bool {
	if t := h.big; t != nil {
		old, ok := t.m[field]
		if ok {
			t.bytes -= int64(len(field) + len(old))
			delete(t.m, field)
		}
		return ok
	}
	f, v := h.find(field)
	if f < 0 {
		return false
	}
	h.lp = append(h.lp[:f], h.lp[v+1+int(h.lp[v]):]...)
	h.lp[0]--
	return true
}

func (h *Hash) Len() int {
	if h.big != nil {
		return len(h.big.m)
	}
	if len(h.lp) == 0 {
		return 0
	}
	return int(h.lp[0])
}

// Range calls fn for each field until fn returns false. Order is
// unspecified. The hash must not be modified during the call.
func (h *Hash) Range(fn func(field, value string) bool) {
	if h.big != nil {
		for f, v := range h.big.m {
			if !fn(f, v) {
				return
			}
		}
		return
	}
	// One copy of the whole listpack, sliced up, costs one allocation
	// instead of two per field.
	lp := string(h.lp)
	for i := 1; i < len(lp); {
		f, next := lpEntry(lp, i)
		v, next := lpEntry(lp, next)
		if !fn(f, v) {
			return
		}
		i = next
	}
}

// Map returns the contents as a map, for read-only use. Small hashes build
// a fresh one, so prefer Range.
func (h *Hash) Map() map[string]string {
	if h.big != nil {
		return h.big.m
	}
	m := make(map[string]string, h.Len())
	h.Range(func(f, v string) bool { m[f] = v; return true })
	return m
}

func (h *Hash) MemSize() int64 {
	if t := h.big; t != nil {
		return t.bytes + int64(len(t.m))*48 + 48 + 32
	}
	return int64(cap(h.lp)) + 32
}

// Encoding is the OBJECT ENCODING name.
func (h *Hash) Encoding() string {
	if h.big != nil {
		return "hashtable"
	}
	return "listpack"
}

func (h *Hash) convert() {
	t := &hashTable{m: make(map[string]string, h.Len()+1)}
	h.Range(func(f, v string) bool { t.set(f, v); return true })
	h.lp, h.big = nil, t
}
