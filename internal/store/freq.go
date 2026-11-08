package store

// sketch is a TinyLFU frequency estimator: a Count-Min Sketch of 4-bit
// counters behind a doorkeeper Bloom filter. It remembers how often keys were
// used recently, including keys that are no longer stored, which is what lets
// eviction tell a popular key from one that was touched once by a scan.
//
// There is one sketch per shard and it is only used under the shard lock, so
// it needs no synchronisation of its own.
type sketch struct {
	// table holds 4 rows of width counters, 16 counters per word. The 4
	// counters of a key live in one 64-byte block (8 words), so an update
	// costs a single cache miss.
	table     []uint64
	blockMask uint32
	capacity  int // keys the sketch was sized for; the shard grows it past that

	// door absorbs the first access of each key: keys seen once never reach
	// the table, so one-hit wonders don't inflate it.
	door     []uint64
	doorMask uint32

	additions int // accesses since the last aging
	period    int // aging period W
}

const (
	sketchMinWidth = 256
	agingFactor    = 10 // W = agingFactor × width, as in the TinyLFU paper

	// sketchPerKey is the row width per stored key. The sketch must also
	// count keys that are not stored (that's how it recognises a returning
	// key), and in the hit-ratio simulation 4 counters per key scored 4 points
	// higher than 1 at a cost of 12 bytes per key.
	sketchPerKey = 4
	nibbleMask   = 0xf
	halveMask    = 0x7777777777777777
)

// newSketch returns a sketch sized for a shard of the given number of keys.
func newSketch(keys int) *sketch {
	w := sketchMinWidth
	for w < keys*sketchPerKey {
		w <<= 1
	}
	return &sketch{
		table:     make([]uint64, w/4), // 4 rows × w counters × 4 bits
		blockMask: uint32(w/32 - 1),    // 8 words = 128 counters = 32 per row
		capacity:  w / sketchPerKey,
		door:      make([]uint64, w/8), // 8 bits per counter slot
		doorMask:  uint32(w/8 - 1),
		period:    agingFactor * w,
	}
}

// block returns the 64-byte block holding the 4 counters of hash x. Row r's
// counter is in word 2r or 2r+1 of the block, chosen with the nibble by 5
// bits of x>>32.
func (s *sketch) block(x uint64) []uint64 {
	base := int(uint32(x)&s.blockMask) * 8
	return s.table[base : base+8 : base+8]
}

// doorBits returns the doorkeeper word of hash x and the mask of its two
// bits. Both bits sit in one word (a blocked Bloom filter) so a check costs
// one cache miss.
func (s *sketch) doorBits(x uint64) (*uint64, uint64) {
	w := &s.door[uint32((x*0x9e3779b97f4a7c15)>>32)&s.doorMask]
	return w, 1<<(x>>52&63) | 1<<(x>>58)
}

// increment records one access. x must come from sketchHash.
func (s *sketch) increment(x uint64) {
	s.additions++
	if s.additions >= s.period {
		s.age()
	}
	if w, bits := s.doorBits(x); *w&bits != bits {
		*w |= bits
		return
	}
	blk, h := s.block(x), x>>32
	for r := 0; r < 8; r += 2 {
		w, sh := &blk[(r+int(h&1))&7], h>>1&15*4
		c := *w >> sh & nibbleMask
		*w += ((c+1)>>4 ^ 1) << sh // +1 unless saturated at 15
		h >>= 5
	}
}

// estimate returns the access frequency of x, 0..16.
func (s *sketch) estimate(x uint64) uint8 {
	blk, h := s.block(x), x>>32
	min := uint64(nibbleMask)
	for r := 0; r < 8; r += 2 {
		min = minCount(min, blk[(r+int(h&1))&7]>>(h>>1&15*4)&nibbleMask)
		h >>= 5
	}
	if w, bits := s.doorBits(x); *w&bits == bits {
		min++
	}
	return uint8(min)
}

func minCount(a, b uint64) uint64 {
	if b < a {
		return b
	}
	return a
}

// age halves every counter and clears the doorkeeper, so the sketch follows
// changes in popularity instead of remembering all history.
func (s *sketch) age() {
	for i := range s.table {
		s.table[i] = (s.table[i] >> 1) & halveMask
	}
	clear(s.door)
	s.additions /= 2
}

// sketchHash spreads a key hash over 64 bits (murmur3's fmix64). The shard
// index uses the low bits of the key hash, so without this step every key in
// a shard would share them.
func sketchHash(h32 uint32) uint64 {
	h := uint64(h32)
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// hashKey is 32-bit FNV-1a. Each key is hashed once per operation: the result
// picks the shard, feeds the sketch and is kept in the Entry, so eviction can
// look up a sampled key's frequency without reading the key's bytes again.
func hashKey(key string) uint32 {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return h
}
