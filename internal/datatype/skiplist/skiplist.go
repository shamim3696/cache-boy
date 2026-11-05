// Package skiplist implements sorted sets the same way Redis does: a skip list
// ordered by (score, member) for range queries, plus a hash map member→score
// for O(1) lookups. Each forward link stores a span so rank queries are O(log n).
package skiplist

import (
	"math"
	"math/rand/v2"
)

const (
	maxLevel = 32
	pFactor  = 0.25
)

type Pair struct {
	Member string
	Score  float64
}

type level struct {
	next *node
	span int // number of nodes skipped by this link
}

type node struct {
	member string
	score  float64
	prev   *node
	levels []level
}

type ZSet struct {
	head   *node
	tail   *node
	level  int
	length int
	dict   map[string]float64
	bytes  int64
}

func New() *ZSet {
	return &ZSet{
		head:  &node{levels: make([]level, maxLevel)},
		level: 1,
		dict:  make(map[string]float64),
	}
}

func (z *ZSet) Len() int { return z.length }

func (z *ZSet) MemSize() int64 { return z.bytes + int64(z.length)*96 + 64 }

func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.dict[member]
	return s, ok
}

// Add sets member's score. Returns true if the member is new.
func (z *ZSet) Add(member string, score float64) bool {
	if old, ok := z.dict[member]; ok {
		if old != score {
			z.delete(member, old)
			z.insert(member, score)
			z.dict[member] = score
		}
		return false
	}
	z.insert(member, score)
	z.dict[member] = score
	z.bytes += int64(len(member))
	return true
}

func (z *ZSet) Remove(member string) bool {
	score, ok := z.dict[member]
	if !ok {
		return false
	}
	z.delete(member, score)
	delete(z.dict, member)
	z.bytes -= int64(len(member))
	return true
}

// Rank returns the 0-based rank of member (ascending, or descending if reverse).
func (z *ZSet) Rank(member string, reverse bool) (int, bool) {
	score, ok := z.dict[member]
	if !ok {
		return 0, false
	}
	rank := 0
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && less(x.levels[i].next.score, x.levels[i].next.member, score, member, true) {
			rank += x.levels[i].span
			x = x.levels[i].next
		}
	}
	// rank is now 1-based position of the member.
	if reverse {
		return z.length - rank, true
	}
	return rank - 1, true
}

// RangeByRank returns members at ranks start..stop inclusive (Redis index rules).
func (z *ZSet) RangeByRank(start, stop int, reverse bool) []Pair {
	n := z.length
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
		return []Pair{}
	}
	out := make([]Pair, 0, stop-start+1)
	var x *node
	if reverse {
		x = z.byRank(n - start)
	} else {
		x = z.byRank(start + 1)
	}
	for i := start; i <= stop && x != nil; i++ {
		out = append(out, Pair{x.member, x.score})
		if reverse {
			x = x.prev
		} else {
			x = x.levels[0].next
		}
	}
	return out
}

// Bound is one end of a score range. Exclusive is the "(" prefix in Redis.
type Bound struct {
	Value     float64
	Exclusive bool
}

func (b Bound) okMin(s float64) bool {
	if b.Exclusive {
		return s > b.Value
	}
	return s >= b.Value
}

func (b Bound) okMax(s float64) bool {
	if b.Exclusive {
		return s < b.Value
	}
	return s <= b.Value
}

// RangeByScore returns members with min <= score <= max. offset/count paginate
// (count < 0 means no limit). With reverse, iteration goes from max down.
func (z *ZSet) RangeByScore(min, max Bound, reverse bool, offset, count int) []Pair {
	out := []Pair{}
	var x *node
	if reverse {
		x = z.lastInRange(max)
	} else {
		x = z.firstInRange(min)
	}
	for x != nil && count != 0 {
		if reverse && !min.okMin(x.score) || !reverse && !max.okMax(x.score) {
			break
		}
		if offset > 0 {
			offset--
		} else {
			out = append(out, Pair{x.member, x.score})
			if count > 0 {
				count--
			}
		}
		if reverse {
			x = x.prev
		} else {
			x = x.levels[0].next
		}
	}
	return out
}

// CountByScore returns the number of members in [min, max].
func (z *ZSet) CountByScore(min, max Bound) int {
	first := z.firstInRange(min)
	if first == nil || !max.okMax(first.score) {
		return 0
	}
	last := z.lastInRange(max)
	r1, _ := z.Rank(first.member, false)
	r2, _ := z.Rank(last.member, false)
	return r2 - r1 + 1
}

// LexBound is one end of a lexicographic range: "-" , "+", "[a", "(a".
type LexBound struct {
	Value     string
	Exclusive bool
	Inf       int // -1 = "-", +1 = "+", 0 = Value
}

func (b LexBound) okMin(s string) bool {
	switch b.Inf {
	case -1:
		return true
	case 1:
		return false
	}
	if b.Exclusive {
		return s > b.Value
	}
	return s >= b.Value
}

func (b LexBound) okMax(s string) bool {
	switch b.Inf {
	case -1:
		return false
	case 1:
		return true
	}
	if b.Exclusive {
		return s < b.Value
	}
	return s <= b.Value
}

// RangeByLex assumes all members have the same score (Redis ZRANGEBYLEX rules).
func (z *ZSet) RangeByLex(min, max LexBound, reverse bool, offset, count int) []Pair {
	out := []Pair{}
	all := z.RangeByRank(0, -1, reverse)
	for _, p := range all {
		if !min.okMin(p.Member) || !max.okMax(p.Member) {
			continue
		}
		if count == 0 {
			break
		}
		if offset > 0 {
			offset--
			continue
		}
		out = append(out, p)
		if count > 0 {
			count--
		}
	}
	return out
}

// PopMin removes and returns up to n lowest (or highest with max) members.
func (z *ZSet) Pop(n int, max bool) []Pair {
	var out []Pair
	for i := 0; i < n && z.length > 0; i++ {
		var x *node
		if max {
			x = z.tail
		} else {
			x = z.head.levels[0].next
		}
		out = append(out, Pair{x.member, x.score})
		z.Remove(x.member)
	}
	return out
}

// ForEach visits members in ascending order until fn returns false.
func (z *ZSet) ForEach(fn func(Pair) bool) {
	for x := z.head.levels[0].next; x != nil; x = x.levels[0].next {
		if !fn(Pair{x.member, x.score}) {
			return
		}
	}
}

// --- internals ---

// less orders by score, then member. With orEqual, equal elements count as less.
func less(s1 float64, m1 string, s2 float64, m2 string, orEqual bool) bool {
	if s1 != s2 {
		return s1 < s2
	}
	if orEqual {
		return m1 <= m2
	}
	return m1 < m2
}

func randomLevel() int {
	l := 1
	for l < maxLevel && rand.Float64() < pFactor {
		l++
	}
	return l
}

func (z *ZSet) insert(member string, score float64) {
	var update [maxLevel]*node
	var rank [maxLevel]int
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		if i < z.level-1 {
			rank[i] = rank[i+1]
		}
		for x.levels[i].next != nil && less(x.levels[i].next.score, x.levels[i].next.member, score, member, false) {
			rank[i] += x.levels[i].span
			x = x.levels[i].next
		}
		update[i] = x
	}
	lvl := randomLevel()
	if lvl > z.level {
		for i := z.level; i < lvl; i++ {
			rank[i] = 0
			update[i] = z.head
			update[i].levels[i].span = z.length
		}
		z.level = lvl
	}
	x = &node{member: member, score: score, levels: make([]level, lvl)}
	for i := 0; i < lvl; i++ {
		x.levels[i].next = update[i].levels[i].next
		update[i].levels[i].next = x
		x.levels[i].span = update[i].levels[i].span - (rank[0] - rank[i])
		update[i].levels[i].span = (rank[0] - rank[i]) + 1
	}
	for i := lvl; i < z.level; i++ {
		update[i].levels[i].span++
	}
	if update[0] != z.head {
		x.prev = update[0]
	}
	if x.levels[0].next != nil {
		x.levels[0].next.prev = x
	} else {
		z.tail = x
	}
	z.length++
}

func (z *ZSet) delete(member string, score float64) {
	var update [maxLevel]*node
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && less(x.levels[i].next.score, x.levels[i].next.member, score, member, false) {
			x = x.levels[i].next
		}
		update[i] = x
	}
	x = x.levels[0].next
	if x == nil || x.score != score || x.member != member {
		return
	}
	for i := 0; i < z.level; i++ {
		if update[i].levels[i].next == x {
			update[i].levels[i].span += x.levels[i].span - 1
			update[i].levels[i].next = x.levels[i].next
		} else {
			update[i].levels[i].span--
		}
	}
	if x.levels[0].next != nil {
		x.levels[0].next.prev = x.prev
	} else {
		z.tail = x.prev
	}
	for z.level > 1 && z.head.levels[z.level-1].next == nil {
		z.level--
	}
	z.length--
}

// byRank returns the node at 1-based rank.
func (z *ZSet) byRank(rank int) *node {
	traversed := 0
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && traversed+x.levels[i].span <= rank {
			traversed += x.levels[i].span
			x = x.levels[i].next
		}
		if traversed == rank {
			return x
		}
	}
	return nil
}

func (z *ZSet) firstInRange(min Bound) *node {
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && !min.okMin(x.levels[i].next.score) {
			x = x.levels[i].next
		}
	}
	return x.levels[0].next
}

func (z *ZSet) lastInRange(max Bound) *node {
	x := z.head
	for i := z.level - 1; i >= 0; i-- {
		for x.levels[i].next != nil && max.okMax(x.levels[i].next.score) {
			x = x.levels[i].next
		}
	}
	if x == z.head {
		return nil
	}
	return x
}

// Inf helpers for parsing "-inf"/"+inf".
var (
	NegInf = math.Inf(-1)
	PosInf = math.Inf(1)
)
