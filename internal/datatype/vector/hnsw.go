package vector

import (
	"math"
	"sort"
)

// hnsw is a Hierarchical Navigable Small World graph (Malkov & Yashunin,
// arXiv:1603.09320) over the index's unit vectors, with distance 1 − dot.
//
// Deleted or replaced vectors stay in the graph as tombstones (slot < 0):
// they still route searches but are never returned. The owning Index throws
// the graph away once tombstones pass a quarter of the nodes.
type hnsw struct {
	m, mMax0, efc int
	mL            float64
	nodes         []hnode
	entry         int32 // -1 while empty
	top           int   // level of entry
	dead          int
	linkBytes     int64

	// Scratch reused across calls; like the rest of Index, not safe for
	// concurrent use. Stamps avoid clearing per-node arrays on every query.
	visit []uint32
	vgen  uint32
	fseen []uint32 // filter verdict cache, valid where fseen[n] == fgen
	fok   []bool
	fgen  uint32
}

type hnode struct {
	vec   []float32
	slot  int32 // position in Index's slot arrays, -1 once tombstoned
	links [][]int32
}

// item is a node with its distance to the current query.
type item struct {
	d float32
	n int32
}

func newHNSW(m, efc int) *hnsw {
	m = max(m, 2)
	return &hnsw{m: m, mMax0: 2 * m, efc: max(efc, m), mL: 1 / math.Log(float64(m)), entry: -1}
}

func (g *hnsw) maxLinks(level int) int {
	if level == 0 {
		return g.mMax0
	}
	return g.m
}

// levelFor draws floor(-ln(U)·mL) with U derived from a hash of id rather
// than a shared RNG, so a replica or a snapshot reload that inserts the same
// ids in the same order builds the identical graph.
func levelFor(id string, mL float64) int {
	h := uint64(14695981039346656037)
	for i := 0; i < len(id); i++ {
		h ^= uint64(id[i])
		h *= 1099511628211
	}
	// splitmix64 finaliser: FNV alone leaves the high bits poorly mixed.
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	u := (float64(h>>11) + 1) / (1 << 53) // (0, 1]
	return int(-math.Log(u) * mL)
}

func dot(a, b []float32) float32 {
	b = b[:len(a)]
	var s0, s1, s2, s3, s4, s5, s6, s7 float32
	i := 0
	for ; i+8 <= len(a); i += 8 {
		a, b := a[i:i+8:i+8], b[i:i+8:i+8]
		s0 += a[0] * b[0]
		s1 += a[1] * b[1]
		s2 += a[2] * b[2]
		s3 += a[3] * b[3]
		s4 += a[4] * b[4]
		s5 += a[5] * b[5]
		s6 += a[6] * b[6]
		s7 += a[7] * b[7]
	}
	for ; i < len(a); i++ {
		s0 += a[i] * b[i]
	}
	return (s0 + s4) + (s1 + s5) + (s2 + s6) + (s3 + s7)
}

func dist(a, b []float32) float32 { return 1 - dot(a, b) }

// insert adds vec as a new node (Algorithm 1) and returns its node number.
func (g *hnsw) insert(vec []float32, slot int32, level int) int32 {
	n := int32(len(g.nodes))
	links := make([][]int32, level+1)
	for l := range links {
		links[l] = make([]int32, 0, g.maxLinks(l)+1)
		g.linkBytes += int64(cap(links[l]))*4 + 24
	}
	g.nodes = append(g.nodes, hnode{vec: vec, slot: slot, links: links})
	if g.entry < 0 {
		g.entry, g.top = n, level
		return n
	}
	ep := g.entry
	epd := dist(vec, g.nodes[ep].vec)
	for l := g.top; l > level; l-- {
		ep, epd = g.greedy(vec, ep, epd, l)
	}
	eps := []item{{epd, ep}}
	for l := min(level, g.top); l >= 0; l-- {
		w := g.searchLayer(vec, eps, g.efc, l, nil)
		for _, e := range g.selectNeighbors(w, g.m) {
			g.nodes[n].links[l] = append(g.nodes[n].links[l], e.n)
			g.connect(e.n, n, e.d, l)
		}
		eps = w
	}
	if level > g.top {
		g.entry, g.top = n, level
	}
	return n
}

// connect adds the edge e→n at level l, re-pruning e's list with the
// heuristic when it overflows so hubs don't lose their long-range links.
func (g *hnsw) connect(e, n int32, d float32, l int) {
	links := g.nodes[e].links[l]
	limit := g.maxLinks(l)
	if len(links) < limit {
		g.nodes[e].links[l] = append(links, n)
		return
	}
	ev := g.nodes[e].vec
	cands := make([]item, 0, len(links)+1)
	cands = append(cands, item{d, n})
	for _, o := range links {
		cands = append(cands, item{dist(ev, g.nodes[o].vec), o})
	}
	sortItems(cands)
	kept := g.selectNeighbors(cands, limit)
	links = links[:0]
	for _, k := range kept {
		links = append(links, k.n)
	}
	g.nodes[e].links[l] = links
}

// selectNeighbors is the heuristic of Algorithm 4: walking candidates from
// nearest, keep one only if it is closer to the base than to every neighbour
// already kept. This favours edges in diverse directions, which keeps
// clustered data navigable where plain "M closest" would not.
// cands must be sorted by distance.
func (g *hnsw) selectNeighbors(cands []item, m int) []item {
	if len(cands) <= m {
		return cands
	}
	out := make([]item, 0, m)
	for _, c := range cands {
		cv := g.nodes[c.n].vec
		good := true
		for _, r := range out {
			if dist(cv, g.nodes[r.n].vec) < c.d {
				good = false
				break
			}
		}
		if good {
			out = append(out, c)
			if len(out) == m {
				break
			}
		}
	}
	return out
}

// greedy walks level l towards q, ending at a local minimum.
func (g *hnsw) greedy(q []float32, ep int32, epd float32, l int) (int32, float32) {
	for changed := true; changed; {
		changed = false
		for _, nb := range g.nodes[ep].links[l] {
			if d := dist(q, g.nodes[nb].vec); d < epd {
				ep, epd, changed = nb, d, true
			}
		}
	}
	return ep, epd
}

// searchLayer is Algorithm 2: a best-first search keeping the ef nearest
// nodes that pass accept (nil accepts all). Rejected nodes are still
// expanded, so tombstones and filtered-out nodes keep the graph connected.
// The result is sorted nearest first.
func (g *hnsw) searchLayer(q []float32, eps []item, ef, l int, accept func(int32) bool) []item {
	if len(g.visit) < len(g.nodes) {
		g.visit = append(g.visit, make([]uint32, len(g.nodes)-len(g.visit))...)
	}
	g.vgen++
	if g.vgen == 0 {
		clear(g.visit)
		g.vgen = 1
	}
	var cand minItems
	var res maxItems
	for _, e := range eps {
		g.visit[e.n] = g.vgen
		cand.push(e)
		if accept == nil || accept(e.n) {
			res.push(e)
			if len(res) > ef {
				res.pop()
			}
		}
	}
	for len(cand) > 0 {
		c := cand.pop()
		if len(res) >= ef && c.d > res[0].d {
			break
		}
		for _, nb := range g.nodes[c.n].links[l] {
			if g.visit[nb] == g.vgen {
				continue
			}
			g.visit[nb] = g.vgen
			d := dist(q, g.nodes[nb].vec)
			if len(res) < ef || d < res[0].d {
				cand.push(item{d, nb})
				if accept == nil || accept(nb) {
					res.push(item{d, nb})
					if len(res) > ef {
						res.pop()
					}
				}
			}
		}
	}
	out := []item(res)
	sortItems(out)
	return out
}

// search returns up to ef accepted nodes near q, nearest first.
func (g *hnsw) search(q []float32, ef int, accept func(int32) bool) []item {
	if g.entry < 0 {
		return nil
	}
	ep := g.entry
	epd := dist(q, g.nodes[ep].vec)
	for l := g.top; l > 0; l-- {
		ep, epd = g.greedy(q, ep, epd, l)
	}
	return g.searchLayer(q, []item{{epd, ep}}, ef, 0, accept)
}

func (g *hnsw) kill(n int32) {
	g.nodes[n].slot = -1
	g.dead++
}

// newQuery starts a fresh filter-verdict generation.
func (g *hnsw) newQuery() {
	if len(g.fseen) < len(g.nodes) {
		g.fseen = append(g.fseen, make([]uint32, len(g.nodes)-len(g.fseen))...)
		g.fok = append(g.fok, make([]bool, len(g.nodes)-len(g.fok))...)
	}
	g.fgen++
	if g.fgen == 0 {
		clear(g.fseen)
		g.fgen = 1
	}
}

// memSize counts links, node headers and scratch; vectors are shared with
// Index and counted there.
func (g *hnsw) memSize() int64 {
	return g.linkBytes + int64(cap(g.nodes))*56 + int64(cap(g.visit))*4 + int64(cap(g.fseen))*5 + 96
}

func sortItems(s []item) {
	sort.Slice(s, func(i, j int) bool {
		if s[i].d != s[j].d {
			return s[i].d < s[j].d
		}
		return s[i].n < s[j].n
	})
}

// minItems and maxItems are hand-rolled binary heaps: container/heap's
// interface boxing costs more than the distance computations here.
type minItems []item

func (h *minItems) push(x item) {
	*h = append(*h, x)
	s := *h
	for i := len(s) - 1; i > 0; {
		p := (i - 1) / 2
		if s[p].d <= s[i].d {
			break
		}
		s[p], s[i] = s[i], s[p]
		i = p
	}
}

func (h *minItems) pop() item {
	s := *h
	top := s[0]
	last := len(s) - 1
	s[0] = s[last]
	s = s[:last]
	for i := 0; ; {
		c := 2*i + 1
		if c >= len(s) {
			break
		}
		if c+1 < len(s) && s[c+1].d < s[c].d {
			c++
		}
		if s[i].d <= s[c].d {
			break
		}
		s[i], s[c] = s[c], s[i]
		i = c
	}
	*h = s
	return top
}

type maxItems []item

func (h *maxItems) push(x item) {
	*h = append(*h, x)
	s := *h
	for i := len(s) - 1; i > 0; {
		p := (i - 1) / 2
		if s[p].d >= s[i].d {
			break
		}
		s[p], s[i] = s[i], s[p]
		i = p
	}
}

func (h *maxItems) pop() item {
	s := *h
	top := s[0]
	last := len(s) - 1
	s[0] = s[last]
	s = s[:last]
	for i := 0; ; {
		c := 2*i + 1
		if c >= len(s) {
			break
		}
		if c+1 < len(s) && s[c+1].d > s[c].d {
			c++
		}
		if s[i].d >= s[c].d {
			break
		}
		s[i], s[c] = s[c], s[i]
		i = c
	}
	*h = s
	return top
}
