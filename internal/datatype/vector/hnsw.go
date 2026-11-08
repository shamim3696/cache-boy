package vector

import (
	"cmp"
	"math"
	"slices"
)

// hnsw is a Hierarchical Navigable Small World graph (Malkov & Yashunin,
// arXiv:1603.09320) over the index's unit vectors, with distance 1 − dot.
//
// Deleted vectors stay in the graph as tombstones (slot < 0): they still
// route searches but are never returned. The next insert takes over a
// tombstone's node instead of growing the graph, repairing its neighbours
// locally, so churn never needs a full rebuild and memory stays bounded by
// the peak number of live vectors.
type hnsw struct {
	m, mMax0, efc int
	mL            float64
	nodes         []hnode
	free          []int32 // tombstoned nodes, reused LIFO
	entry         int32   // -1 while empty
	top           int     // level of entry
	linkBytes     int64

	// Scratch reused across calls; like the rest of Index, not safe for
	// concurrent use. Stamps avoid clearing per-node arrays on every query.
	visit []uint32
	vgen  uint32
	fseen []uint32 // filter verdict cache, valid where fseen[n] == fgen
	fok   []bool
	fgen  uint32
	excl  int32 // node being re-linked, invisible to its own search
	cand  minItems
	res   maxItems
	eps   []item
	sel   []item
	prune []item
	nbrs  []int32
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
	return &hnsw{m: m, mMax0: 2 * m, efc: max(efc, m), mL: 1 / math.Log(float64(m)), entry: -1, excl: -1}
}

func (g *hnsw) maxLinks(level int) int {
	if level == 0 {
		return g.mMax0
	}
	return g.m
}

func (g *hnsw) live() int { return len(g.nodes) - len(g.free) }

// levelFor draws floor(-ln(U)·mL) with U derived from a hash of id rather
// than a shared RNG, so a replica or a snapshot reload that applies the same
// operations in the same order builds the identical graph.
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

// insert stores vec, reusing a tombstoned node when one is free, and
// returns its node number. level is used only for a brand-new node; a
// reused node keeps its own so the layers above stay consistent.
func (g *hnsw) insert(vec []float32, slot int32, level int) int32 {
	if n, ok := g.takeFree(); ok {
		g.unlink(n)
		g.nodes[n].vec, g.nodes[n].slot = vec, slot
		g.link(n, len(g.nodes[n].links)-1)
		return n
	}
	n := int32(len(g.nodes))
	links := make([][]int32, level+1)
	for l := range links {
		links[l] = make([]int32, 0, g.maxLinks(l)+1)
		g.linkBytes += int64(cap(links[l]))*4 + 24
	}
	g.nodes = append(g.nodes, hnode{vec: vec, slot: slot, links: links})
	if len(g.visit) < len(g.nodes) {
		g.visit = append(g.visit, make([]uint32, len(g.nodes)-len(g.visit))...)
	}
	if g.entry < 0 {
		g.entry, g.top = n, level
		return n
	}
	g.link(n, level)
	if level > g.top {
		g.entry, g.top = n, level
	}
	return n
}

// takeFree pops a tombstone to reuse. The entry point is never reused:
// clearing its links would cut every search off at the top.
func (g *hnsw) takeFree() (int32, bool) {
	k := len(g.free) - 1
	if k >= 0 && g.free[k] == g.entry {
		k--
	}
	if k < 0 {
		return 0, false
	}
	n := g.free[k]
	g.free = slices.Delete(g.free, k, k+1)
	return n, true
}

// link connects node n, whose links are empty, at levels 0..level
// (Algorithm 1 without the node allocation).
func (g *hnsw) link(n int32, level int) {
	vec := g.nodes[n].vec
	g.excl = n
	defer func() { g.excl = -1 }()
	ep := g.entry
	epd := dist(vec, g.nodes[ep].vec)
	for l := g.top; l > level; l-- {
		ep, epd = g.greedy(vec, ep, epd, l)
	}
	g.eps = append(g.eps[:0], item{epd, ep})
	for l := min(level, g.top); l >= 0; l-- {
		w := g.searchLayer(vec, g.eps, g.efc, l, nil)
		g.sel = g.selectNeighbors(w, g.m, g.sel[:0])
		for _, e := range g.sel {
			g.nodes[n].links[l] = append(g.nodes[n].links[l], e.n)
		}
		g.eps = append(g.eps[:0], w...)
		for _, e := range g.sel {
			g.connect(e.n, n, e.d, l)
		}
	}
}

// unlink detaches tombstone n before reuse. Each out-neighbour e drops its
// edge to n and re-selects its links with the heuristic over its own links
// plus n's other neighbours, the two-hop set that n used to bridge. Edges
// into n from elsewhere are left and will point at the new vector, as in
// hnswlib's replace_deleted; they remain valid, if longer, routes.
func (g *hnsw) unlink(n int32) {
	for l, out := range g.nodes[n].links {
		g.nbrs = append(g.nbrs[:0], out...)
		for _, e := range g.nbrs {
			ev := g.nodes[e].vec
			g.newVisit()
			g.visit[e], g.visit[n] = g.vgen, g.vgen
			g.prune = g.prune[:0]
			for _, src := range [2][]int32{g.nodes[e].links[l], g.nbrs} {
				for _, c := range src {
					if g.visit[c] != g.vgen {
						g.visit[c] = g.vgen
						g.prune = append(g.prune, item{dist(ev, g.nodes[c].vec), c})
					}
				}
			}
			sortItems(g.prune)
			g.setLinks(e, l, g.prune)
		}
		g.nodes[n].links[l] = g.nodes[n].links[l][:0]
	}
}

// connect adds the edge e→n at level l, re-pruning e's list with the
// heuristic when it overflows so hubs don't lose their long-range links.
func (g *hnsw) connect(e, n int32, d float32, l int) {
	links := g.nodes[e].links[l]
	if slices.Contains(links, n) {
		return
	}
	if len(links) < g.maxLinks(l) {
		g.nodes[e].links[l] = append(links, n)
		return
	}
	ev := g.nodes[e].vec
	g.prune = append(g.prune[:0], item{d, n})
	for _, o := range links {
		g.prune = append(g.prune, item{dist(ev, g.nodes[o].vec), o})
	}
	sortItems(g.prune)
	g.setLinks(e, l, g.prune)
}

// setLinks replaces e's links at l with the heuristic choice from sorted
// cands. It works in place: selection only ever keeps a prefix-ordered
// subset, so writing over cands as we go is safe.
func (g *hnsw) setLinks(e int32, l int, cands []item) {
	kept := g.selectNeighbors(cands, g.maxLinks(l), cands[:0])
	links := g.nodes[e].links[l][:0]
	for _, k := range kept {
		links = append(links, k.n)
	}
	g.nodes[e].links[l] = links
}

// selectNeighbors is the heuristic of Algorithm 4: walking candidates from
// nearest, keep one only if it is closer to the base than to every neighbour
// already kept. This favours edges in diverse directions, which keeps
// clustered data navigable where plain "M closest" would not. cands must be
// sorted by distance; the result is appended to out, which may alias cands.
func (g *hnsw) selectNeighbors(cands []item, m int, out []item) []item {
	if len(cands) <= m {
		return append(out, cands...)
	}
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
			if nb == g.excl {
				continue
			}
			if d := dist(q, g.nodes[nb].vec); d < epd {
				ep, epd, changed = nb, d, true
			}
		}
	}
	return ep, epd
}

func (g *hnsw) newVisit() {
	g.vgen++
	if g.vgen == 0 {
		clear(g.visit)
		g.vgen = 1
	}
}

// searchLayer is Algorithm 2: a best-first search keeping the ef nearest
// nodes that pass accept (nil accepts all). Rejected nodes are still
// expanded, so tombstones and filtered-out nodes keep the graph connected.
// The result is sorted nearest first and valid until the next call.
func (g *hnsw) searchLayer(q []float32, eps []item, ef, l int, accept func(int32) bool) []item {
	g.newVisit()
	if g.excl >= 0 {
		g.visit[g.excl] = g.vgen
	}
	g.cand, g.res = g.cand[:0], g.res[:0]
	for _, e := range eps {
		g.visit[e.n] = g.vgen
		g.cand.push(e)
		if accept == nil || accept(e.n) {
			g.res.push(e)
			if len(g.res) > ef {
				g.res.pop()
			}
		}
	}
	for len(g.cand) > 0 {
		c := g.cand.pop()
		if len(g.res) >= ef && c.d > g.res[0].d {
			break
		}
		for _, nb := range g.nodes[c.n].links[l] {
			if g.visit[nb] == g.vgen {
				continue
			}
			g.visit[nb] = g.vgen
			d := dist(q, g.nodes[nb].vec)
			if len(g.res) < ef || d < g.res[0].d {
				g.cand.push(item{d, nb})
				if accept == nil || accept(nb) {
					g.res.push(item{d, nb})
					if len(g.res) > ef {
						g.res.pop()
					}
				}
			}
		}
	}
	out := []item(g.res)
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
	g.free = append(g.free, n)
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

// memSize counts links, node headers and scratch. Vectors of live nodes are
// shared with Index and counted there; tombstones' old vectors are not.
func (g *hnsw) memSize() int64 {
	return g.linkBytes + int64(cap(g.nodes))*56 + int64(cap(g.visit))*4 +
		int64(cap(g.fseen))*5 + int64(cap(g.free))*4 + 256
}

func sortItems(s []item) {
	slices.SortFunc(s, func(a, b item) int {
		if c := cmp.Compare(a.d, b.d); c != 0 {
			return c
		}
		return cmp.Compare(a.n, b.n)
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
