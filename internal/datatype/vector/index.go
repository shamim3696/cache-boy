package vector

import (
	"container/heap"
	"fmt"
	"sort"
)

type Result struct {
	ID    string
	Score float32 // cosine similarity, 1 = identical
	Attr  string
}

// Search returns the k most similar vectors to q, best first.
// filter, if non-nil, skips ids for which it returns false; it is called at
// most once per id.
//
// Below Params.Threshold vectors (or when k covers the whole index) the scan
// is exact. Otherwise the HNSW graph is built on first use and answers
// approximately; if it finds fewer than k matches passing the filter, Search
// falls back to the exact scan so a selective filter never loses results.
func (x *Index) Search(q []float32, k int, filter func(id, attr string) bool) ([]Result, error) {
	if x.dim != 0 && len(q) != x.dim {
		return nil, fmt.Errorf("ERR query dimension %d does not match index dimension %d", len(q), x.dim)
	}
	if k <= 0 {
		return []Result{}, nil
	}
	qn := normalize(q)
	if x.Len() < x.Params.Threshold || k >= x.Len() {
		return x.exact(qn, k, func(i int) bool { return filter == nil || filter(x.ids[i], x.attr[i]) }), nil
	}
	if x.g == nil {
		x.buildGraph()
	}
	g := x.g
	g.newQuery()
	accept := func(n int32) bool {
		s := g.nodes[n].slot
		if s < 0 {
			return false
		}
		if filter == nil {
			return true
		}
		if g.fseen[n] != g.fgen {
			g.fseen[n], g.fok[n] = g.fgen, filter(x.ids[s], x.attr[s])
		}
		return g.fok[n]
	}
	// Top-k recall depends on the beam per result, not the beam alone: on
	// high-dimensional data ef=64 gives ~0.75 recall@10 but ~0.97 at 250.
	ef := max(x.Params.EfSearch, efPerResult*k)
	if filter != nil {
		// Rejected nodes don't fill the result list, but a wider beam
		// still finds more survivors before the fallback is needed.
		ef = max(4*x.Params.EfSearch, efPerResult*k)
	}
	w := g.search(qn, ef, accept)
	if len(w) < k {
		return x.exact(qn, k, func(i int) bool { return accept(x.node[i]) }), nil
	}
	out := make([]Result, len(w))
	for i, it := range w {
		s := g.nodes[it.n].slot
		// Recompute rather than use 1-d so scores match the exact path bit for bit.
		out[i] = Result{x.ids[s], dot(x.vecs[s], qn), x.attr[s]}
	}
	sortResults(out)
	return out[:k], nil
}

// buildGraph inserts every live vector in slot order, which matches IDs()
// and therefore snapshot order, so reloads reproduce the same graph.
func (x *Index) buildGraph() {
	g := newHNSW(x.Params.M, x.Params.EfConstruction)
	g.nodes = make([]hnode, 0, len(x.ids))
	x.node = make([]int32, len(x.ids))
	for i, id := range x.ids {
		x.node[i] = g.insert(x.vecs[i], int32(i), levelFor(id, g.mL))
	}
	x.g = g
}

// exact scans every slot that passes accept.
func (x *Index) exact(qn []float32, k int, accept func(slot int) bool) []Result {
	h := &minHeap{}
	for i, v := range x.vecs {
		if !accept(i) {
			continue
		}
		d := dot(v, qn)
		if h.Len() < k {
			heap.Push(h, Result{x.ids[i], d, x.attr[i]})
		} else if d > (*h)[0].Score {
			(*h)[0] = Result{x.ids[i], d, x.attr[i]}
			heap.Fix(h, 0)
		}
	}
	out := []Result(*h)
	sortResults(out)
	return out
}

func sortResults(out []Result) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
}

// minHeap keeps the current top-k with the worst score on top.
type minHeap []Result

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].Score < h[j].Score }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(Result)) }
func (h *minHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
