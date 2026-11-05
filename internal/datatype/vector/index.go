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
// filter, if non-nil, skips ids for which it returns false.
func (x *Index) Search(q []float32, k int, filter func(id, attr string) bool) ([]Result, error) {
	if x.dim != 0 && len(q) != x.dim {
		return nil, fmt.Errorf("ERR query dimension %d does not match index dimension %d", len(q), x.dim)
	}
	if k <= 0 {
		return []Result{}, nil
	}
	qn := normalize(q)
	h := &minHeap{}
	for i, v := range x.vecs {
		if filter != nil && !filter(x.ids[i], x.attr[i]) {
			continue
		}
		var dot float32
		for j := range v {
			dot += v[j] * qn[j]
		}
		if h.Len() < k {
			heap.Push(h, Result{x.ids[i], dot, x.attr[i]})
		} else if dot > (*h)[0].Score {
			(*h)[0] = Result{x.ids[i], dot, x.attr[i]}
			heap.Fix(h, 0)
		}
	}
	out := []Result(*h)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].ID < out[j].ID
	})
	return out, nil
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
