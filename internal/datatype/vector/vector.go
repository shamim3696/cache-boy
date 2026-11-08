// Package vector stores embeddings by ID and answers nearest-neighbour queries.
// Small indexes are searched exactly; past Params.Threshold vectors an HNSW
// graph answers approximately (see hnsw.go and index.go).
package vector

import (
	"errors"
	"fmt"
	"math"
)

var ErrEmpty = errors.New("ERR zero-length vector")

// Params tunes the HNSW graph. Changes apply the next time the graph is started.
type Params struct {
	M              int // links per node above layer 0; layer 0 allows 2M
	EfConstruction int // candidate list size while inserting
	EfSearch       int // minimum candidate list size while querying (x2 with a filter)
	Threshold      int // below this many vectors Search is exact
}

var DefaultParams = Params{M: 16, EfConstruction: 200, EfSearch: 64, Threshold: 1000}

// efPerResult widens the query beam with k: a k=1 lookup uses EfSearch,
// a k=10 query 250.
const efPerResult = 25

// catchUp is how many not-yet-indexed vectors each Add also indexes while
// the graph is being built, so no single Add pays for the whole build.
const catchUp = 2

type Index struct {
	Params Params

	dim  int
	ids  []string
	vecs [][]float32 // unit length, for cosine similarity
	raw  [][]float32 // as given, returned by Get
	attr []string    // optional JSON attributes per vector
	pos  map[string]int

	// The graph starts at Threshold/2 vectors and is filled in a few
	// vectors per Add, so it is complete well before Threshold. Slots not
	// yet indexed have node -1 and all lie at or after cursor.
	g       *hnsw
	node    []int32 // slot → graph node, valid while g != nil
	pending int
	cursor  int
}

func New() *Index { return &Index{Params: DefaultParams, pos: map[string]int{}} }

func (x *Index) Len() int { return len(x.ids) }
func (x *Index) Dim() int { return x.dim }

func (x *Index) MemSize() int64 {
	n := int64(len(x.ids))*(int64(x.dim)*8+64) + 64
	if x.g != nil {
		n += x.g.memSize() + int64(cap(x.node))*4
	}
	return n
}

// Add inserts or replaces a vector. The first vector fixes the dimension.
// Returns true if id is new.
func (x *Index) Add(id string, v []float32, attr string) (bool, error) {
	if len(v) == 0 {
		return false, ErrEmpty
	}
	if x.dim == 0 {
		x.dim = len(v)
	} else if len(v) != x.dim {
		return false, fmt.Errorf("ERR vector dimension %d does not match index dimension %d", len(v), x.dim)
	}
	norm := normalize(v)
	if i, ok := x.pos[id]; ok {
		x.vecs[i], x.raw[i], x.attr[i] = norm, v, attr
		if x.g != nil && x.node[i] >= 0 {
			x.g.kill(x.node[i])
			x.index(i)
		}
		return false, nil
	}
	x.pos[id] = len(x.ids)
	x.ids = append(x.ids, id)
	x.vecs = append(x.vecs, norm)
	x.raw = append(x.raw, v)
	x.attr = append(x.attr, attr)
	switch {
	case x.g != nil:
		x.node = append(x.node, -1)
		x.index(len(x.ids) - 1)
	case x.Len() >= x.Params.Threshold/2:
		x.g = newHNSW(x.Params.M, x.Params.EfConstruction)
		x.node = make([]int32, len(x.ids))
		for i := range x.node {
			x.node[i] = -1
		}
		x.pending, x.cursor = len(x.ids), 0
	}
	for k := 0; k < catchUp && x.g != nil && x.pending > 0; k++ {
		for x.node[x.cursor] >= 0 {
			x.cursor++
		}
		x.pending--
		x.index(x.cursor)
	}
	return true, nil
}

// index inserts slot i's vector into the graph.
func (x *Index) index(i int) {
	x.node[i] = x.g.insert(x.vecs[i], int32(i), levelFor(x.ids[i], x.g.mL))
}

func (x *Index) Remove(id string) bool {
	i, ok := x.pos[id]
	if !ok {
		return false
	}
	last := len(x.ids) - 1
	x.ids[i], x.vecs[i], x.raw[i], x.attr[i] = x.ids[last], x.vecs[last], x.raw[last], x.attr[last]
	x.pos[x.ids[i]] = i
	x.ids, x.vecs, x.raw, x.attr = x.ids[:last], x.vecs[:last], x.raw[:last], x.attr[:last]
	delete(x.pos, id)
	if x.g == nil {
		return true
	}
	if n := x.node[i]; n >= 0 {
		x.g.kill(n)
	} else {
		x.pending--
	}
	if i != last {
		x.node[i] = x.node[last]
		if x.node[i] >= 0 {
			x.g.nodes[x.node[i]].slot = int32(i)
		} else if i < x.cursor {
			// Keep every unindexed slot at or after the cursor.
			x.pending--
			x.index(i)
		}
	}
	x.node = x.node[:last]
	if x.Len() < x.Params.Threshold/4 {
		// Small enough to scan, and well below where the graph starts, so
		// a workload hovering near the line doesn't rebuild repeatedly.
		x.g, x.node, x.pending, x.cursor = nil, nil, 0, 0
	}
	return true
}

// Get returns the stored vector and attributes.
func (x *Index) Get(id string) ([]float32, string, bool) {
	i, ok := x.pos[id]
	if !ok {
		return nil, "", false
	}
	return x.raw[i], x.attr[i], true
}

// IDs returns all ids (order unspecified).
func (x *Index) IDs() []string { return append([]string(nil), x.ids...) }

func normalize(v []float32) []float32 {
	var sum float64
	for _, f := range v {
		sum += float64(f) * float64(f)
	}
	n := float32(math.Sqrt(sum))
	out := make([]float32, len(v))
	if n == 0 {
		return out
	}
	for i, f := range v {
		out[i] = f / n
	}
	return out
}
