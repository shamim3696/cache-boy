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

// Params tunes the HNSW graph. Changes apply the next time the graph is built.
type Params struct {
	M              int // links per node above layer 0; layer 0 allows 2M
	EfConstruction int // candidate list size while inserting
	EfSearch       int // minimum candidate list size while querying (x4 with a filter)
	Threshold      int // below this many vectors Search is exact and no graph is kept
}

var DefaultParams = Params{M: 16, EfConstruction: 200, EfSearch: 64, Threshold: 1000}

// efPerResult widens the query beam with k: a k=1 lookup uses EfSearch,
// a k=10 query 250.
const efPerResult = 25

type Index struct {
	Params Params

	dim  int
	ids  []string
	vecs [][]float32 // unit length, for cosine similarity
	raw  [][]float32 // as given, returned by Get
	attr []string    // optional JSON attributes per vector
	pos  map[string]int

	g    *hnsw   // nil until a Search needs it
	node []int32 // slot → graph node, valid while g != nil
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
		if x.g != nil {
			x.g.kill(x.node[i])
			x.node[i] = x.g.insert(norm, int32(i), levelFor(id, x.g.mL))
			x.pruneGraph()
		}
		return false, nil
	}
	x.pos[id] = len(x.ids)
	if x.g != nil {
		x.node = append(x.node, x.g.insert(norm, int32(len(x.ids)), levelFor(id, x.g.mL)))
	}
	x.ids = append(x.ids, id)
	x.vecs = append(x.vecs, norm)
	x.raw = append(x.raw, v)
	x.attr = append(x.attr, attr)
	return true, nil
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
	if x.g != nil {
		n := x.node[i]
		x.node[i] = x.node[last]
		x.g.nodes[x.node[i]].slot = int32(i)
		x.node = x.node[:last]
		x.g.kill(n) // after the move, in case i was the last slot
		x.pruneGraph()
	}
	return true
}

// pruneGraph drops the graph once tombstones pass 25% of its nodes, or once
// the index has shrunk well below the threshold (half, so it doesn't flap).
// The next Search over the threshold rebuilds it from the live vectors.
func (x *Index) pruneGraph() {
	if x.g.dead*4 > len(x.g.nodes) || x.Len() < x.Params.Threshold/2 {
		x.g, x.node = nil, nil
	}
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
