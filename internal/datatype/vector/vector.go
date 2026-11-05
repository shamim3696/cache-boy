// Package vector stores embeddings by ID and answers nearest-neighbour queries.
// Search is exact (brute force) over normalised vectors, which is fine up to
// ~100k vectors per key. See index.go.
package vector

import (
	"errors"
	"fmt"
	"math"
)

var ErrEmpty = errors.New("ERR zero-length vector")

type Index struct {
	dim  int
	ids  []string
	vecs [][]float32 // unit length, for cosine similarity
	raw  [][]float32 // as given, returned by Get
	attr []string    // optional JSON attributes per vector
	pos  map[string]int
}

func New() *Index { return &Index{pos: map[string]int{}} }

func (x *Index) Len() int { return len(x.ids) }
func (x *Index) Dim() int { return x.dim }

func (x *Index) MemSize() int64 {
	return int64(len(x.ids))*(int64(x.dim)*8+64) + 64
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
		return false, nil
	}
	x.pos[id] = len(x.ids)
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
