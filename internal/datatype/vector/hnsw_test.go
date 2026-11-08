package vector

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func randVecs(n, d int, seed uint64) [][]float32 {
	r := rand.New(rand.NewPCG(seed, 1))
	out := make([][]float32, n)
	for i := range out {
		v := make([]float32, d)
		for j := range v {
			v[j] = float32(r.NormFloat64())
		}
		out[i] = v
	}
	return out
}

// clusteredVecs draws points around a few centres, the shape real
// embeddings tend to have and the case where naive neighbour selection
// strands whole clusters.
func clusteredVecs(n, d, clusters int, seed uint64) [][]float32 {
	r := rand.New(rand.NewPCG(seed, 2))
	centres := randVecs(clusters, d, seed+100)
	out := make([][]float32, n)
	for i := range out {
		c := centres[r.IntN(clusters)]
		v := make([]float32, d)
		for j := range v {
			v[j] = c[j] + 0.25*float32(r.NormFloat64())
		}
		out[i] = v
	}
	return out
}

func build(vecs [][]float32) *Index {
	x := New()
	for i, v := range vecs {
		x.Add(fmt.Sprintf("v%d", i), v, "")
	}
	return x
}

// recall compares HNSW top-k against the exact scan over the same index.
func recall(t *testing.T, x *Index, queries [][]float32, k int) float64 {
	t.Helper()
	hit, total := 0, 0
	for _, q := range queries {
		got, err := x.Search(q, k, nil)
		if err != nil {
			t.Fatal(err)
		}
		want := x.exact(normalize(q), k, func(int) bool { return true })
		ids := map[string]bool{}
		for _, r := range got {
			ids[r.ID] = true
		}
		for _, r := range want {
			if ids[r.ID] {
				hit++
			}
		}
		total += len(want)
	}
	return float64(hit) / float64(total)
}

func TestHNSWRecall(t *testing.T) {
	if testing.Short() {
		t.Skip("builds 20k-vector graphs")
	}
	// Queries are held out from the same distribution as the data.
	for _, tc := range []struct {
		name string
		data [][]float32
	}{
		{"random", randVecs(20200, 64, 1)},
		{"clustered", clusteredVecs(20200, 64, 50, 2)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := build(tc.data[:20000])
			queries := tc.data[20000:]
			start := time.Now()
			x.Search(queries[0], 10, nil)
			t.Logf("build %v", time.Since(start))
			if x.g == nil {
				t.Fatal("graph not built over the threshold")
			}
			r := recall(t, x, queries, 10)
			t.Logf("recall@10 = %.4f", r)
			if r < 0.95 {
				t.Fatalf("recall@10 %.4f < 0.95", r)
			}
		})
	}
}

func TestHNSWDeletes(t *testing.T) {
	data := randVecs(5000, 32, 3)
	x := build(data)
	queries := randVecs(100, 32, 4)
	x.Search(queries[0], 10, nil)

	// 20% deleted, including the last slot: tombstones stay in the graph.
	queries = append(queries, data[4999])
	deleted := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("v%d", i*5+4)
		x.Remove(id)
		deleted[id] = true
	}
	if x.g == nil || x.g.dead != 1000 {
		t.Fatal("expected a graph holding 1000 tombstones")
	}
	check := func() {
		t.Helper()
		for _, q := range queries {
			res, _ := x.Search(q, 10, nil)
			if len(res) != 10 {
				t.Fatalf("got %d results", len(res))
			}
			for _, r := range res {
				if deleted[r.ID] {
					t.Fatalf("deleted id %s returned", r.ID)
				}
			}
		}
		if r := recall(t, x, queries, 10); r < 0.95 {
			t.Fatalf("recall@10 %.4f after deletes", r)
		}
	}
	check()

	// 30% deleted: past the 25% tombstone limit, so the graph is rebuilt.
	for i := 0; i < 500; i++ {
		id := fmt.Sprintf("v%d", i*5+1)
		x.Remove(id)
		deleted[id] = true
	}
	if x.Len() != 3500 {
		t.Fatalf("len %d", x.Len())
	}
	check()
	if x.g == nil || x.g.dead != 0 || len(x.g.nodes) != 3500 {
		t.Fatal("graph not rebuilt without tombstones")
	}
}

func TestHNSWReplace(t *testing.T) {
	data := randVecs(3000, 32, 5)
	x := build(data)
	x.Search(data[0], 1, nil)
	fresh := randVecs(300, 32, 6)
	for i, v := range fresh {
		if isNew, _ := x.Add(fmt.Sprintf("v%d", i), v, "new"); isNew {
			t.Fatal("replacement reported as new")
		}
	}
	if x.Len() != 3000 {
		t.Fatalf("len %d", x.Len())
	}
	for i, v := range fresh {
		id := fmt.Sprintf("v%d", i)
		res, _ := x.Search(v, 1, nil)
		if len(res) != 1 || res[0].ID != id || res[0].Attr != "new" || res[0].Score < 0.999 {
			t.Fatalf("query by new vector of %s: %v", id, res)
		}
		// The old vector must not still answer for id.
		res, _ = x.Search(data[i], 1, nil)
		if res[0].ID == id && res[0].Score > 0.999 {
			t.Fatalf("stale vector of %s still indexed", id)
		}
	}
}

func TestHNSWFilter(t *testing.T) {
	data := clusteredVecs(5000, 32, 20, 7)
	x := New()
	for i, v := range data {
		x.Add(fmt.Sprintf("v%d", i), v, fmt.Sprint(i%100))
	}
	for _, keep := range []func(int) bool{
		func(i int) bool { return i%2 == 0 },   // half pass
		func(i int) bool { return i%100 == 0 }, // 1% pass
		func(i int) bool { return i == 4242 },  // one passes
		func(int) bool { return false },        // none pass
	} {
		var n int
		filter := func(id, attr string) bool {
			n++
			var i int
			fmt.Sscanf(id, "v%d", &i)
			return keep(i)
		}
		for qi, q := range randVecs(20, 32, 8) {
			n = 0
			got, _ := x.Search(q, 10, filter)
			if n > x.Len() {
				t.Fatalf("filter called %d times for %d ids", n, x.Len())
			}
			want := x.exact(normalize(q), 10, func(i int) bool { return filter(x.ids[i], x.attr[i]) })
			hit := 0
			for _, w := range want {
				if slices.ContainsFunc(got, func(r Result) bool { return r.ID == w.ID }) {
					hit++
				}
			}
			for _, r := range got {
				var i int
				fmt.Sscanf(r.ID, "v%d", &i)
				if !keep(i) {
					t.Fatalf("filtered-out %s returned", r.ID)
				}
			}
			if len(got) != len(want) || (len(want) > 0 && float64(hit)/float64(len(want)) < 0.8) {
				t.Fatalf("query %d: got %d results, %d/%d of exact", qi, len(got), hit, len(want))
			}
		}
	}
}

func TestHNSWThreshold(t *testing.T) {
	x := New()
	data := randVecs(1200, 16, 9)
	for i := 0; i < 999; i++ {
		x.Add(fmt.Sprintf("v%d", i), data[i], "")
	}
	x.Search(data[0], 5, nil)
	if x.g != nil {
		t.Fatal("graph built below threshold")
	}
	for i := 999; i < 1200; i++ {
		x.Add(fmt.Sprintf("v%d", i), data[i], "")
	}
	if x.g != nil {
		t.Fatal("graph built before any search")
	}
	res, _ := x.Search(data[1100], 5, nil)
	if x.g == nil || len(x.g.nodes) != 1200 || res[0].ID != "v1100" {
		t.Fatalf("graph not built over threshold: %v", res)
	}
	x.Add("late", data[3], "")
	if len(x.g.nodes) != 1201 {
		t.Fatal("Add not maintained incrementally")
	}
	if res, _ := x.Search(data[3], 2, nil); len(res) != 2 || res[0].Score < 0.999 || res[1].Score < 0.999 {
		t.Fatalf("incremental insert not found: %v", res)
	}
	// A whole-index k goes exact and returns everything.
	if res, _ := x.Search(data[0], 5000, nil); len(res) != x.Len() {
		t.Fatalf("k > len: %d results", len(res))
	}
	for i := 0; i < 800; i++ {
		x.Remove(fmt.Sprintf("v%d", i))
	}
	if x.g != nil {
		t.Fatal("graph kept after shrinking below threshold")
	}
	if res, _ := x.Search(data[900], 1, nil); res[0].ID != "v900" {
		t.Fatal(res)
	}
}

func TestHNSWDeterministic(t *testing.T) {
	data := randVecs(2000, 16, 10)
	a, b := build(data), build(data)
	a.Search(data[0], 1, nil)
	b.Search(data[0], 1, nil)
	if a.g.entry != b.g.entry || a.g.top != b.g.top {
		t.Fatal("entry points differ")
	}
	for n := range a.g.nodes {
		if !slices.EqualFunc(a.g.nodes[n].links, b.g.nodes[n].links, slices.Equal) {
			t.Fatalf("node %d links differ", n)
		}
	}
	if levelFor("abc", a.g.mL) != levelFor("abc", b.g.mL) {
		t.Fatal("level not a function of id")
	}
}

// BenchmarkSearch compares exact and HNSW queries on 128-dim data. Random
// Gaussian data is the worst case for any graph index (no low-dimensional
// structure); clustered data is closer to real embeddings. The random 100k
// graph takes minutes to build.
func BenchmarkSearch(b *testing.B) {
	for _, kind := range []string{"clustered", "random"} {
		for _, n := range []int{10_000, 100_000} {
			var all [][]float32
			if kind == "random" {
				all = randVecs(n+500, 128, 11)
			} else {
				all = clusteredVecs(n+500, 128, n/1000, 11)
			}
			x, qs := build(all[:n]), all[n:]
			name := fmt.Sprintf("%s/n=%d", kind, n)
			b.Run("exact/"+name, func(b *testing.B) {
				for i := 0; b.Loop(); i++ {
					x.exact(normalize(qs[i%len(qs)]), 10, func(int) bool { return true })
				}
			})
			start := time.Now()
			x.buildGraph()
			build := time.Since(start)
			hit := 0
			for _, q := range qs[:100] {
				got, _ := x.Search(q, 10, nil)
				for _, w := range x.exact(normalize(q), 10, func(int) bool { return true }) {
					if slices.ContainsFunc(got, func(r Result) bool { return r.ID == w.ID }) {
						hit++
					}
				}
			}
			for _, k := range []int{1, 10} {
				b.Run(fmt.Sprintf("hnsw-k%d/%s", k, name), func(b *testing.B) {
					for i := 0; b.Loop(); i++ {
						x.Search(qs[i%len(qs)], k, nil)
					}
					if k == 10 {
						b.ReportMetric(float64(hit)/1000, "recall@10")
						b.ReportMetric(build.Seconds(), "build-s")
						b.ReportMetric(float64(x.MemSize())/(1<<20), "MiB")
						g := x.g
						x.g = nil
						b.ReportMetric(float64(x.MemSize())/(1<<20), "MiB-nograph")
						x.g = g
					}
				})
			}
		}
	}
}
