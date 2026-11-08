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
			start := time.Now()
			x := build(tc.data[:20000])
			t.Logf("build %v", time.Since(start))
			if x.g == nil {
				t.Fatal("graph not built over the threshold")
			}
			r := recall(t, x, tc.data[20000:], 10)
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
	queries := append(randVecs(100, 32, 4), data[4999])
	deleted := map[string]bool{}
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

	// 30% deleted, including the last slot: all stay as tombstones.
	for i := 0; i < 1500; i++ {
		id := fmt.Sprintf("v%d", 4999-i*10/3)
		if !x.Remove(id) {
			t.Fatalf("remove %s", id)
		}
		deleted[id] = true
	}
	if x.Len() != 3500 || len(x.g.free) != 1500 || len(x.g.nodes) != 5000 {
		t.Fatalf("len %d, %d tombstones of %d nodes", x.Len(), len(x.g.free), len(x.g.nodes))
	}
	check()

	// New ids take over the tombstones instead of growing the graph.
	for i, v := range randVecs(1500, 32, 13) {
		x.Add(fmt.Sprintf("n%d", i), v, "")
	}
	// Only a tombstoned entry point is never reused.
	if len(x.g.nodes) > 5001 || len(x.g.free) > 1 {
		t.Fatalf("%d nodes, %d tombstones after refill", len(x.g.nodes), len(x.g.free))
	}
	check()
	for i, v := range randVecs(1500, 32, 13)[:50] {
		if res, _ := x.Search(v, 1, nil); res[0].ID != fmt.Sprintf("n%d", i) {
			t.Fatalf("reused node n%d not found: %v", i, res)
		}
	}
}

func TestHNSWReplace(t *testing.T) {
	data := randVecs(3000, 32, 5)
	x := build(data)
	fresh := randVecs(300, 32, 6)
	for i, v := range fresh {
		if isNew, _ := x.Add(fmt.Sprintf("v%d", i), v, "new"); isNew {
			t.Fatal("replacement reported as new")
		}
	}
	if x.Len() != 3000 || len(x.g.nodes) > 3001 {
		t.Fatalf("len %d, %d nodes", x.Len(), len(x.g.nodes))
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
	if r := recall(t, x, randVecs(100, 32, 7), 10); r < 0.95 {
		t.Fatalf("recall@10 %.4f after replacements", r)
	}
}

// TestHNSWChurn mimics semcache under LRU pressure: 50k adds into a cache
// capped at 10k, so 40k deletes, with a filtered k=1 lookup before each
// add. No operation may stall: there are no full rebuilds.
func TestHNSWChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("50k inserts")
	}
	const adds, capacity = 50_000, 10_000
	data := clusteredVecs(adds+200, 128, 200, 14)
	x := New()
	var addT, delT, searchT []time.Duration
	nodes := 0
	for i := 0; i < adds; i++ {
		start := time.Now()
		x.Search(data[i], 1, func(id, _ string) bool { return id[1] != '7' })
		searchT = append(searchT, time.Since(start))

		start = time.Now()
		x.Add(fmt.Sprintf("e%d", i), data[i], "")
		addT = append(addT, time.Since(start))

		if x.Len() > capacity {
			start = time.Now()
			x.Remove(fmt.Sprintf("e%d", i-capacity))
			delT = append(delT, time.Since(start))
		}
		if x.g != nil {
			nodes = max(nodes, len(x.g.nodes))
		}
	}
	for _, s := range []struct {
		name string
		d    []time.Duration
	}{{"add", addT}, {"remove", delT}, {"search", searchT}} {
		var sum time.Duration
		for _, d := range s.d {
			sum += d
		}
		slices.Sort(s.d)
		// Windows clocks tick in ~0.5ms steps, so the mean is the sharper figure.
		t.Logf("%s: n=%d mean=%v p99=%v p99.9=%v max=%v", s.name, len(s.d), sum/time.Duration(len(s.d)),
			s.d[len(s.d)*99/100], s.d[len(s.d)*999/1000], s.d[len(s.d)-1])
	}
	if len(delT) != adds-capacity {
		t.Fatalf("%d deletes", len(delT))
	}
	if p99 := addT[len(addT)*99/100]; p99 > 5*time.Millisecond {
		t.Errorf("add p99 %v", p99)
	}
	// Not the max: on a shared machine a preempted thread shows up as a
	// multi-millisecond outlier in searches and adds alike.
	if p999 := addT[len(addT)*999/1000]; p999 > 10*time.Millisecond {
		t.Errorf("add p99.9 %v", p999)
	}
	// Tombstones are reused, so the graph never outgrows the live set by more
	// than the entry point and the slot freed just before each add.
	if nodes > capacity+2 {
		t.Fatalf("graph grew to %d nodes for %d live", nodes, capacity)
	}
	if r := recall(t, x, data[adds:], 10); r < 0.95 {
		t.Fatalf("recall@10 %.4f after churn", r)
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
	add := func(from, to int) {
		for i := from; i < to; i++ {
			x.Add(fmt.Sprintf("v%d", i), data[i], "")
		}
	}
	add(0, 499)
	if x.g != nil {
		t.Fatal("graph started below half the threshold")
	}
	add(499, 500)
	if x.g == nil || x.pending != 500-catchUp {
		t.Fatal("graph not started at half the threshold")
	}
	// Each Add indexes itself plus catchUp backlog vectors.
	add(500, 760)
	if x.pending != 0 || len(x.g.nodes) != 760 {
		t.Fatalf("backlog %d, %d nodes", x.pending, len(x.g.nodes))
	}
	add(760, 1200)
	if len(x.g.nodes) != 1200 {
		t.Fatal("Add not maintained incrementally")
	}
	if res, _ := x.Search(data[1100], 5, nil); res[0].ID != "v1100" {
		t.Fatalf("incremental insert not found: %v", res)
	}
	x.Add("late", data[3], "")
	if res, _ := x.Search(data[3], 2, nil); len(res) != 2 || res[0].Score < 0.999 || res[1].Score < 0.999 {
		t.Fatalf("duplicate vector not found: %v", res)
	}
	// A whole-index k goes exact and returns everything.
	if res, _ := x.Search(data[0], 5000, nil); len(res) != x.Len() {
		t.Fatalf("k > len: %d results", len(res))
	}
	for i := 0; i < 900; i++ {
		x.Remove(fmt.Sprintf("v%d", i))
	}
	if x.g == nil {
		t.Fatal("graph dropped above a quarter of the threshold")
	}
	for i := 900; i < 960; i++ {
		x.Remove(fmt.Sprintf("v%d", i))
	}
	if x.g != nil {
		t.Fatal("graph kept below a quarter of the threshold")
	}
	if res, _ := x.Search(data[1000], 1, nil); res[0].ID != "v1000" {
		t.Fatal(res)
	}
}

// TestHNSWBacklogRemove removes vectors while the graph is still being
// filled, including ones the cursor has passed, and checks every slot ends
// up indexed exactly once.
func TestHNSWBacklogRemove(t *testing.T) {
	x := New()
	data := randVecs(1500, 16, 15)
	for i := 0; i < 600; i++ {
		x.Add(fmt.Sprintf("v%d", i), data[i], "")
	}
	for i := 0; i < 600; i += 7 {
		x.Remove(fmt.Sprintf("v%d", i))
	}
	for i := 600; i < 1500; i++ {
		x.Add(fmt.Sprintf("v%d", i), data[i], "")
		if i%5 == 0 {
			x.Remove(fmt.Sprintf("v%d", i-3))
		}
	}
	if x.pending != 0 {
		t.Fatalf("backlog %d", x.pending)
	}
	seen := map[int32]bool{}
	for i, n := range x.node {
		if n < 0 || seen[n] || int(x.g.nodes[n].slot) != i {
			t.Fatalf("slot %d maps to node %d", i, n)
		}
		seen[n] = true
	}
	if live := len(x.g.nodes) - len(x.g.free); live != x.Len() {
		t.Fatalf("%d live nodes for %d vectors", live, x.Len())
	}
}

func sameGraph(t *testing.T, a, b *hnsw) {
	t.Helper()
	if a.entry != b.entry || a.top != b.top || len(a.nodes) != len(b.nodes) || !slices.Equal(a.free, b.free) {
		t.Fatal("graph shape differs")
	}
	for n := range a.nodes {
		if !slices.EqualFunc(a.nodes[n].links, b.nodes[n].links, slices.Equal) {
			t.Fatalf("node %d links differ", n)
		}
	}
}

func TestHNSWDeterministic(t *testing.T) {
	data := randVecs(3000, 16, 10)
	ops := func() *Index {
		x := build(data[:2000])
		for i := 0; i < 500; i++ {
			x.Remove(fmt.Sprintf("v%d", i*3))
		}
		for i := 2000; i < 3000; i++ {
			x.Add(fmt.Sprintf("v%d", i), data[i], "")
		}
		return x
	}
	sameGraph(t, ops().g, ops().g)
	if levelFor("abc", 0.36) != levelFor("abc", 0.36) {
		t.Fatal("level not a function of id")
	}
}

// BenchmarkSearch compares exact and HNSW queries on 128-dim data. Random
// Gaussian data is the worst case for any graph index (no low-dimensional
// structure); clustered data is closer to real embeddings. build-s is the
// time for all n Adds, graph maintenance included.
func BenchmarkSearch(b *testing.B) {
	for _, kind := range []string{"clustered", "random"} {
		for _, n := range []int{10_000, 100_000} {
			var all [][]float32
			if kind == "random" {
				all = randVecs(n+500, 128, 11)
			} else {
				all = clusteredVecs(n+500, 128, n/1000, 11)
			}
			start := time.Now()
			x, qs := build(all[:n]), all[n:]
			build := time.Since(start) // every Add, graph maintenance included
			name := fmt.Sprintf("%s/n=%d", kind, n)
			b.Run("exact/"+name, func(b *testing.B) {
				for i := 0; b.Loop(); i++ {
					x.exact(normalize(qs[i%len(qs)]), 10, func(int) bool { return true })
				}
			})
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
