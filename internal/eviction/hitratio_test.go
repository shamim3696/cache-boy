package eviction

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/store"
)

// simulate runs a read-through cache over the real DB and Evictor: GET, and
// SET on a miss. Keys follow a Zipf distribution over universe keys, and every
// scanEvery requests a scan of scanLen never-repeated keys goes through the
// same path, to show how well each policy resists cache pollution. The hit
// ratio counts only the Zipf requests.
func simulate(t testing.TB, policy string, universe, capacity, warmup, ops, scanEvery, scanLen int) float64 {
	cfg := config.Default()
	cfg.MaxMemoryPolicy = policy
	db := store.New(16)
	value := []byte("12345678")
	keyOf := func(prefix byte, i int) string { return fmt.Sprintf("%c%09d", prefix, i) }

	// Size maxmemory from the real per-key accounting.
	unlock := db.LockAll()
	db.Put(keyOf('x', 0), db.NewStringValue(value))
	perKey := db.UsedMemory()
	db.Flush()
	unlock()
	cfg.MaxMemory = perKey * int64(capacity)
	ev := New(db, config.NewStore(cfg))

	rng := rand.New(rand.NewPCG(1, 2))
	zipf := rand.NewZipf(rng, 1.01, 1, uint64(universe-1))
	access := func(key string) bool {
		unlock := db.Lock(key)
		hit := db.Get(key) != nil
		unlock()
		if !hit {
			if err := ev.Free(); err != nil {
				t.Fatal(err)
			}
			unlock := db.Lock(key)
			db.Put(key, db.NewStringValue(value))
			unlock()
		}
		return hit
	}
	hits, scanned := 0, 0
	for i := range warmup + ops {
		if scanLen > 0 && i%scanEvery == 0 {
			for range scanLen {
				access(keyOf('s', scanned))
				scanned++
			}
		}
		if access(keyOf('k', int(zipf.Uint64()))) && i >= warmup {
			hits++
		}
	}
	return float64(hits) / float64(ops)
}

// TestHitRatio compares the policies on a Zipf workload, with and without
// scans. The cache holds 5% of a 1M-key universe. Run with -v for the table.
func TestHitRatio(t *testing.T) {
	universe, warmup, ops := 1<<20, 300_000, 700_000
	if testing.Short() {
		universe, warmup, ops = 1<<17, 50_000, 100_000
	}
	capacity := universe / 20
	policies := []string{"allkeys-lru", "allkeys-lfu", "allkeys-tinylfu"}
	scans := []int{0, capacity / 4}
	ratios := make([][]float64, len(scans))
	t.Run("sim", func(t *testing.T) {
		for si, scanLen := range scans {
			ratios[si] = make([]float64, len(policies))
			for pi, p := range policies {
				t.Run(fmt.Sprintf("%s/scan=%d", p, scanLen), func(t *testing.T) {
					t.Parallel()
					ratios[si][pi] = simulate(t, p, universe, capacity, warmup, ops, 20_000, scanLen)
				})
			}
		}
	})
	t.Logf("Zipf s=1.01 over %d keys, cache holds %d (5%%), %d requests after %d warm-up",
		universe, capacity, ops, warmup)
	t.Logf("%-16s %10s %26s", "policy", "plain", fmt.Sprintf("+%d-key scan every 20k", capacity/4))
	for pi, p := range policies {
		t.Logf("%-16s %10.4f %26.4f", p, ratios[0][pi], ratios[1][pi])
	}
	for si := range scans {
		if ratios[si][2] < ratios[si][0] {
			t.Errorf("scan=%d: tinylfu %.4f < lru %.4f", scans[si], ratios[si][2], ratios[si][0])
		}
	}
}
