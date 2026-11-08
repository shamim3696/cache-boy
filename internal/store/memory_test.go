package store

import (
	"os"
	"runtime"
	"strconv"
	"testing"
)

// TestMemoryFootprint measures real heap bytes per key for typical small
// values, both for the value alone and for a whole keyspace entry. It is
// slow, so it only runs with CACHEBOY_MEMREPORT=1:
//
//	CACHEBOY_MEMREPORT=1 go test ./internal/store -run MemoryFootprint -v
func TestMemoryFootprint(t *testing.T) {
	if os.Getenv("CACHEBOY_MEMREPORT") == "" {
		t.Skip("set CACHEBOY_MEMREPORT=1 to run")
	}
	workloads := []struct {
		name   string
		prefix string
		kind   Kind
		keys   int
		value  func(i int) sizer
	}{
		{"hash 5 small fields", "h:", KindHash, 200_000, func(i int) sizer {
			h := NewHash()
			for j := range 5 {
				h.Set("field"+strconv.Itoa(j), "value-"+strconv.Itoa(i+j))
			}
			return h
		}},
		{"set 10 small ints", "s:", KindSet, 200_000, func(i int) sizer {
			s := NewSet()
			for j := range 10 {
				s.Add(strconv.Itoa((i + j*97) % 1000))
			}
			return s
		}},
		{"int string", "i:", KindString, 1_000_000, func(i int) sizer {
			return NewString([]byte(strconv.Itoa(i*31)), 0)
		}},
		{"set 10 small strings", "n:", KindSet, 200_000, func(i int) sizer {
			s := NewSet()
			for j := range 10 {
				s.Add("tag:" + strconv.Itoa(j) + ":" + strconv.Itoa(i%50))
			}
			return s
		}},
	}
	for _, w := range workloads {
		base := heapAlloc()
		values := make([]sizer, w.keys)
		sliceCost := heapAlloc() - base
		for i := range values {
			values[i] = w.value(i)
		}
		valueBytes := heapAlloc() - base - sliceCost
		runtime.KeepAlive(values)
		values = nil

		base = heapAlloc()
		db := New(16)
		for i := range w.keys {
			db.Put(w.prefix+strconv.Itoa(i), NewEntry(w.kind, w.value(i)))
		}
		keyBytes := heapAlloc() - base
		t.Logf("%-21s %7d keys: value %6.1f B, whole key %6.1f B, accounted %6.1f B",
			w.name, w.keys, per(valueBytes, w.keys), per(keyBytes, w.keys), per(db.UsedMemory(), w.keys))
		runtime.KeepAlive(db)
	}
}

func per(bytes int64, n int) float64 { return float64(bytes) / float64(n) }

func heapAlloc() int64 {
	var ms runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&ms)
	return int64(ms.HeapAlloc)
}
