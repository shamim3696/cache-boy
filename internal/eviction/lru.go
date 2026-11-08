package eviction

import (
	"time"

	"github.com/shamim3696/cache-boy/internal/store"
)

// lruBetter prefers the key idle the longest.
func lruBetter(a, b store.Candidate, _ int64) bool { return a.Access < b.Access }

func nowMs() int64 { return time.Now().UnixMilli() }
