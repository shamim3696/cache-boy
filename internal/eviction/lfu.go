package eviction

import "cache-boy/internal/store"

const decayMs = 60_000

// lfuBetter prefers the key used least often. The counter decays by one per
// idle minute, so keys that were popular long ago can still be evicted.
func lfuBetter(a, b store.Candidate, now int64) bool {
	return decayed(a, now) < decayed(b, now)
}

func decayed(c store.Candidate, now int64) int64 {
	f := int64(c.Freq) - (now-c.Access)/decayMs
	if f < 0 {
		return 0
	}
	return f
}
