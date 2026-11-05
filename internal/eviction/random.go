package eviction

import "cache-boy/internal/store"

// randomBetter keeps the first sampled key: samples are already random.
func randomBetter(_, _ store.Candidate, _ int64) bool { return false }
