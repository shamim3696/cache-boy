package eviction

import "github.com/shamim3696/cache-boy/internal/store"

// tinyLFUBetter prefers the key with the lowest recent access frequency, as
// estimated by the shard's TinyLFU sketch, and among equals the one idle the
// longest. Unlike the per-key LFU counter, the sketch also remembers keys that
// were evicted and come back, and ignores keys seen only once, so a scan of
// one-off keys can't push out the frequently used ones.
//
// There is no separate admission step: eviction runs before a write command,
// so a new key is never compared at insertion time. A new one-off key starts
// with an estimate of at most 1 and is evicted as soon as it is sampled, which
// gives most of the benefit of TinyLFU admission without changing SET.
func tinyLFUBetter(a, b store.Candidate, _ int64) bool {
	if a.Sketch != b.Sketch {
		return a.Sketch < b.Sketch
	}
	return a.Access < b.Access
}
