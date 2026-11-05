package pubsub

import "cache-boy/internal/glob"

// matchPattern reports whether a channel matches a PSUBSCRIBE pattern.
func matchPattern(pattern, channel string) bool { return glob.Match(pattern, channel) }
