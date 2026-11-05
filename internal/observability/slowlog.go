package observability

import (
	"sync"
	"time"
)

// SlowEntry is one SLOWLOG record.
type SlowEntry struct {
	ID         int64
	Time       time.Time
	Duration   time.Duration
	Args       []string
	ClientAddr string
	ClientName string
}

// Slowlog keeps the most recent commands slower than a threshold.
type Slowlog struct {
	mu      sync.Mutex
	entries []SlowEntry // newest last
	nextID  int64
}

func NewSlowlog() *Slowlog { return &Slowlog{} }

// Record adds an entry if dur exceeds thresholdMicros (<0 disables, 0 logs everything).
func (s *Slowlog) Record(thresholdMicros int64, maxLen int, dur time.Duration, args []string, addr, name string) {
	if thresholdMicros < 0 || dur.Microseconds() < thresholdMicros {
		return
	}
	if len(args) > 32 { // keep entries small, like Redis
		args = append(append([]string{}, args[:31]...), "... (more arguments)")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, SlowEntry{s.nextID, time.Now(), dur, append([]string(nil), args...), addr, name})
	s.nextID++
	if maxLen >= 0 && len(s.entries) > maxLen {
		s.entries = s.entries[len(s.entries)-maxLen:]
	}
}

// Get returns up to n entries, newest first (n < 0 = all).
func (s *Slowlog) Get(n int) []SlowEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 0 || n > len(s.entries) {
		n = len(s.entries)
	}
	out := make([]SlowEntry, 0, n)
	for i := len(s.entries) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, s.entries[i])
	}
	return out
}

func (s *Slowlog) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}

func (s *Slowlog) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = nil
}
