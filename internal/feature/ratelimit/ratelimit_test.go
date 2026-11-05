package ratelimit

import (
	"testing"
	"time"
)

func TestGCRA(t *testing.T) {
	l := New()
	now := int64(1_000_000_000_000)
	period := 10 * time.Second // limit 10: one cell per second
	for i := 0; i < 10; i++ {
		r := l.take("k", 10, period, 1, false, now)
		if !r.Allowed || r.Remaining != int64(9-i) {
			t.Fatalf("request %d: %+v", i, r)
		}
	}
	r := l.take("k", 10, period, 1, false, now)
	if r.Allowed || r.RetryAfter != time.Second || r.Remaining != 0 || r.ResetAfter != period {
		t.Fatalf("over limit: %+v", r)
	}
	// One emission interval later, exactly one more request fits.
	now += int64(time.Second)
	if r := l.take("k", 10, period, 1, false, now); !r.Allowed || r.Remaining != 0 {
		t.Fatalf("after 1s: %+v", r)
	}
	if r := l.take("k", 10, period, 1, true, now); r.Allowed {
		t.Fatalf("peek: %+v", r)
	}
	// After a full period the burst is back.
	now += int64(period)
	if r := l.take("k", 10, period, 1, true, now); !r.Allowed || r.Remaining != 10 || r.ResetAfter != 0 {
		t.Fatalf("peek after period: %+v", r)
	}
	if !l.reset("k", now-int64(period)) || l.reset("k", now) {
		t.Fatal("reset")
	}
	// A cost above the limit is never allowed.
	if r := l.take("big", 10, period, 11, false, now); r.Allowed {
		t.Fatalf("cost > limit: %+v", r)
	}
}

func TestSweep(t *testing.T) {
	l := New()
	now := int64(1_000_000_000_000)
	for i := 0; i < 5000; i++ {
		l.take(string(rune(i)), 1, time.Millisecond, 1, false, now)
	}
	// Much later every entry is idle; enough operations sweep them away.
	later := now + int64(time.Hour)
	for i := 0; i < numShards*sweepEvery; i++ {
		l.take("x", 1000000, time.Hour, 0, false, later)
	}
	if n := l.Len(); n > 1 {
		t.Fatalf("%d entries left after sweep", n)
	}
}
