package queue

import (
	"testing"
	"time"
)

func TestTransitionsAreDeterministic(t *testing.T) {
	// Two managers applying the same operations at different times end up
	// identical once both have caught up with the clock.
	build := func(lag int64) *Manager {
		m := New()
		m.Lock()
		defer m.Unlock()
		m.RestoreLocked("q", Job{ID: "a", Payload: "1", Retries: 0, State: Ready, At: 100})
		m.RestoreLocked("q", Job{ID: "b", Payload: "2", Retries: 1, State: Delayed, At: 500})
		m.RestoreLocked("q", Job{ID: "c", Payload: "3", Retries: 2, State: Ready, At: 120})
		// Leader popped "a" at t=200 with visibility 100.
		m.touch("q", 200+lag)
		m.ClaimLocked("q", "a", 1, 300)
		return m
	}
	m1, m2 := build(0), build(1000)
	for _, m := range []*Manager{m1, m2} {
		m.Lock()
		m.touch("q", 2000)
		m.Unlock()
	}
	s1, s2 := m1.Snapshot()["q"], m2.Snapshot()["q"]
	if len(s1) != 3 || len(s1) != len(s2) {
		t.Fatalf("%v vs %v", s1, s2)
	}
	for i := range s1 {
		if s1[i] != s2[i] {
			t.Fatalf("job %d: %+v vs %+v", i, s1[i], s2[i])
		}
	}
	// "a" timed out with attempts 1 > retries 0: dead.
	if c := m1.Len("q"); c != (Counts{Ready: 2, Dead: 1}) {
		t.Fatalf("counts %+v", c)
	}
}

func TestPopOrderAndNack(t *testing.T) {
	m := New()
	now := time.Now().UnixMilli()
	m.Restore("q", Job{ID: "late", State: Ready, At: now, Retries: 3})
	m.Restore("q", Job{ID: "early", State: Ready, At: now - 10, Retries: 3})
	m.Lock()
	defer m.Unlock()
	j, ok := m.PopLocked("q", time.Minute, now)
	if !ok || j.ID != "early" || j.Attempts != 1 || j.State != Inflight || j.At != now+60000 {
		t.Fatalf("pop %+v", j)
	}
	n, ok := m.NackLocked("q", "early", time.Second, now)
	if !ok || n.State != Delayed || n.At != now+1000 {
		t.Fatalf("nack %+v", n)
	}
	if _, ok := m.NackLocked("q", "early", 0, now); ok {
		t.Fatal("nack of a job not in flight")
	}
	if !m.DeleteLocked("q", "early") || !m.DeleteLocked("q", "late") {
		t.Fatal("delete")
	}
	if len(m.queues) != 0 {
		t.Fatal("empty queue not dropped")
	}
}
