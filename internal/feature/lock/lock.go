// Package lock implements named distributed locks with fencing tokens.
//
// Every successful acquisition gets a token from one server-wide counter that
// only goes up. The counter is seeded from the clock (unix microseconds), so
// tokens keep increasing across restarts as long as the clock doesn't go
// back. Locks are leader-local: they are neither persisted nor replicated.
//
// Expiry is lazy (checked on every access) plus a timer per lock, which
// removes it and wakes waiters when it expires.
package lock

import (
	"sync"
	"time"
)

type entry struct {
	token   int64
	owner   string
	expires time.Time
	timer   *time.Timer
}

type Manager struct {
	mu      sync.Mutex
	locks   map[string]*entry
	waiters map[string]chan struct{} // closed when the lock is freed
	last    int64                    // last token handed out
}

func New() *Manager {
	return &Manager{
		locks:   map[string]*entry{},
		waiters: map[string]chan struct{}{},
		last:    time.Now().UnixMicro(),
	}
}

// Info describes a held lock.
type Info struct {
	Token int64
	Owner string
	TTL   time.Duration
}

// TryAcquire takes the lock if it is free (or expired).
func (m *Manager) TryAcquire(name string, ttl time.Duration, owner string) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	tok, ok, _, _ := m.tryLocked(name, ttl, owner, time.Now())
	return tok, ok
}

// Acquire takes the lock, waiting up to wait for it to be freed. It returns
// early (false) when cancel is closed.
func (m *Manager) Acquire(name string, ttl time.Duration, owner string, wait time.Duration, cancel ...<-chan struct{}) (int64, bool) {
	deadline := time.Now().Add(wait)
	for {
		now := time.Now()
		m.mu.Lock()
		tok, ok, expires, ch := m.tryLocked(name, ttl, owner, now)
		m.mu.Unlock()
		if ok {
			return tok, true
		}
		if !now.Before(deadline) {
			return 0, false
		}
		until := deadline
		if expires.Before(until) {
			until = expires
		}
		t := time.NewTimer(until.Sub(now))
		var c1, c2 <-chan struct{}
		if len(cancel) > 0 {
			c1 = cancel[0]
		}
		if len(cancel) > 1 {
			c2 = cancel[1]
		}
		select {
		case <-ch:
		case <-t.C:
		case <-c1:
			t.Stop()
			return 0, false
		case <-c2:
			t.Stop()
			return 0, false
		}
		t.Stop()
	}
}

// tryLocked takes the lock or returns the current holder's expiry and a
// channel closed when the lock is released.
func (m *Manager) tryLocked(name string, ttl time.Duration, owner string, now time.Time) (int64, bool, time.Time, chan struct{}) {
	if e := m.live(name, now); e != nil {
		ch := m.waiters[name]
		if ch == nil {
			ch = make(chan struct{})
			m.waiters[name] = ch
		}
		return 0, false, e.expires, ch
	}
	m.last++
	e := &entry{token: m.last, owner: owner, expires: now.Add(ttl)}
	m.locks[name] = e
	e.timer = time.AfterFunc(ttl, func() { m.expire(name, e) })
	return e.token, true, time.Time{}, nil
}

// Release frees the lock if token matches the live holder.
func (m *Manager) Release(name string, token int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.live(name, time.Now())
	if e == nil || e.token != token {
		return false
	}
	m.freeLocked(name, e)
	return true
}

// Extend sets a new ttl (from now) if token matches the live holder.
func (m *Manager) Extend(name string, token int64, ttl time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	e := m.live(name, now)
	if e == nil || e.token != token {
		return false
	}
	e.expires = now.Add(ttl)
	e.timer.Reset(ttl)
	return true
}

// Get returns the live holder of name.
func (m *Manager) Get(name string) (Info, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	e := m.live(name, now)
	if e == nil {
		return Info{}, false
	}
	return Info{Token: e.token, Owner: e.owner, TTL: e.expires.Sub(now)}, true
}

// Len returns the number of held locks.
func (m *Manager) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.locks)
}

// live returns the unexpired entry of name, removing an expired one.
func (m *Manager) live(name string, now time.Time) *entry {
	e := m.locks[name]
	if e == nil {
		return nil
	}
	if !now.Before(e.expires) {
		m.freeLocked(name, e)
		return nil
	}
	return e
}

func (m *Manager) expire(name string, e *entry) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.locks[name] != e {
		return
	}
	if now := time.Now(); now.Before(e.expires) { // extended meanwhile
		e.timer.Reset(e.expires.Sub(now))
		return
	}
	m.freeLocked(name, e)
}

func (m *Manager) freeLocked(name string, e *entry) {
	e.timer.Stop()
	delete(m.locks, name)
	if ch := m.waiters[name]; ch != nil {
		close(ch)
		delete(m.waiters, name)
	}
}
