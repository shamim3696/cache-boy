package store

// wheel is a hashed timing wheel of keys with a TTL, bucketed by expiry time.
// Active expiry visits only buckets whose time has passed, so it finds every
// expired key without scanning keys that are still alive.
//
// Updates are O(1) appends: when a key's TTL changes, or the key is deleted
// or replaced, its old wheel entry is left behind and skipped when its bucket
// comes up (the key's current Entry is the source of truth). To keep those
// stale entries from piling up, for example with a sliding session TTL that
// is pushed forward on every request, the wheel is rebuilt from the volatile
// set whenever it holds more than 2×len(volatile)+wheelSlack entries. A
// rebuild costs O(volatile) and happens at most once per that many adds.
type wheel struct {
	buckets map[int64][]string
	cursor  int64 // every bucket below cursor is empty
	n       int   // entries in buckets, live or stale
}

const (
	wheelTickMs = 100
	wheelSlack  = 1024
)

func newWheel(now int64) wheel {
	return wheel{buckets: map[int64][]string{}, cursor: now / wheelTickMs}
}

// tick is the bucket of an expiry time. Times already in the past go to the
// previous tick so they are due immediately but don't drag the cursor back.
func (w *wheel) tick(expireAt, now int64) int64 {
	return max(expireAt/wheelTickMs, now/wheelTickMs-1)
}

func (w *wheel) add(key string, expireAt, now int64) {
	t := w.tick(expireAt, now)
	w.buckets[t] = append(w.buckets[t], key)
	w.n++
	if t < w.cursor {
		w.cursor = t
	}
}

// due returns the oldest bucket whose time has fully passed, or false.
func (w *wheel) due(now int64) (int64, bool) {
	end := now / wheelTickMs // buckets < end are fully in the past
	if end-w.cursor > int64(len(w.buckets))+64 {
		// After a long pause, jumping to the smallest bucket is cheaper than
		// walking every empty tick.
		next := end
		for t := range w.buckets {
			next = min(next, t)
		}
		w.cursor = next
	}
	for ; w.cursor < end; w.cursor++ {
		if len(w.buckets[w.cursor]) > 0 {
			return w.cursor, true
		}
		delete(w.buckets, w.cursor)
	}
	return 0, false
}

// expireDue deletes up to budget expired keys of s, checking wheel entries in
// due buckets and skipping stale ones. It returns the number of keys deleted
// and whether due entries are left for the next call.
func (db *DB) expireDue(s *shard, budget int, now int64) (expired int, more bool) {
	w := &s.wheel
	for budget > 0 {
		t, ok := w.due(now)
		if !ok {
			return expired, false
		}
		keys := w.buckets[t]
		for budget > 0 && len(keys) > 0 {
			k := keys[len(keys)-1]
			keys[len(keys)-1] = "" // let the GC have the string
			keys = keys[:len(keys)-1]
			w.n--
			budget--
			if e := s.data[k]; e != nil && e.ExpireAt > 0 && e.ExpireAt <= now {
				db.deleteIn(s, k)
				db.Expired.Add(1)
				db.Emit(Event{Key: k, Op: "expired"})
				expired++
			}
		}
		if len(keys) == 0 {
			delete(w.buckets, t)
		} else {
			w.buckets[t] = keys
		}
	}
	_, more = w.due(now)
	return expired, more
}

// track records a key's new expiry (0 = none) in the volatile set and the
// wheel. oldAt is the expiry of the entry the key held until now, or 0. A
// stored key's wheel entry is never consumed while the key lives, so if the
// old expiry falls in the same bucket the existing wheel entry still covers
// the key. Requires the shard lock.
func (s *shard) track(key string, oldAt, newAt int64) {
	if newAt <= 0 {
		delete(s.volatile, key)
		return
	}
	s.volatile[key] = struct{}{}
	w, now := &s.wheel, nowMs()
	if oldAt > 0 && w.tick(oldAt, now) == w.tick(newAt, now) {
		return
	}
	w.add(key, newAt, now)
	if w.n > 2*len(s.volatile)+wheelSlack {
		s.rebuildWheel(now)
	}
}

func (s *shard) rebuildWheel(now int64) {
	s.wheel = newWheel(now)
	for k := range s.volatile {
		s.wheel.add(k, s.data[k].ExpireAt, now)
	}
}
