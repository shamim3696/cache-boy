// Package queue implements job queues with delayed jobs, a visibility
// timeout, retries and a dead-letter list.
//
// Every job is in exactly one state:
//
//	ready    waiting to be popped, ordered by (at, seq) where at is when it became ready
//	delayed  not ready before at
//	inflight popped, must be acked before at (the visibility deadline)
//	dead     ran out of retries (at = when it died)
//
// Time-driven transitions are pure functions of a job's own fields and the
// clock, and are applied lazily whenever a queue is touched:
//
//	delayed  → ready              at at
//	inflight → ready (or dead when attempts > retries) at the deadline
//
// Replication works by sending "set this job to this state" operations
// (Restore, Claim, Ack, Purge) with absolute timestamps, so a replica or an
// AOF replay converges to the same contents whenever it applies them. The
// command layer turns each mutation into such operations.
package queue

import (
	"container/heap"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type State uint8

const (
	Ready State = iota + 1
	Delayed
	Inflight
	Dead
)

func (s State) String() string {
	switch s {
	case Ready:
		return "ready"
	case Delayed:
		return "delayed"
	case Inflight:
		return "inflight"
	case Dead:
		return "dead"
	}
	return "unknown"
}

// ParseState is the inverse of State.String.
func ParseState(s string) (State, bool) {
	switch s {
	case "ready":
		return Ready, true
	case "delayed":
		return Delayed, true
	case "inflight":
		return Inflight, true
	case "dead":
		return Dead, true
	}
	return 0, false
}

// Job is a snapshot of a job (returned by value; never shared).
type Job struct {
	ID       string
	Payload  string
	Retries  int64
	Attempts int64
	State    State
	At       int64 // unix ms, meaning depends on State
}

type job struct {
	Job
	seq int64
	idx int // position in its heap
}

// jobHeap is a min-heap by (At, seq) that supports removal.
type jobHeap []*job

func (h jobHeap) Len() int { return len(h) }
func (h jobHeap) Less(i, j int) bool {
	if h[i].At != h[j].At {
		return h[i].At < h[j].At
	}
	return h[i].seq < h[j].seq
}
func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idx = i
	h[j].idx = j
}
func (h *jobHeap) Push(x any) {
	j := x.(*job)
	j.idx = len(*h)
	*h = append(*h, j)
}
func (h *jobHeap) Pop() any {
	old := *h
	n := len(old)
	j := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	j.idx = -1
	return j
}

type queue struct {
	jobs     map[string]*job
	ready    jobHeap
	delayed  jobHeap
	inflight jobHeap
	dead     jobHeap
}

func newQueue() *queue { return &queue{jobs: map[string]*job{}} }

func (q *queue) heapOf(s State) *jobHeap {
	switch s {
	case Ready:
		return &q.ready
	case Delayed:
		return &q.delayed
	case Inflight:
		return &q.inflight
	default:
		return &q.dead
	}
}

// Manager holds every queue. All methods are safe for concurrent use.
type Manager struct {
	mu      sync.Mutex
	queues  map[string]*queue
	waiters map[string]chan struct{} // closed when a job may have become ready
	seq     int64
	ids     atomic.Int64
}

func New() *Manager {
	return &Manager{queues: map[string]*queue{}, waiters: map[string]chan struct{}{}}
}

// Lock/Unlock let the command layer run a mutation and propagate it
// atomically, so AOF/replicas see operations in execution order. Methods
// ending in "Locked" require it.
func (m *Manager) Lock()   { m.mu.Lock() }
func (m *Manager) Unlock() { m.mu.Unlock() }

// NewID returns a unique job id.
func (m *Manager) NewID() string {
	return strconv.FormatInt(time.Now().UnixMilli(), 10) + "-" + strconv.FormatInt(m.ids.Add(1), 10)
}

func (m *Manager) get(name string, create bool) *queue {
	q := m.queues[name]
	if q == nil && create {
		q = newQueue()
		m.queues[name] = q
	}
	return q
}

func (m *Manager) dropIfEmpty(name string, q *queue) {
	if len(q.jobs) == 0 {
		delete(m.queues, name)
	}
}

func (m *Manager) wake(name string) {
	if ch := m.waiters[name]; ch != nil {
		close(ch)
		delete(m.waiters, name)
	}
}

func (m *Manager) insert(name string, q *queue, j *job) {
	m.seq++
	j.seq = m.seq
	q.jobs[j.ID] = j
	heap.Push(q.heapOf(j.State), j)
	if j.State == Ready {
		m.wake(name)
	}
}

func (q *queue) remove(j *job) {
	heap.Remove(q.heapOf(j.State), j.idx)
	delete(q.jobs, j.ID)
}

// promote applies time-driven transitions due at now.
func (m *Manager) promote(name string, q *queue, now int64) {
	woke := false
	for q.delayed.Len() > 0 && q.delayed[0].At <= now {
		j := heap.Pop(&q.delayed).(*job)
		j.State = Ready
		heap.Push(&q.ready, j)
		woke = true
	}
	for q.inflight.Len() > 0 && q.inflight[0].At <= now {
		j := heap.Pop(&q.inflight).(*job)
		if j.Attempts > j.Retries {
			j.State = Dead
			heap.Push(&q.dead, j)
		} else {
			j.State = Ready
			heap.Push(&q.ready, j)
			woke = true
		}
	}
	if woke {
		m.wake(name)
	}
}

// touch returns the queue after applying due transitions (nil if missing).
func (m *Manager) touch(name string, now int64) *queue {
	q := m.queues[name]
	if q != nil {
		m.promote(name, q, now)
	}
	return q
}

// ExistsLocked reports whether a job with id is in the queue (any state).
func (m *Manager) ExistsLocked(name, id string) bool {
	q := m.queues[name]
	return q != nil && q.jobs[id] != nil
}

// RestoreLocked creates or replaces a job with exactly the given fields.
// This is the replication primitive behind QPUSH, QNACK, QRETRYDEAD and Dump.
func (m *Manager) RestoreLocked(name string, j Job) {
	q := m.get(name, true)
	if old := q.jobs[j.ID]; old != nil {
		q.remove(old)
	}
	m.insert(name, q, &job{Job: j})
}

// Restore is RestoreLocked with locking.
func (m *Manager) Restore(name string, j Job) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.RestoreLocked(name, j)
}

// PopLocked claims the oldest ready job for visibility ms. It returns the
// job as it is after the claim (Inflight, Attempts incremented).
func (m *Manager) PopLocked(name string, visibility time.Duration, now int64) (Job, bool) {
	q := m.touch(name, now)
	if q == nil || q.ready.Len() == 0 {
		return Job{}, false
	}
	j := heap.Pop(&q.ready).(*job)
	j.State = Inflight
	j.Attempts++
	j.At = now + visibility.Milliseconds()
	heap.Push(&q.inflight, j)
	return j.Job, true
}

// ClaimLocked puts job id in flight with the given attempts and deadline,
// whatever its current state (replay of a QPOP). Returns false if unknown.
func (m *Manager) ClaimLocked(name, id string, attempts, deadline int64) bool {
	q := m.queues[name]
	if q == nil {
		return false
	}
	j := q.jobs[id]
	if j == nil {
		return false
	}
	heap.Remove(q.heapOf(j.State), j.idx)
	j.State, j.Attempts, j.At = Inflight, attempts, deadline
	heap.Push(&q.inflight, j)
	return true
}

// AckLocked deletes an in-flight job. Returns false if it isn't in flight.
func (m *Manager) AckLocked(name, id string, now int64) bool {
	q := m.touch(name, now)
	if q == nil {
		return false
	}
	j := q.jobs[id]
	if j == nil || j.State != Inflight {
		return false
	}
	q.remove(j)
	m.dropIfEmpty(name, q)
	return true
}

// DeleteLocked deletes a job in any state (replay of QACK).
func (m *Manager) DeleteLocked(name, id string) bool {
	q := m.queues[name]
	if q == nil || q.jobs[id] == nil {
		return false
	}
	q.remove(q.jobs[id])
	m.dropIfEmpty(name, q)
	return true
}

// NackLocked returns an in-flight job to the queue: ready (or delayed by
// delay), or dead when its attempts exceed its retries. It returns the job's
// new state, or false if the job isn't in flight.
func (m *Manager) NackLocked(name, id string, delay time.Duration, now int64) (Job, bool) {
	q := m.touch(name, now)
	if q == nil {
		return Job{}, false
	}
	j := q.jobs[id]
	if j == nil || j.State != Inflight {
		return Job{}, false
	}
	next := j.Job
	switch {
	case j.Attempts > j.Retries:
		next.State, next.At = Dead, now
	case delay > 0:
		next.State, next.At = Delayed, now+delay.Milliseconds()
	default:
		next.State, next.At = Ready, now
	}
	m.RestoreLocked(name, next)
	return next, true
}

// RetryDeadLocked moves every dead job back to ready with attempts reset.
// Returns the jobs as restored, oldest first.
func (m *Manager) RetryDeadLocked(name string, now int64) []Job {
	q := m.touch(name, now)
	if q == nil || q.dead.Len() == 0 {
		return nil
	}
	dead := sortedJobs(q.dead)
	out := make([]Job, 0, len(dead))
	for _, j := range dead {
		n := j.Job
		n.State, n.Attempts, n.At = Ready, 0, now
		m.RestoreLocked(name, n)
		out = append(out, n)
	}
	return out
}

// PurgeLocked deletes the queue. Returns how many jobs it held.
func (m *Manager) PurgeLocked(name string) int {
	q := m.queues[name]
	if q == nil {
		return 0
	}
	delete(m.queues, name)
	return len(q.jobs)
}

// Counts is QLEN's reply.
type Counts struct{ Ready, Delayed, Inflight, Dead int }

func (m *Manager) Len(name string) Counts {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.touch(name, time.Now().UnixMilli())
	if q == nil {
		return Counts{}
	}
	return Counts{q.ready.Len(), q.delayed.Len(), q.inflight.Len(), q.dead.Len()}
}

// DeadJobs returns up to count dead jobs (count < 0: all), oldest first.
func (m *Manager) DeadJobs(name string, count int) []Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	q := m.touch(name, time.Now().UnixMilli())
	if q == nil {
		return nil
	}
	dead := sortedJobs(q.dead)
	if count >= 0 && count < len(dead) {
		dead = dead[:count]
	}
	out := make([]Job, len(dead))
	for i, j := range dead {
		out[i] = j.Job
	}
	return out
}

// WaitInfoLocked returns a channel closed when a job of name may become
// ready, and the time of the next time-driven transition (0 if none).
func (m *Manager) WaitInfoLocked(name string) (<-chan struct{}, int64) {
	ch := m.waiters[name]
	if ch == nil {
		ch = make(chan struct{})
		m.waiters[name] = ch
	}
	var next int64
	if q := m.queues[name]; q != nil {
		if q.delayed.Len() > 0 {
			next = q.delayed[0].At
		}
		if q.inflight.Len() > 0 && (next == 0 || q.inflight[0].At < next) {
			next = q.inflight[0].At
		}
	}
	return ch, next
}

// Names returns every queue name, sorted.
func (m *Manager) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.queues))
	for n := range m.queues {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Snapshot returns every job of every queue, ordered by queue name, then
// state (ready, delayed, inflight, dead), then (at, seq). Restoring them in
// this order recreates the same queues.
func (m *Manager) Snapshot() map[string][]Job {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]Job, len(m.queues))
	for name, q := range m.queues {
		var jobs []Job
		for _, h := range []jobHeap{q.ready, q.delayed, q.inflight, q.dead} {
			for _, j := range sortedJobs(h) {
				jobs = append(jobs, j.Job)
			}
		}
		out[name] = jobs
	}
	return out
}

// RestoreArgs is the internal command that recreates one job exactly:
// q.restore queue id payload retries attempts state at.
func RestoreArgs(name string, j Job) []string {
	return []string{"q.restore", name, j.ID, j.Payload,
		strconv.FormatInt(j.Retries, 10), strconv.FormatInt(j.Attempts, 10),
		j.State.String(), strconv.FormatInt(j.At, 10)}
}

// Dump emits q.restore for every job, recreating all queues.
func (m *Manager) Dump(emit func(args []string)) {
	snap := m.Snapshot()
	names := make([]string, 0, len(snap))
	for n := range snap {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		for _, j := range snap[n] {
			emit(RestoreArgs(n, j))
		}
	}
}

func sortedJobs(h jobHeap) []*job {
	out := append([]*job(nil), h...)
	sort.Slice(out, func(a, b int) bool {
		if out[a].At != out[b].At {
			return out[a].At < out[b].At
		}
		return out[a].seq < out[b].seq
	})
	return out
}
