package command

import (
	"strconv"
	"time"

	"github.com/shamim3696/cache-boy/internal/feature/queue"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Job queues (feature/queue).
//
// Queue names are namespaced by hand (they are not keyspace keys). Every
// mutation is propagated to AOF/replicas as "set this job to this state"
// operations with absolute timestamps, written while the queue manager's
// mutex is held so their order matches execution order:
//
//	q.restore queue id payload retries attempts state at   (QPUSH, QNACK, QRETRYDEAD, Dump)
//	q.claim   queue id attempts deadline                   (QPOP)
//	q.ack     queue id                                      (QACK)
//	QPURGE    queue
//
// Names in propagated commands are stored (namespaced) names. The q.*
// commands are internal and hidden from the docs.

func init() {
	const w = FlagWrite | FlagNoPropagate // propagated by hand, see above
	const internal = w | FlagNoScript
	Register(
		&Spec{Name: "qpush", Arity: -3, Flags: w | FlagDenyOOM, Group: "features",
			Summary: "QPUSH queue payload [DELAY ms] [RETRIES n] [ID id] — adds a job, returns its id", Handler: qpushCmd},
		&Spec{Name: "qpop", Arity: -2, Flags: w | FlagBlocking, Group: "features",
			Summary: "QPOP queue [VISIBILITY ms] [BLOCK ms] — [id, payload, attempt] or null", Handler: qpopCmd},
		&Spec{Name: "qack", Arity: 3, Flags: w, Group: "features",
			Summary: "QACK queue id — completes an in-flight job", Handler: qackCmd},
		&Spec{Name: "qnack", Arity: -3, Flags: w, Group: "features",
			Summary: "QNACK queue id [DELAY ms] — retries an in-flight job (dead-letters it after RETRIES)", Handler: qnackCmd},
		&Spec{Name: "qlen", Arity: 2, Flags: FlagReadOnly, Group: "features",
			Summary: "QLEN queue — {ready, delayed, inflight, dead}", Handler: qlenCmd},
		&Spec{Name: "qdead", Arity: -2, Flags: FlagReadOnly, Group: "features",
			Summary: "QDEAD queue [count] — dead-lettered jobs [id, payload, attempts]", Handler: qdeadCmd},
		&Spec{Name: "qretrydead", Arity: 2, Flags: w, Group: "features",
			Summary: "QRETRYDEAD queue — moves dead jobs back to ready", Handler: qretrydeadCmd},
		&Spec{Name: "qpurge", Arity: 2, Flags: w, Group: "features",
			Summary: "QPURGE queue — deletes every job of the queue", Handler: qpurgeCmd},

		&Spec{Name: "q.restore", Arity: 8, Flags: internal, Group: "features",
			Summary: "internal: recreates a job exactly", Handler: qRestoreCmd},
		&Spec{Name: "q.claim", Arity: 5, Flags: internal, Group: "features",
			Summary: "internal: replays a QPOP", Handler: qClaimCmd},
		&Spec{Name: "q.ack", Arity: 3, Flags: internal, Group: "features",
			Summary: "internal: replays a QACK", Handler: qAckCmd},
	)
}

const (
	defaultVisibility = 30 * time.Second
	defaultRetries    = 3
)

func qpushCmd(x *Ctx) resp.Value {
	var delay time.Duration
	retries := int64(defaultRetries)
	id := ""
	for i := 3; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		switch x.ArgLower(i) {
		case "delay":
			d, ok := featureDuration(x.Arg(i + 1))
			if !ok {
				return resp.Err("ERR invalid DELAY")
			}
			delay = d
		case "retries":
			n, ok := x.Int(i + 1)
			if !ok || n < 0 {
				return resp.Err("ERR RETRIES must be a non-negative integer")
			}
			retries = n
		case "id":
			id = x.Arg(i + 1)
			if id == "" {
				return resp.Err("ERR empty job id")
			}
		default:
			return ErrSyntax
		}
	}
	name := x.StoredKey(x.Arg(1))
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	if id != "" && m.ExistsLocked(name, id) {
		return resp.Bulk(id)
	}
	for id == "" || m.ExistsLocked(name, id) {
		id = m.NewID()
	}
	now := x.Now()
	j := queue.Job{ID: id, Payload: x.Arg(2), Retries: retries, State: queue.Ready, At: now}
	if delay > 0 {
		j.State, j.At = queue.Delayed, now+delay.Milliseconds()
	}
	m.RestoreLocked(name, j)
	x.PropagateNow(queue.RestoreArgs(name, j)...)
	return resp.Bulk(id)
}

func qpopCmd(x *Ctx) resp.Value {
	visibility := defaultVisibility
	block, blocking := time.Duration(0), false
	for i := 2; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		d, ok := featureDuration(x.Arg(i + 1))
		if !ok {
			return resp.Errf("ERR invalid %s", x.Arg(i))
		}
		switch x.ArgLower(i) {
		case "visibility":
			if d <= 0 {
				return resp.Err("ERR VISIBILITY must be positive")
			}
			visibility = d
		case "block":
			block, blocking = d, true
		default:
			return ErrSyntax
		}
	}
	if x.InExec {
		blocking = false
	}
	name := x.StoredKey(x.Arg(1))
	m := x.E.Queues
	var deadline time.Time
	if blocking && block > 0 {
		deadline = time.Now().Add(block)
	}
	for {
		now := time.Now()
		m.Lock()
		j, ok := m.PopLocked(name, visibility, now.UnixMilli())
		if ok {
			x.PropagateNow("q.claim", name, j.ID, strconv.FormatInt(j.Attempts, 10), strconv.FormatInt(j.At, 10))
			m.Unlock()
			return resp.Array(resp.Bulk(j.ID), resp.Bulk(j.Payload), resp.Int(j.Attempts))
		}
		if !blocking || (!deadline.IsZero() && !now.Before(deadline)) {
			m.Unlock()
			return resp.NullArray()
		}
		ch, next := m.WaitInfoLocked(name)
		m.Unlock()

		wake := deadline
		if next > 0 {
			if t := time.UnixMilli(next); wake.IsZero() || t.Before(wake) {
				wake = t
			}
		}
		if !qpopWait(x, ch, wake) {
			return resp.NullArray()
		}
	}
}

// qpopWait waits until ch is closed or wake passes (zero: no limit). It
// returns false if the client disconnected or the server is stopping.
func qpopWait(x *Ctx, ch <-chan struct{}, wake time.Time) bool {
	var timer <-chan time.Time
	if !wake.IsZero() {
		d := time.Until(wake)
		if d < time.Millisecond {
			d = time.Millisecond
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-ch:
	case <-timer:
	case <-x.C.Done():
		return false
	case <-x.E.Stopping():
		return false
	}
	return true
}

func qackCmd(x *Ctx) resp.Value {
	name := x.StoredKey(x.Arg(1))
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	if !m.AckLocked(name, x.Arg(2), x.Now()) {
		return resp.Int(0)
	}
	x.PropagateNow("q.ack", name, x.Arg(2))
	return resp.Int(1)
}

func qnackCmd(x *Ctx) resp.Value {
	var delay time.Duration
	switch x.NArgs() {
	case 3:
	case 5:
		if x.ArgLower(3) != "delay" {
			return ErrSyntax
		}
		d, ok := featureDuration(x.Arg(4))
		if !ok {
			return resp.Err("ERR invalid DELAY")
		}
		delay = d
	default:
		return ErrSyntax
	}
	name := x.StoredKey(x.Arg(1))
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	j, ok := m.NackLocked(name, x.Arg(2), delay, x.Now())
	if !ok {
		return resp.Int(0)
	}
	x.PropagateNow(queue.RestoreArgs(name, j)...)
	return resp.Int(1)
}

func qlenCmd(x *Ctx) resp.Value {
	c := x.E.Queues.Len(x.StoredKey(x.Arg(1)))
	return resp.Map(
		resp.Bulk("ready"), resp.Int(int64(c.Ready)),
		resp.Bulk("delayed"), resp.Int(int64(c.Delayed)),
		resp.Bulk("inflight"), resp.Int(int64(c.Inflight)),
		resp.Bulk("dead"), resp.Int(int64(c.Dead)),
	)
}

func qdeadCmd(x *Ctx) resp.Value {
	count := -1
	switch x.NArgs() {
	case 2:
	case 3:
		n, ok := x.Int(2)
		if !ok || n < 0 {
			return resp.Err("ERR count must be a non-negative integer")
		}
		count = int(n)
	default:
		return ErrSyntax
	}
	jobs := x.E.Queues.DeadJobs(x.StoredKey(x.Arg(1)), count)
	out := make([]resp.Value, len(jobs))
	for i, j := range jobs {
		out[i] = resp.Array(resp.Bulk(j.ID), resp.Bulk(j.Payload), resp.Int(j.Attempts))
	}
	return resp.Array(out...)
}

func qretrydeadCmd(x *Ctx) resp.Value {
	name := x.StoredKey(x.Arg(1))
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	jobs := m.RetryDeadLocked(name, x.Now())
	for _, j := range jobs {
		x.PropagateNow(queue.RestoreArgs(name, j)...)
	}
	return resp.Int(int64(len(jobs)))
}

func qpurgeCmd(x *Ctx) resp.Value {
	name := x.StoredKey(x.Arg(1))
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	n := m.PurgeLocked(name)
	if n > 0 {
		x.PropagateNow("QPURGE", name)
	}
	return resp.Int(int64(n))
}

// --- internal replication commands (names are stored names) ---

func qRestoreCmd(x *Ctx) resp.Value {
	retries, ok1 := x.Int(4)
	attempts, ok2 := x.Int(5)
	state, ok3 := queue.ParseState(x.Arg(6))
	at, ok4 := x.Int(7)
	if !ok1 || !ok2 || !ok3 || !ok4 || x.Arg(2) == "" {
		return ErrSyntax
	}
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	m.RestoreLocked(x.Arg(1), queue.Job{ID: x.Arg(2), Payload: x.Arg(3), Retries: retries, Attempts: attempts, State: state, At: at})
	x.PropagateNow(x.Args...)
	return resp.OK
}

func qClaimCmd(x *Ctx) resp.Value {
	attempts, ok1 := x.Int(3)
	deadline, ok2 := x.Int(4)
	if !ok1 || !ok2 {
		return ErrSyntax
	}
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	if !m.ClaimLocked(x.Arg(1), x.Arg(2), attempts, deadline) {
		return resp.Int(0)
	}
	x.PropagateNow(x.Args...)
	return resp.Int(1)
}

func qAckCmd(x *Ctx) resp.Value {
	m := x.E.Queues
	m.Lock()
	defer m.Unlock()
	if !m.DeleteLocked(x.Arg(1), x.Arg(2)) {
		return resp.Int(0)
	}
	x.PropagateNow(x.Args...)
	return resp.Int(1)
}
