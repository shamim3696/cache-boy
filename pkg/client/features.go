package client

import (
	"context"
	"errors"
	"strings"
	"time"
)

// --- tags ---

// TagAdd attaches tags to a key; returns how many were newly attached.
func (c *Client) TagAdd(ctx context.Context, key string, tags ...string) (int64, error) {
	return asInt(c.Do(ctx, "TAG.ADD", key, tags))
}

// TagInvalidate deletes every key carrying any of the tags; returns the count.
func (c *Client) TagInvalidate(ctx context.Context, tags ...string) (int64, error) {
	return asInt(c.Do(ctx, "TAG.INVALIDATE", tags))
}

// --- rate limiting ---

// RateLimitResult is the reply of RATELIMIT.
type RateLimitResult struct {
	Allowed    bool
	Remaining  int64
	RetryAfter time.Duration // 0 when allowed
	ResetAfter time.Duration
}

// RateLimit consumes cost (default 1) from a GCRA limit of `limit` per
// `period` for key.
func (c *Client) RateLimit(ctx context.Context, key string, limit int64, period time.Duration, cost ...int64) (RateLimitResult, error) {
	args := []any{"RATELIMIT", key, limit, ms(period)}
	if len(cost) > 0 {
		args = append(args, "COST", cost[0])
	}
	v, err := c.Do(ctx, args...)
	if err != nil {
		return RateLimitResult{}, err
	}
	if len(v.Elems) < 4 {
		return RateLimitResult{}, errors.New("cache-boy: unexpected RATELIMIT reply")
	}
	n := func(i int) int64 { x, _ := asInt(v.Elems[i], nil); return x }
	return RateLimitResult{
		Allowed:    n(0) == 1,
		Remaining:  n(1),
		RetryAfter: time.Duration(n(2)) * time.Millisecond,
		ResetAfter: time.Duration(n(3)) * time.Millisecond,
	}, nil
}

// --- locks ---

// ErrNotAcquired is returned by Lock when the lock is held by someone else.
var ErrNotAcquired = errors.New("cache-boy: lock not acquired")

// LockOptions are the options of Lock.
type LockOptions struct {
	Wait  time.Duration // how long to wait for the lock (0 = don't wait)
	Owner string        // shown in LOCK.INFO
}

// Lock is a held distributed lock. Token is the fencing token: pass it along
// with writes so stale holders can be rejected.
type Lock struct {
	c     *Client
	Name  string
	Token int64
}

// Lock acquires name for ttl, or returns ErrNotAcquired.
func (c *Client) Lock(ctx context.Context, name string, ttl time.Duration, opts ...LockOptions) (*Lock, error) {
	args := []any{"LOCK", name, ms(ttl)}
	if len(opts) > 0 {
		if opts[0].Wait > 0 {
			args = append(args, "WAIT", ms(opts[0].Wait))
		}
		if opts[0].Owner != "" {
			args = append(args, "OWNER", opts[0].Owner)
		}
	}
	tok, err := asInt(c.Do(ctx, args...))
	if errors.Is(err, ErrNil) {
		return nil, ErrNotAcquired
	}
	if err != nil {
		return nil, err
	}
	return &Lock{c: c, Name: name, Token: tok}, nil
}

// Unlock releases the lock; false if it had expired or was taken over.
func (l *Lock) Unlock(ctx context.Context) (bool, error) {
	return asBool(l.c.Do(ctx, "UNLOCK", l.Name, l.Token))
}

// Extend sets a new TTL; false if the lock is no longer held.
func (l *Lock) Extend(ctx context.Context, ttl time.Duration) (bool, error) {
	return asBool(l.c.Do(ctx, "LOCK.EXTEND", l.Name, l.Token, ms(ttl)))
}

// --- job queue ---

// QPushOptions are the options of QPush.
type QPushOptions struct {
	Delay   time.Duration // deliver later
	Retries int           // max retries before the dead-letter list (0 = server default, 3)
	ID      string        // idempotency id; a duplicate is ignored
}

// QPush enqueues a job and returns its id.
func (c *Client) QPush(ctx context.Context, queue string, payload any, opts ...QPushOptions) (string, error) {
	args := []any{"QPUSH", queue, payload}
	if len(opts) > 0 {
		o := opts[0]
		if o.Delay > 0 {
			args = append(args, "DELAY", ms(o.Delay))
		}
		if o.Retries > 0 {
			args = append(args, "RETRIES", o.Retries)
		}
		if o.ID != "" {
			args = append(args, "ID", o.ID)
		}
	}
	return asString(c.Do(ctx, args...))
}

// Job is a job taken with QPop. Ack or Nack it when done.
type Job struct {
	c       *Client
	Queue   string
	ID      string
	Payload string
	Attempt int64
}

// QPopOptions are the options of QPop.
type QPopOptions struct {
	Visibility time.Duration // re-deliver if not acked within this time (server default 30s)
	Block      time.Duration // wait up to this long for a job
	Forever    bool          // wait until a job arrives or ctx ends (BLOCK 0)
}

// QPop takes the next job, or returns ErrNil when none is ready (after
// blocking, if asked). Blocking honours ctx cancellation.
func (c *Client) QPop(ctx context.Context, queue string, opts ...QPopOptions) (*Job, error) {
	args := []any{"QPOP", queue}
	if len(opts) > 0 {
		o := opts[0]
		if o.Visibility > 0 {
			args = append(args, "VISIBILITY", ms(o.Visibility))
		}
		if o.Forever {
			args = append(args, "BLOCK", 0)
		} else if o.Block > 0 {
			args = append(args, "BLOCK", ms(o.Block))
		}
	}
	v, err := c.Do(ctx, args...)
	if err != nil {
		return nil, err
	}
	if v.IsNull() || len(v.Elems) < 2 {
		return nil, ErrNil
	}
	j := &Job{c: c, Queue: queue, ID: v.Elems[0].String(), Payload: v.Elems[1].String()}
	if len(v.Elems) > 2 {
		j.Attempt, _ = asInt(v.Elems[2], nil)
	}
	return j, nil
}

// QAck marks a job as done.
func (c *Client) QAck(ctx context.Context, queue, id string) (bool, error) {
	return asBool(c.Do(ctx, "QACK", queue, id))
}

// QNack fails a job: it is retried after delay, or dead-lettered.
func (c *Client) QNack(ctx context.Context, queue, id string, delay time.Duration) (bool, error) {
	args := []any{"QNACK", queue, id}
	if delay > 0 {
		args = append(args, "DELAY", ms(delay))
	}
	return asBool(c.Do(ctx, args...))
}

// Ack is QAck for this job.
func (j *Job) Ack(ctx context.Context) (bool, error) { return j.c.QAck(ctx, j.Queue, j.ID) }

// Nack is QNack for this job.
func (j *Job) Nack(ctx context.Context, delay time.Duration) (bool, error) {
	return j.c.QNack(ctx, j.Queue, j.ID, delay)
}

// --- cache-aside with stampede protection ---

// CacheGet statuses.
const (
	CacheHit     = "hit"
	CacheCompute = "compute"
	CacheTimeout = "timeout"
)

// CacheResult is the reply of CACHE.GET.
type CacheResult struct {
	Status string // CacheHit, CacheCompute or CacheTimeout
	Value  string // set when Status is CacheHit
}

// CacheGetOptions are the options of CacheGet.
type CacheGetOptions struct {
	Lock time.Duration // compute lock duration (server default 10s)
	Wait time.Duration // how long to wait for another computer (server default 5s)
}

// CacheGet runs CACHE.GET. Prefer Wrap, which implements the whole protocol.
func (c *Client) CacheGet(ctx context.Context, key string, opts ...CacheGetOptions) (CacheResult, error) {
	args := []any{"CACHE.GET", key}
	if len(opts) > 0 {
		if opts[0].Lock > 0 {
			args = append(args, "LOCK", ms(opts[0].Lock))
		}
		if opts[0].Wait > 0 {
			args = append(args, "WAIT", ms(opts[0].Wait))
		}
	}
	v, err := c.Do(ctx, args...)
	if err != nil {
		return CacheResult{}, err
	}
	if len(v.Elems) < 1 {
		return CacheResult{}, errors.New("cache-boy: unexpected CACHE.GET reply")
	}
	r := CacheResult{Status: strings.ToLower(v.Elems[0].String())}
	if len(v.Elems) > 1 && !v.Elems[1].IsNull() {
		r.Value = v.Elems[1].String()
	}
	return r, nil
}

// CacheRelease gives up the compute lock taken by CACHE.GET.
func (c *Client) CacheRelease(ctx context.Context, key string) (bool, error) {
	return asBool(c.Do(ctx, "CACHE.RELEASE", key))
}

// Wrap returns the cached value of key, or computes it with fn exactly once
// across all clients (others wait for the result):
//
//   - hit: the cached value is returned.
//   - compute: fn runs; its result is stored with SET key value PX ttl. If fn
//     fails, the compute lock is released (CACHE.RELEASE) and the error returned.
//   - timeout (another client took too long): fn runs and its result is
//     returned without being stored.
func (c *Client) Wrap(ctx context.Context, key string, ttl time.Duration, fn func(ctx context.Context) (string, error)) (string, error) {
	r, err := c.CacheGet(ctx, key)
	if err != nil {
		return "", err
	}
	switch r.Status {
	case CacheHit:
		return r.Value, nil
	case CacheCompute:
		v, err := fn(ctx)
		if err != nil {
			rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			c.CacheRelease(rctx, key)
			return "", err
		}
		if _, err := c.Set(ctx, key, v, SetOptions{TTL: ttl}); err != nil {
			return v, err
		}
		return v, nil
	default: // timeout
		return fn(ctx)
	}
}
