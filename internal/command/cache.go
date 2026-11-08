package command

import (
	"time"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

// Cache-aside with stampede protection (feature/singleflight).
//
// CACHE.GET returns ["hit", value] when the key holds a string. Otherwise
// the first caller gets the compute lock (["compute", null]) and the others
// block until the key is written (any write clears the lock and wakes them,
// then they re-run and usually see a hit), the lock is released, or WAIT
// runs out (["timeout", null]). A waiter re-running after its WAIT finds the
// lock expired takes it over instead of timing out.

func init() {
	Register(
		&Spec{Name: "cache.get", Arity: -2, Flags: FlagReadOnly | FlagBlocking, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "CACHE.GET key [LOCK ms] [WAIT ms] — [hit|compute|timeout, value]", Handler: cacheGetCmd},
		&Spec{Name: "cache.release", Arity: 2, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "CACHE.RELEASE key — gives up the compute lock", Handler: cacheReleaseCmd},
	)
}

const (
	defaultCacheLock = 10 * time.Second
	defaultCacheWait = 5 * time.Second
)

func cacheGetCmd(x *Ctx) resp.Value {
	lockTTL, wait := defaultCacheLock, defaultCacheWait
	for i := 2; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		d, ok := featureDuration(x.Arg(i + 1))
		if !ok {
			return resp.Errf("ERR invalid %s", x.Arg(i))
		}
		switch x.ArgLower(i) {
		case "lock":
			if d <= 0 {
				return resp.Err("ERR LOCK must be positive")
			}
			lockTTL = d
		case "wait":
			wait = d
		default:
			return ErrSyntax
		}
	}
	key := x.Keys[0]
	e, err := x.DB().GetKind(key, store.KindString)
	if err != nil {
		return ErrWrongTyp
	}
	if e != nil {
		return resp.Array(resp.Bulk("hit"), resp.Bytes(e.Str().Bytes()))
	}
	if x.E.Flight.Acquire(key, x.C.ID, lockTTL) {
		return resp.Array(resp.Bulk("compute"), resp.Null())
	}
	if x.TimedOut || wait <= 0 {
		return resp.Array(resp.Bulk("timeout"), resp.Null())
	}
	return x.Block(wait)
}

func cacheReleaseCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	if !x.E.Flight.Release(key) {
		return resp.Int(0)
	}
	x.Emit(key) // wakes CACHE.GET waiters, one of them takes the lock
	return resp.Int(1)
}
