package command

import (
	"strconv"
	"time"

	"cache-boy/internal/protocol/resp"
)

// Distributed locks with fencing tokens (feature/lock).
//
// Locks are leader-local: they are not keyspace keys, not persisted and not
// replicated, so a failover loses them (fencing tokens keep increasing
// across restarts because the counter is seeded from the clock). Lock names
// are namespaced by hand. LOCK ... WAIT blocks the client's goroutine
// without holding any shard lock; inside MULTI or a script it doesn't wait.

func init() {
	Register(
		&Spec{Name: "lock", Arity: -3, Group: "features",
			Summary: "LOCK name ttl [WAIT ms] [OWNER id] — fencing token, or null if not acquired", Handler: lockCmd},
		&Spec{Name: "unlock", Arity: 3, Group: "features",
			Summary: "UNLOCK name token — releases the lock if the token matches", Handler: unlockCmd},
		&Spec{Name: "lock.extend", Arity: 4, Group: "features",
			Summary: "LOCK.EXTEND name token ttl — sets a new ttl if the token matches", Handler: lockExtendCmd},
		&Spec{Name: "lock.info", Arity: 2, Group: "features",
			Summary: "LOCK.INFO name — {token, owner, ttl_ms} or null", Handler: lockInfoCmd},
	)
}

func lockCmd(x *Ctx) resp.Value {
	ttl, ok := featureDuration(x.Arg(2))
	if !ok || ttl <= 0 {
		return resp.Err("ERR invalid ttl")
	}
	var wait time.Duration
	owner := "client:" + strconv.FormatInt(x.C.ID, 10)
	for i := 3; i < x.NArgs(); i++ {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		switch x.ArgLower(i) {
		case "wait":
			d, ok := featureDuration(x.Arg(i + 1))
			if !ok {
				return resp.Err("ERR invalid WAIT")
			}
			wait = d
		case "owner":
			owner = x.Arg(i + 1)
		default:
			return ErrSyntax
		}
		i++
	}
	name := x.StoredKey(x.Arg(1))
	var tok int64
	if wait > 0 && !x.InExec {
		tok, ok = x.E.Locks.Acquire(name, ttl, owner, wait, x.C.Done(), x.E.Stopping())
	} else {
		tok, ok = x.E.Locks.TryAcquire(name, ttl, owner)
	}
	if !ok {
		return resp.Null()
	}
	return resp.Int(tok)
}

func unlockCmd(x *Ctx) resp.Value {
	tok, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	return featInt01(x.E.Locks.Release(x.StoredKey(x.Arg(1)), tok))
}

func lockExtendCmd(x *Ctx) resp.Value {
	tok, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	ttl, ok := featureDuration(x.Arg(3))
	if !ok || ttl <= 0 {
		return resp.Err("ERR invalid ttl")
	}
	return featInt01(x.E.Locks.Extend(x.StoredKey(x.Arg(1)), tok, ttl))
}

func lockInfoCmd(x *Ctx) resp.Value {
	info, ok := x.E.Locks.Get(x.StoredKey(x.Arg(1)))
	if !ok {
		return resp.Null()
	}
	return resp.Map(
		resp.Bulk("token"), resp.Int(info.Token),
		resp.Bulk("owner"), resp.Bulk(info.Owner),
		resp.Bulk("ttl_ms"), resp.Int(featCeilMs(info.TTL)),
	)
}
