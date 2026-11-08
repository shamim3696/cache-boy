package command

import (
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/transaction"
)

func init() {
	Register(
		&Spec{Name: "multi", Arity: 1, Flags: FlagNoMulti, Group: "transaction", Summary: "Starts a transaction", Handler: multiCmd},
		&Spec{Name: "exec", Arity: 1, Flags: FlagNoMulti | FlagAllKeys | FlagNoPropagate | FlagNoScript, Group: "transaction", Summary: "Runs all queued commands atomically", Handler: execCmd},
		&Spec{Name: "discard", Arity: 1, Flags: FlagNoMulti | FlagNoScript, Group: "transaction", Summary: "Drops queued commands", Handler: discardCmd},
		&Spec{Name: "watch", Arity: -2, Flags: FlagNoMulti | FlagNoScript, Group: "transaction", FirstKey: 1, LastKey: -1, Summary: "Aborts the next EXEC if any key changes", Handler: watchCmd},
		&Spec{Name: "unwatch", Arity: 1, Flags: FlagNoScript, Group: "transaction", Summary: "Forgets all watched keys", Handler: unwatchCmd},
	)
}

func multiCmd(x *Ctx) resp.Value {
	if x.C.Multi != nil {
		return resp.Err("ERR MULTI calls can not be nested")
	}
	x.C.Multi = &transaction.Queue{}
	return resp.OK
}

func execCmd(x *Ctx) resp.Value {
	c := x.C
	if c.Multi == nil {
		return resp.Err("ERR EXEC without MULTI")
	}
	q := c.Multi
	c.Multi = nil
	defer func() {
		x.E.Tx.Unwatch(c.Watcher)
		c.Watcher = nil
	}()
	if q.Failed {
		return resp.Err("EXECABORT Transaction discarded because of previous errors.")
	}
	// All shards are locked (FlagAllKeys), so nothing can change between this
	// check and running the commands.
	if c.Watcher != nil && c.Watcher.Dirty() {
		return resp.NullArray()
	}
	out := make([]resp.Value, len(q.Commands))
	for i, args := range q.Commands {
		out[i] = x.E.ExecLocked(c, args)
	}
	return resp.Array(out...)
}

func discardCmd(x *Ctx) resp.Value {
	if x.C.Multi == nil {
		return resp.Err("ERR DISCARD without MULTI")
	}
	x.C.Multi = nil
	x.E.Tx.Unwatch(x.C.Watcher)
	x.C.Watcher = nil
	return resp.OK
}

func watchCmd(x *Ctx) resp.Value {
	if x.C.Multi != nil {
		return resp.Err("ERR WATCH inside MULTI is not allowed")
	}
	x.C.Watcher = x.E.Tx.Watch(x.C.Watcher, x.Keys...)
	return resp.OK
}

func unwatchCmd(x *Ctx) resp.Value {
	x.E.Tx.Unwatch(x.C.Watcher)
	x.C.Watcher = nil
	return resp.OK
}
