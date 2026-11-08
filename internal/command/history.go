package command

import (
	"bytes"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Key history (feature/history), enabled with CONFIG SET history-depth N.
// History is per node and in memory: it is not persisted. RESTOREVERSION
// propagates as SET/DEL of the restored value, so replicas stay in sync even
// if their own history differs.

func init() {
	Register(
		&Spec{Name: "history", Arity: -2, Flags: FlagReadOnly, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "HISTORY key [COUNT n] — [version, timestamp_ms, value] newest first", Handler: historyCmd},
		&Spec{Name: "getversion", Arity: 3, Flags: FlagReadOnly, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "GETVERSION key version — the value of an old version", Handler: getversionCmd},
		&Spec{Name: "restoreversion", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "RESTOREVERSION key version — makes an old version current", Handler: restoreversionCmd},
		&Spec{Name: "history.clear", Arity: 2, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "HISTORY.CLEAR key — forgets a key's history", Handler: historyClearCmd},
	)
}

func historyCmd(x *Ctx) resp.Value {
	count := -1
	switch x.NArgs() {
	case 2:
	case 4:
		n, ok := x.Int(3)
		if x.ArgLower(2) != "count" || !ok || n < 0 {
			return ErrSyntax
		}
		count = int(n)
	default:
		return ErrSyntax
	}
	vs := x.E.History.Versions(x.Keys[0], count)
	out := make([]resp.Value, len(vs))
	for i, v := range vs {
		val := resp.Null()
		if !v.Deleted {
			val = resp.Bytes(v.Value)
		}
		out[i] = resp.Array(resp.Int(v.N), resp.Int(v.TimeMs), val)
	}
	return resp.Array(out...)
}

func getversionCmd(x *Ctx) resp.Value {
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	v, ok := x.E.History.Get(x.Keys[0], n)
	if !ok || v.Deleted {
		return resp.Null()
	}
	return resp.Bytes(v.Value)
}

func restoreversionCmd(x *Ctx) resp.Value {
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	key := x.Keys[0]
	v, ok := x.E.History.Get(key, n)
	if !ok {
		return resp.Err("ERR unknown version")
	}
	if v.Deleted { // restoring a deletion deletes the key
		if !x.DB().Delete(key) {
			x.NoChange()
		}
		x.Propagate("DEL", key)
		return resp.OK
	}
	val := bytes.Clone(v.Value)
	x.DB().Put(key, x.DB().NewStringValue(val))
	x.Propagate("SET", key, string(val))
	return resp.OK
}

func historyClearCmd(x *Ctx) resp.Value {
	return featInt01(x.E.History.Clear(x.Keys[0]))
}
