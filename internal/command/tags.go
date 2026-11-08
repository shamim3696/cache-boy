package command

import (
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Tag-based invalidation. The index lives in Engine.Tags (feature/tags) and
// is keyed by stored (namespaced) keys. SET ... TAGS calls Engine.Tags.Tag.

func init() {
	Register(
		&Spec{Name: "tag.add", Arity: -3, Flags: FlagWrite, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "TAG.ADD key tag [tag ...] — attaches tags to an existing key", Handler: tagAddCmd},
		&Spec{Name: "tag.remove", Arity: -3, Flags: FlagWrite, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "TAG.REMOVE key tag [tag ...] — detaches tags from a key", Handler: tagRemoveCmd},
		&Spec{Name: "tag.keys", Arity: 2, Flags: FlagReadOnly, Group: "features",
			Summary: "TAG.KEYS tag — keys carrying the tag", Handler: tagKeysCmd},
		&Spec{Name: "tag.of", Arity: 2, Flags: FlagReadOnly, Group: "features", FirstKey: 1, LastKey: 1,
			Summary: "TAG.OF key — tags of a key", Handler: tagOfCmd},
		&Spec{Name: "tag.invalidate", Arity: -2, Flags: FlagWrite | FlagAllKeys | FlagNoPropagate, Group: "features",
			Summary: "TAG.INVALIDATE tag [tag ...] — deletes every key carrying any of the tags", Handler: tagInvalidateCmd},
		&Spec{Name: "tag.list", Arity: 1, Flags: FlagReadOnly, Group: "features",
			Summary: "TAG.LIST — every tag in use", Handler: tagListCmd},
	)
}

func tagAddCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	if x.DB().Peek(key) == nil {
		x.NoChange()
		return resp.Int(0)
	}
	n := x.E.Tags.Tag(key, x.Args[2:]...)
	if n == 0 {
		x.NoChange()
	}
	return resp.Int(int64(n))
}

func tagRemoveCmd(x *Ctx) resp.Value {
	n := x.E.Tags.Untag(x.Keys[0], x.Args[2:]...)
	if n == 0 {
		x.NoChange()
	}
	return resp.Int(int64(n))
}

// liveKeys keeps the stored keys that still exist, locking their shards
// unless the caller already holds every lock (MULTI/EXEC, scripts, or a
// FlagAllKeys command). Expired keys found here emit "expired", which drops
// them from the index.
func liveKeys(x *Ctx, keys []string) []string {
	if len(keys) == 0 {
		return keys
	}
	if !x.InExec && x.spec.Flags&FlagAllKeys == 0 {
		unlock := x.DB().Lock(keys...)
		defer unlock()
	}
	out := keys[:0:0]
	for _, k := range keys {
		if x.DB().Peek(k) != nil {
			out = append(out, k)
		}
	}
	return out
}

func tagKeysCmd(x *Ctx) resp.Value {
	var visible []string
	for _, k := range x.E.Tags.Keys(x.Arg(1)) {
		if _, ok := x.UserKey(k); ok {
			visible = append(visible, k)
		}
	}
	live := liveKeys(x, visible)
	out := make([]string, 0, len(live))
	for _, k := range live {
		u, _ := x.UserKey(k)
		out = append(out, u)
	}
	return resp.Strings(sortStrings(out))
}

func tagOfCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	if x.DB().Peek(key) == nil {
		return resp.Array()
	}
	return resp.Strings(x.E.Tags.Of(key))
}

func tagInvalidateCmd(x *Ctx) resp.Value {
	// KeysOf returns a copy: the index changes as keys are deleted below.
	n := 0
	for _, k := range x.E.Tags.KeysOf(x.Args[1:]...) {
		if _, ok := x.UserKey(k); !ok {
			continue // only the caller's namespace
		}
		if x.DB().Peek(k) == nil { // expired: already dropped from the index
			continue
		}
		x.DB().Delete(k)
		x.Emit(k)
		x.PropagateNow("DEL", k)
		n++
	}
	return resp.Int(int64(n))
}

func tagListCmd(x *Ctx) resp.Value {
	return resp.Strings(x.E.Tags.List())
}
