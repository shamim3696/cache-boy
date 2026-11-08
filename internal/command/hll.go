package command

import (
	"github.com/shamim3696/cache-boy/internal/datatype/hll"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

// HyperLogLog commands. Unlike Redis, a sketch is its own kind (TYPE says
// "hyperloglog"), not a string, so GET/SETRANGE on it fail with WRONGTYPE.
func init() {
	Register(
		&Spec{Name: "pfadd", Arity: -2, Flags: FlagWrite | FlagDenyOOM, Group: "hyperloglog", FirstKey: 1, LastKey: 1, Summary: "PFADD key [element ...]", Handler: pfaddCmd},
		&Spec{Name: "pfcount", Arity: -2, Flags: FlagReadOnly, Group: "hyperloglog", FirstKey: 1, LastKey: -1, Summary: "PFCOUNT key [key ...]: cardinality of the union", Handler: pfcountCmd},
		&Spec{Name: "pfmerge", Arity: -2, Flags: FlagWrite | FlagDenyOOM, Group: "hyperloglog", FirstKey: 1, LastKey: -1, Summary: "PFMERGE destkey [sourcekey ...]", Handler: pfmergeCmd},
	)
}

// getHLL returns key's sketch, nil if missing, or false on a wrong type.
func getHLL(x *Ctx, key string) (*hll.HLL, bool) {
	e, err := x.DB().GetKind(key, store.KindHLL)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.HLL(), true
}

func pfaddCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	h, ok := getHLL(x, key)
	if !ok {
		return ErrWrongTyp
	}
	changed := h == nil
	if changed {
		h = hll.New()
		x.DB().Put(key, store.NewEntry(store.KindHLL, h))
	}
	for _, el := range x.Args[2:] {
		if h.Add([]byte(el)) {
			changed = true
		}
	}
	if !changed {
		x.NoChange()
		return resp.Int(0)
	}
	x.DB().Updated(key)
	return resp.Int(1)
}

func pfcountCmd(x *Ctx) resp.Value {
	if len(x.Keys) == 1 {
		h, ok := getHLL(x, x.Keys[0])
		if !ok {
			return ErrWrongTyp
		}
		if h == nil {
			return resp.Int(0)
		}
		return resp.Int(h.Count())
	}
	union := hll.New()
	for _, k := range x.Keys {
		h, ok := getHLL(x, k)
		if !ok {
			return ErrWrongTyp
		}
		if h != nil {
			union.Merge(h)
		}
	}
	return resp.Int(union.Count())
}

// pfmergeCmd stores the union of the sources and destkey's current value.
func pfmergeCmd(x *Ctx) resp.Value {
	srcs := make([]*hll.HLL, 0, len(x.Keys))
	for _, k := range x.Keys {
		h, ok := getHLL(x, k)
		if !ok {
			return ErrWrongTyp
		}
		if h != nil {
			srcs = append(srcs, h)
		}
	}
	dest := x.Keys[0]
	h, _ := getHLL(x, dest)
	if h == nil {
		h = hll.New()
		x.DB().Put(dest, store.NewEntry(store.KindHLL, h))
	}
	for _, s := range srcs {
		if s != h {
			h.Merge(s)
		}
	}
	x.DB().Updated(dest)
	return resp.OK
}
