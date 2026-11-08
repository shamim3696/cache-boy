package command

import (
	"math"

	"cache-boy/internal/datatype/bloom"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

// Bloom filter commands, with RedisBloom's syntax and replies. Hashing is
// deterministic, so every write is propagated as sent.
func init() {
	Register(
		&Spec{Name: "bf.reserve", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "BF.RESERVE key error_rate capacity [EXPANSION n] [NONSCALING]", Handler: bfReserveCmd},
		&Spec{Name: "bf.add", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "Adds an item; 1 if it was not present", Handler: bfAddCmd},
		&Spec{Name: "bf.madd", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "BF.MADD key item [item ...]", Handler: bfAddCmd},
		&Spec{Name: "bf.exists", Arity: 3, Flags: FlagReadOnly, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "1 if the item may exist, 0 if it surely doesn't", Handler: bfExistsCmd},
		&Spec{Name: "bf.mexists", Arity: -3, Flags: FlagReadOnly, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "BF.MEXISTS key item [item ...]", Handler: bfExistsCmd},
		&Spec{Name: "bf.card", Arity: 2, Flags: FlagReadOnly, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "Number of items added", Handler: bfCardCmd},
		&Spec{Name: "bf.info", Arity: -2, Flags: FlagReadOnly, Group: "bloom", FirstKey: 1, LastKey: 1, Summary: "BF.INFO key [CAPACITY|SIZE|FILTERS|ITEMS|EXPANSION]", Handler: bfInfoCmd},
	)
}

// getBloom returns key's filter, nil if missing, or false on a wrong type.
func getBloom(x *Ctx, key string) (*bloom.Filter, bool) {
	e, err := x.DB().GetKind(key, store.KindBloom)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.Bloom(), true
}

func bfReserveCmd(x *Ctx) resp.Value {
	rate, ok := x.Float(2)
	if !ok {
		return resp.Err("ERR bad error rate")
	}
	if !(rate > 0 && rate < 1) {
		return resp.Err("ERR (0 < error rate range < 1)")
	}
	capacity, ok := x.Int(3)
	if !ok {
		return resp.Err("ERR bad capacity")
	}
	if capacity <= 0 {
		return resp.Err("ERR (capacity should be larger than 0)")
	}
	expansion := int64(bloom.DefaultExpansion)
	var nonScaling, expansionSet bool
	for i := 4; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "expansion":
			if i+1 >= x.NArgs() {
				return ErrSyntax
			}
			n, ok := x.Int(i + 1)
			if !ok {
				return resp.Err("ERR bad expansion")
			}
			if n < 1 || n > math.MaxUint16 {
				return resp.Err("ERR expansion should be greater or equal to 1")
			}
			expansion, expansionSet = n, true
			i++
		case "nonscaling":
			nonScaling = true
		default:
			return ErrSyntax
		}
	}
	if nonScaling && expansionSet {
		return resp.Err("ERR nonscaling filters cannot expand")
	}
	if x.DB().Get(x.Keys[0]) != nil {
		return resp.Err("ERR item exists")
	}
	f, err := bloom.New(rate, uint64(capacity), uint32(expansion), nonScaling)
	if err != nil {
		return resp.Err(err.Error())
	}
	x.DB().Put(x.Keys[0], store.NewEntry(store.KindBloom, f))
	return resp.OK
}

// bfAddCmd serves BF.ADD (integer reply) and BF.MADD (array). A missing key
// gets a scaling filter with RedisBloom's defaults.
func bfAddCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	f, ok := getBloom(x, key)
	if !ok {
		return ErrWrongTyp
	}
	created := f == nil
	if created {
		f, _ = bloom.New(bloom.DefaultErrorRate, bloom.DefaultCapacity, bloom.DefaultExpansion, false)
		x.DB().Put(key, store.NewEntry(store.KindBloom, f))
	}
	changed := created
	out := make([]resp.Value, 0, x.NArgs()-2)
	for _, item := range x.Args[2:] {
		added, err := f.Add(item)
		switch {
		case err != nil:
			out = append(out, resp.Err(err.Error()))
		case added:
			changed = true
			out = append(out, resp.Int(1))
		default:
			out = append(out, resp.Int(0))
		}
	}
	if changed {
		x.DB().Updated(key)
	} else {
		x.NoChange()
	}
	if x.Name == "bf.add" {
		return out[0]
	}
	return resp.Array(out...)
}

func bfExistsCmd(x *Ctx) resp.Value {
	f, ok := getBloom(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	out := make([]resp.Value, 0, x.NArgs()-2)
	for _, item := range x.Args[2:] {
		out = append(out, intBool(f != nil && f.Exists(item)))
	}
	if x.Name == "bf.exists" {
		return out[0]
	}
	return resp.Array(out...)
}

func bfCardCmd(x *Ctx) resp.Value {
	f, ok := getBloom(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if f == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(f.Count()))
}

func bfInfoCmd(x *Ctx) resp.Value {
	if x.NArgs() > 3 {
		return wrongArgs("bf.info")
	}
	f, ok := getBloom(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if f == nil {
		return resp.Err("ERR not found")
	}
	var expansion resp.Value = resp.Int(int64(f.Expansion()))
	if f.NonScaling() {
		expansion = resp.Null()
	}
	fields := []struct {
		opt, name string
		v         resp.Value
	}{
		{"capacity", "Capacity", resp.Int(int64(f.Capacity()))},
		{"size", "Size", resp.Int(f.MemSize())},
		{"filters", "Number of filters", resp.Int(int64(f.NumFilters()))},
		{"items", "Number of items inserted", resp.Int(int64(f.Count()))},
		{"expansion", "Expansion rate", expansion},
	}
	if x.NArgs() == 3 {
		for _, fl := range fields {
			if fl.opt == x.ArgLower(2) {
				return resp.Array(fl.v)
			}
		}
		return resp.Err("ERR Invalid information value")
	}
	m := make([]resp.Value, 0, 2*len(fields))
	for _, fl := range fields {
		m = append(m, resp.Bulk(fl.name), fl.v)
	}
	return resp.Map(m...)
}
