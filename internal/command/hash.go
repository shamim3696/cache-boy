package command

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "hset", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Sets fields of a hash", Handler: hsetCmd},
		&Spec{Name: "hmset", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Sets fields of a hash (replies OK)", Handler: hsetCmd},
		&Spec{Name: "hsetnx", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Sets a field if it doesn't exist", Handler: hsetnxCmd},
		&Spec{Name: "hget", Arity: 3, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Returns a field's value", Handler: hgetCmd},
		&Spec{Name: "hmget", Arity: -3, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Returns several fields' values", Handler: hmgetCmd},
		&Spec{Name: "hdel", Arity: -3, Flags: FlagWrite, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Deletes fields", Handler: hdelCmd},
		&Spec{Name: "hlen", Arity: 2, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Number of fields", Handler: hlenCmd},
		&Spec{Name: "hexists", Arity: 3, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Whether a field exists", Handler: hexistsCmd},
		&Spec{Name: "hgetall", Arity: 2, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Returns every field and value", Handler: hgetallCmd},
		&Spec{Name: "hkeys", Arity: 2, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Returns every field", Handler: hgetallCmd},
		&Spec{Name: "hvals", Arity: 2, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Returns every value", Handler: hgetallCmd},
		&Spec{Name: "hincrby", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Adds to an integer field", Handler: hincrbyCmd},
		&Spec{Name: "hincrbyfloat", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Adds to a float field", Handler: hincrbyfloatCmd},
		&Spec{Name: "hstrlen", Arity: 3, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "Length of a field's value", Handler: hstrlenCmd},
		&Spec{Name: "hrandfield", Arity: -2, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "HRANDFIELD key [count [WITHVALUES]]", Handler: hrandfieldCmd},
		&Spec{Name: "hscan", Arity: -3, Flags: FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Summary: "HSCAN key cursor [MATCH p] [COUNT n] [NOVALUES]", Handler: hscanCmd},
	)
}

func newHash() sizer { return store.NewHash() }

// getHash returns key's hash, nil if missing, or false on a wrong type.
func getHash(x *Ctx, key string) (*store.Hash, bool) {
	e, err := x.DB().GetKind(key, store.KindHash)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.Hash(), true
}

type fieldValue struct{ field, value string }

// sortedPairs returns the hash's fields and values ordered by field, so
// replies don't depend on the encoding's or Go's map iteration order.
func sortedPairs(h *store.Hash) []fieldValue {
	pairs := make([]fieldValue, 0, h.Len())
	h.Range(func(f, v string) bool {
		pairs = append(pairs, fieldValue{f, v})
		return true
	})
	slices.SortFunc(pairs, func(a, b fieldValue) int { return strings.Compare(a.field, b.field) })
	return pairs
}

// hsetCmd serves HSET and HMSET.
func hsetCmd(x *Ctx) resp.Value {
	if x.NArgs()%2 != 0 {
		return wrongArgs(x.Name)
	}
	key := x.Keys[0]
	e, ok := getOrCreate(x, key, store.KindHash, newHash)
	if !ok {
		return ErrWrongTyp
	}
	added := 0
	for i := 2; i < x.NArgs(); i += 2 {
		if e.Hash().Set(x.Arg(i), x.Arg(i+1)) {
			added++
		}
	}
	x.DB().Updated(key)
	if x.Name == "hmset" {
		return resp.OK
	}
	return resp.Int(int64(added))
}

func hsetnxCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	e, ok := getOrCreate(x, key, store.KindHash, newHash)
	if !ok {
		return ErrWrongTyp
	}
	if _, exists := e.Hash().Get(x.Arg(2)); exists {
		x.NoChange()
		return resp.Int(0)
	}
	e.Hash().Set(x.Arg(2), x.Arg(3))
	x.DB().Updated(key)
	return resp.Int(1)
}

func hgetCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if h != nil {
		if v, ok := h.Get(x.Arg(2)); ok {
			return resp.Bulk(v)
		}
	}
	return resp.Null()
}

func hmgetCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	out := make([]resp.Value, 0, x.NArgs()-2)
	for _, f := range x.Args[2:] {
		v, ok := "", false
		if h != nil {
			v, ok = h.Get(f)
		}
		if ok {
			out = append(out, resp.Bulk(v))
		} else {
			out = append(out, resp.Null())
		}
	}
	return resp.Array(out...)
}

func hdelCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	n := 0
	if h != nil {
		for _, f := range x.Args[2:] {
			if h.Del(f) {
				n++
			}
		}
	}
	if n == 0 {
		x.NoChange()
		return resp.Int(0)
	}
	x.DB().Updated(x.Keys[0])
	return resp.Int(int64(n))
}

func hlenCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if h == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(h.Len()))
}

func hexistsCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if h == nil {
		return resp.Int(0)
	}
	_, found := h.Get(x.Arg(2))
	return intBool(found)
}

// intBool is the 1/0 integer reply of predicates like HEXISTS.
func intBool(b bool) resp.Value {
	if b {
		return resp.Int(1)
	}
	return resp.Int(0)
}

// hgetallCmd serves HGETALL, HKEYS and HVALS.
func hgetallCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	var pairs []fieldValue
	if h != nil {
		pairs = sortedPairs(h)
	}
	out := make([]resp.Value, 0, len(pairs)*2)
	for _, p := range pairs {
		switch x.Name {
		case "hgetall":
			out = append(out, resp.Bulk(p.field), resp.Bulk(p.value))
		case "hkeys":
			out = append(out, resp.Bulk(p.field))
		default:
			out = append(out, resp.Bulk(p.value))
		}
	}
	if x.Name == "hgetall" {
		return resp.Map(out...)
	}
	return resp.Array(out...)
}

func hincrbyCmd(x *Ctx) resp.Value {
	delta, ok := x.Int(3)
	if !ok {
		return ErrNotInt
	}
	key, field := x.Keys[0], x.Arg(2)
	e, ok := getOrCreate(x, key, store.KindHash, newHash)
	if !ok {
		return ErrWrongTyp
	}
	h := e.Hash()
	var cur int64
	if v, exists := h.Get(field); exists {
		if cur, ok = ParseInt(v); !ok {
			x.DB().Updated(key)
			return resp.Err("ERR hash value is not an integer")
		}
	}
	if delta > 0 && cur > math.MaxInt64-delta || delta < 0 && cur < math.MinInt64-delta {
		x.DB().Updated(key)
		return ErrOverflow
	}
	cur += delta
	h.Set(field, strconv.FormatInt(cur, 10))
	x.DB().Updated(key)
	return resp.Int(cur)
}

func hincrbyfloatCmd(x *Ctx) resp.Value {
	delta, ok := x.Float(3)
	if !ok {
		return ErrNotFloat
	}
	key, field := x.Keys[0], x.Arg(2)
	e, ok := getOrCreate(x, key, store.KindHash, newHash)
	if !ok {
		return ErrWrongTyp
	}
	h := e.Hash()
	var cur float64
	if v, exists := h.Get(field); exists {
		if cur, ok = ParseFloat(v); !ok {
			x.DB().Updated(key)
			return resp.Err("ERR hash value is not a float")
		}
	}
	cur += delta
	if math.IsNaN(cur) || math.IsInf(cur, 0) {
		x.DB().Updated(key)
		return resp.Err("ERR increment would produce NaN or Infinity")
	}
	s := formatIncrFloat(cur)
	h.Set(field, s)
	x.DB().Updated(key)
	x.Propagate("HSET", key, field, s)
	return resp.Bulk(s)
}

func hstrlenCmd(x *Ctx) resp.Value {
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if h == nil {
		return resp.Int(0)
	}
	v, _ := h.Get(x.Arg(2))
	return resp.Int(int64(len(v)))
}

// parseRandCount reads "[count [WITH...]]" of SRANDMEMBER, HRANDFIELD and
// ZRANDMEMBER. hasCount is false when no count was given.
func parseRandCount(x *Ctx, withOpt string) (count int64, hasCount, with bool, bad resp.Value, ok bool) {
	if x.NArgs() == 2 {
		return 0, false, false, resp.Value{}, true
	}
	count, ok = x.Int(2)
	if !ok {
		return 0, false, false, ErrNotInt, false
	}
	if x.NArgs() > 4 || x.NArgs() == 4 && (withOpt == "" || x.ArgLower(3) != withOpt) {
		return 0, false, false, ErrSyntax, false
	}
	if count < -math.MaxInt32 || count > math.MaxInt32 {
		return 0, false, false, resp.Err("ERR value is out of range"), false
	}
	return count, true, x.NArgs() == 4, resp.Value{}, true
}

// randomPicks returns indexes into n items with Redis' count rules: a
// positive count picks distinct items (at most n), a negative one picks
// |count| items that may repeat.
func randomPicks(n int, count int64) []int {
	if n == 0 {
		return nil
	}
	if count < 0 {
		out := make([]int, -count)
		for i := range out {
			out[i] = rand.IntN(n)
		}
		return out
	}
	return rand.Perm(n)[:min(int(count), n)]
}

func hrandfieldCmd(x *Ctx) resp.Value {
	count, hasCount, withValues, bad, ok := parseRandCount(x, "withvalues")
	if !ok {
		return bad
	}
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if h == nil {
		if hasCount {
			return resp.Array()
		}
		return resp.Null()
	}
	pairs := sortedPairs(h)
	if !hasCount {
		return resp.Bulk(pairs[rand.IntN(len(pairs))].field)
	}
	var out []resp.Value
	for _, i := range randomPicks(len(pairs), count) {
		out = append(out, resp.Bulk(pairs[i].field))
		if withValues {
			out = append(out, resp.Bulk(pairs[i].value))
		}
	}
	return resp.Array(out...)
}

func hscanCmd(x *Ctx) resp.Value {
	_, o, bad, ok := parseScan(x, 2, false, true)
	if !ok {
		return bad
	}
	h, ok := getHash(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	var items []resp.Value
	if h != nil {
		for _, p := range sortedPairs(h) {
			if !o.matches(p.field) {
				continue
			}
			items = append(items, resp.Bulk(p.field))
			if !o.noValues {
				items = append(items, resp.Bulk(p.value))
			}
		}
	}
	return scanReply(0, items)
}
