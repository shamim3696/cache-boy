package command

import (
	"math"
	"math/rand/v2"
	"strconv"
	"time"

	"cache-boy/internal/datatype/deque"
	"cache-boy/internal/datatype/jsondoc"
	"cache-boy/internal/datatype/skiplist"
	"cache-boy/internal/datatype/timeseries"
	"cache-boy/internal/datatype/vector"
	"cache-boy/internal/glob"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "del", Arity: -2, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: -1, Summary: "Deletes keys", Handler: delCmd},
		&Spec{Name: "unlink", Arity: -2, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: -1, Summary: "Deletes keys", Handler: delCmd},
		&Spec{Name: "exists", Arity: -2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: -1, Summary: "Counts how many of the keys exist", Handler: existsCmd},
		&Spec{Name: "type", Arity: 2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Returns the type of a key", Handler: typeCmd},
		&Spec{Name: "keys", Arity: 2, Flags: FlagReadOnly | FlagAllKeys, Group: "keyspace", Summary: "Returns keys matching a glob pattern", Handler: keysCmd},
		&Spec{Name: "scan", Arity: -2, Flags: FlagReadOnly | FlagAllKeys, Group: "keyspace", Summary: "Iterates the keyspace: SCAN cursor [MATCH p] [COUNT n] [TYPE t]", Handler: scanCmd},
		&Spec{Name: "randomkey", Arity: 1, Flags: FlagReadOnly | FlagAllKeys, Group: "keyspace", Summary: "Returns a random key", Handler: randomKeyCmd},
		&Spec{Name: "rename", Arity: 3, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 2, Summary: "Renames a key, keeping its TTL", Handler: renameCmd},
		&Spec{Name: "renamenx", Arity: 3, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 2, Summary: "Renames a key if the new name is free", Handler: renameCmd},
		&Spec{Name: "copy", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "keyspace", FirstKey: 1, LastKey: 2, Summary: "Copies a value: COPY src dst [REPLACE]", Handler: copyCmd},
		&Spec{Name: "expire", Arity: -3, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Sets a TTL in seconds", Handler: expireCmd},
		&Spec{Name: "pexpire", Arity: -3, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Sets a TTL in milliseconds", Handler: expireCmd},
		&Spec{Name: "expireat", Arity: -3, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Sets an expiry as a unix time in seconds", Handler: expireCmd},
		&Spec{Name: "pexpireat", Arity: -3, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Sets an expiry as a unix time in milliseconds", Handler: expireCmd},
		&Spec{Name: "ttl", Arity: 2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Remaining TTL in seconds", Handler: ttlCmd},
		&Spec{Name: "pttl", Arity: 2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Remaining TTL in milliseconds", Handler: ttlCmd},
		&Spec{Name: "expiretime", Arity: 2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Expiry as a unix time in seconds", Handler: ttlCmd},
		&Spec{Name: "pexpiretime", Arity: 2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Expiry as a unix time in milliseconds", Handler: ttlCmd},
		&Spec{Name: "persist", Arity: 2, Flags: FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: 1, Summary: "Removes the TTL", Handler: persistCmd},
		&Spec{Name: "touch", Arity: -2, Flags: FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: -1, Summary: "Updates the access time of keys", Handler: existsCmd},
		&Spec{Name: "object", Arity: -2, Flags: FlagReadOnly, Group: "keyspace", Summary: "OBJECT ENCODING|FREQ|IDLETIME|REFCOUNT key", Handler: objectCmd,
			KeysFn: func(args []string) []int {
				if len(args) >= 3 {
					return []int{2}
				}
				return nil
			}},
	)
}

func delCmd(x *Ctx) resp.Value {
	n := 0
	for _, k := range x.Keys {
		// Peek first: an expired key must not count as deleted.
		if x.DB().Peek(k) != nil && x.DB().Delete(k) {
			n++
		}
	}
	if n == 0 {
		x.NoChange()
	}
	return resp.Int(int64(n))
}

// existsCmd serves EXISTS and TOUCH: both count the existing keys, and the
// lookup itself updates the access time.
func existsCmd(x *Ctx) resp.Value {
	n := 0
	for _, k := range x.Keys {
		if x.DB().Get(k) != nil {
			n++
		}
	}
	return resp.Int(int64(n))
}

func typeCmd(x *Ctx) resp.Value {
	e := x.DB().Get(x.Keys[0])
	if e == nil {
		return resp.Simple("none")
	}
	return resp.Simple(e.Kind.String())
}

func keysCmd(x *Ctx) resp.Value {
	pattern := x.Arg(1)
	var out []string
	x.DB().ForEach(func(key string, _ *store.Entry) bool {
		if u, ok := x.UserKey(key); ok && glob.Match(pattern, u) {
			out = append(out, u)
		}
		return true
	})
	return resp.Strings(sortStrings(out))
}

// scanOpts are the options shared by SCAN, HSCAN, SSCAN and ZSCAN.
type scanOpts struct {
	match    string
	count    int
	typ      string
	noValues bool
}

func (o scanOpts) matches(s string) bool { return o.match == "" || glob.Match(o.match, s) }

// parseScan reads "cursor [MATCH p] [COUNT n] ..." starting at the cursor's position.
func parseScan(x *Ctx, at int, allowType, allowNoValues bool) (uint64, scanOpts, resp.Value, bool) {
	o := scanOpts{count: 10}
	cursor, err := strconv.ParseUint(x.Arg(at), 10, 64)
	if err != nil {
		return 0, o, resp.Err("ERR invalid cursor"), false
	}
	for i := at + 1; i < x.NArgs(); i++ {
		opt := x.ArgLower(i)
		switch {
		case opt == "match" && i+1 < x.NArgs():
			o.match = x.Arg(i + 1)
			i++
		case opt == "count" && i+1 < x.NArgs():
			n, ok := x.Int(i + 1)
			if !ok {
				return 0, o, ErrNotInt, false
			}
			if n < 1 {
				return 0, o, ErrSyntax, false
			}
			o.count = int(min(n, math.MaxInt32))
			i++
		case opt == "type" && allowType && i+1 < x.NArgs():
			o.typ = x.ArgLower(i + 1)
			i++
		case opt == "novalues" && allowNoValues:
			o.noValues = true
		default:
			return 0, o, ErrSyntax, false
		}
	}
	return cursor, o, resp.Value{}, true
}

func scanReply(cursor uint64, items []resp.Value) resp.Value {
	return resp.Array(resp.Bulk(strconv.FormatUint(cursor, 10)), resp.Array(items...))
}

func scanCmd(x *Ctx) resp.Value {
	cursor, o, bad, ok := parseScan(x, 1, true, false)
	if !ok {
		return bad
	}
	next, keys := x.DB().Scan(cursor, o.count, func(key string, e *store.Entry) bool {
		u, ok := x.UserKey(key)
		return ok && o.matches(u) && (o.typ == "" || e.Kind.String() == o.typ)
	})
	items := make([]resp.Value, len(keys))
	for i, k := range keys {
		u, _ := x.UserKey(k)
		items[i] = resp.Bulk(u)
	}
	return scanReply(next, items)
}

func randomKeyCmd(x *Ctx) resp.Value {
	// The fast path usually hits a key of the caller's namespace; if keys of
	// other namespaces dominate, fall back to reservoir sampling.
	for range 8 {
		k, ok := x.DB().RandomKey()
		if !ok {
			return resp.Null()
		}
		if u, ok := x.UserKey(k); ok {
			return resp.Bulk(u)
		}
	}
	var pick string
	n := 0
	x.DB().ForEach(func(key string, _ *store.Entry) bool {
		if u, ok := x.UserKey(key); ok {
			n++
			if rand.IntN(n) == 0 {
				pick = u
			}
		}
		return true
	})
	if n == 0 {
		return resp.Null()
	}
	return resp.Bulk(pick)
}

func renameCmd(x *Ctx) resp.Value {
	src, dst := x.Keys[0], x.Keys[1]
	nx := x.Name == "renamenx"
	e := x.DB().Get(src)
	if e == nil {
		return ErrNoKey
	}
	if src == dst {
		x.NoChange()
		if nx {
			return resp.Int(0)
		}
		return resp.OK
	}
	if nx && x.DB().Get(dst) != nil {
		x.NoChange()
		return resp.Int(0)
	}
	x.DB().Delete(src)
	x.DB().Put(dst, e) // the entry keeps its ExpireAt
	if nx {
		return resp.Int(1)
	}
	return resp.OK
}

func copyCmd(x *Ctx) resp.Value {
	src, dst := x.Keys[0], x.Keys[1]
	replace := false
	for i := 3; i < x.NArgs(); i++ {
		switch {
		case x.ArgLower(i) == "replace":
			replace = true
		case x.ArgLower(i) == "db" && i+1 < x.NArgs():
			if x.Arg(i+1) != "0" {
				return resp.Err("ERR DB index is out of range")
			}
			i++
		default:
			return ErrSyntax
		}
	}
	if src == dst {
		return resp.Err("ERR source and destination objects are the same")
	}
	e := x.DB().Get(src)
	if e == nil || (!replace && x.DB().Get(dst) != nil) {
		x.NoChange()
		return resp.Int(0)
	}
	c := cloneEntry(x, e)
	c.ExpireAt = e.ExpireAt
	x.DB().Put(dst, c)
	return resp.Int(1)
}

// cloneEntry deep-copies a value so the copy can be modified independently.
func cloneEntry(x *Ctx, e *store.Entry) *store.Entry {
	switch e.Kind {
	case store.KindString:
		return x.DB().NewStringValue(append([]byte(nil), e.Str().Bytes()...))
	case store.KindList:
		d := deque.New()
		for _, s := range e.List().Slice() {
			d.PushBack(s)
		}
		return store.NewEntry(e.Kind, d)
	case store.KindHash:
		h := store.NewHash()
		for f, v := range e.Hash().Map() {
			h.Set(f, v)
		}
		return store.NewEntry(e.Kind, h)
	case store.KindSet:
		s := store.NewSet()
		for _, m := range e.Set().Members() {
			s.Add(m)
		}
		return store.NewEntry(e.Kind, s)
	case store.KindZSet:
		z := skiplist.New()
		e.ZSet().ForEach(func(p skiplist.Pair) bool { z.Add(p.Member, p.Score); return true })
		return store.NewEntry(e.Kind, z)
	case store.KindJSON:
		d, err := jsondoc.Parse(e.JSON().String())
		if err != nil {
			panic(err) // a stored document always re-parses
		}
		return store.NewEntry(e.Kind, d)
	case store.KindTimeSeries:
		old := e.Series()
		s := timeseries.New(old.Retention)
		for k, v := range old.Labels {
			s.Labels[k] = v
		}
		for _, p := range old.Samples() {
			s.Add(p.TS, p.Value)
		}
		return store.NewEntry(e.Kind, s)
	case store.KindVector:
		old := e.Vectors()
		idx := vector.New()
		for _, id := range old.IDs() {
			v, attr, _ := old.Get(id)
			idx.Add(id, append([]float32(nil), v...), attr)
		}
		return store.NewEntry(e.Kind, idx)
	}
	panic("cloneEntry: unknown kind " + e.Kind.String())
}

func expireCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	var nx, xx, gt, lt bool
	for i := 3; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "gt":
			gt = true
		case "lt":
			lt = true
		default:
			return resp.Errf("ERR Unsupported option %s", x.Arg(i))
		}
	}
	if nx && (xx || gt || lt) {
		return resp.Err("ERR NX and XX, GT or LT options at the same time are not compatible")
	}
	if gt && lt {
		return resp.Err("ERR GT and LT options at the same time are not compatible")
	}
	var unit int64 = 1
	if x.Name == "expire" || x.Name == "expireat" {
		unit = 1000
	}
	var base int64
	if x.Name == "expire" || x.Name == "pexpire" {
		base = x.Now()
	}
	at, ok := mulAdd(n, unit, base)
	if !ok {
		return resp.Errf("ERR invalid expire time in '%s' command", x.Name)
	}
	e := x.DB().Get(key)
	if e == nil {
		x.NoChange()
		return resp.Int(0)
	}
	// A key without TTL counts as an infinite TTL for GT/LT.
	cur := e.ExpireAt
	switch {
	case nx && cur != 0, xx && cur == 0,
		gt && (cur == 0 || at <= cur),
		lt && cur != 0 && at >= cur:
		x.NoChange()
		return resp.Int(0)
	}
	setExpireAt(x, key, at)
	return resp.Int(1)
}

// setExpireAt applies an absolute expiry to an existing key and propagates it
// as PEXPIREAT, or as DEL when the time is already past.
func setExpireAt(x *Ctx, key string, at int64) {
	if at <= x.Now() {
		x.DB().Delete(key)
		x.Propagate("DEL", key)
		return
	}
	x.DB().SetExpire(key, at)
	x.Propagate("PEXPIREAT", key, strconv.FormatInt(at, 10))
}

// mulAdd returns n*unit+base, or false on overflow.
func mulAdd(n, unit, base int64) (int64, bool) {
	if n > math.MaxInt64/unit || n < math.MinInt64/unit {
		return 0, false
	}
	n *= unit
	if (base > 0 && n > math.MaxInt64-base) || (base < 0 && n < math.MinInt64-base) {
		return 0, false
	}
	return n + base, true
}

func ttlCmd(x *Ctx) resp.Value {
	e := x.DB().Get(x.Keys[0])
	if e == nil {
		return resp.Int(-2)
	}
	if e.ExpireAt == 0 {
		return resp.Int(-1)
	}
	switch x.Name {
	case "ttl":
		return resp.Int((max(e.ExpireAt-x.Now(), 0) + 500) / 1000)
	case "pttl":
		return resp.Int(max(e.ExpireAt-x.Now(), 0))
	case "expiretime":
		return resp.Int(e.ExpireAt / 1000)
	}
	return resp.Int(e.ExpireAt)
}

func persistCmd(x *Ctx) resp.Value {
	e := x.DB().Get(x.Keys[0])
	if e == nil || e.ExpireAt == 0 {
		x.NoChange()
		return resp.Int(0)
	}
	x.DB().SetExpire(x.Keys[0], 0)
	return resp.Int(1)
}

func objectCmd(x *Ctx) resp.Value {
	sub := x.ArgLower(1)
	if sub == "help" {
		return resp.Strings([]string{
			"OBJECT ENCODING <key>", "OBJECT FREQ <key>", "OBJECT IDLETIME <key>", "OBJECT REFCOUNT <key>",
		})
	}
	if x.NArgs() != 3 {
		return resp.Errf("ERR unknown subcommand or wrong number of arguments for '%s'. Try OBJECT HELP.", x.Arg(1))
	}
	e := x.DB().Peek(x.Keys[0]) // OBJECT must not change the access time it reports
	if e == nil {
		return resp.Null()
	}
	switch sub {
	case "encoding":
		return resp.Bulk(encodingOf(e))
	case "freq":
		return resp.Int(int64(e.Freq()))
	case "idletime":
		return resp.Int(max(time.Now().UnixMilli()-e.LastAccess(), 0) / 1000)
	case "refcount":
		return resp.Int(1)
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try OBJECT HELP.", x.Arg(1))
}

func encodingOf(e *store.Entry) string {
	switch e.Kind {
	case store.KindString:
		return e.Str().Encoding()
	case store.KindHash:
		return e.Hash().Encoding()
	case store.KindSet:
		return e.Set().Encoding()
	case store.KindList:
		return "quicklist"
	case store.KindZSet:
		return "skiplist"
	}
	return e.Kind.String()
}

// --- helpers shared by the data type files ---

// sizer is anything an Entry can hold.
type sizer = interface{ MemSize() int64 }

// getOrCreate returns key's entry, creating an empty one of kind if missing.
// The caller must call x.DB().Updated(key) after filling it, which also
// removes the new entry again if it stayed empty.
func getOrCreate(x *Ctx, key string, kind store.Kind, mk func() sizer) (*store.Entry, bool) {
	e := x.DB().Get(key)
	if e == nil {
		e = store.NewEntry(kind, mk())
		x.DB().Put(key, e)
		return e, true
	}
	return e, e.Kind == kind
}

// numKeys returns a KeysFn for "<numkeys> key ..." where numkeys is at pos;
// fixed lists key positions before it (the destination of ZUNIONSTORE...).
// Malformed arguments yield fewer keys: the handler reports the error.
func numKeys(pos int, fixed ...int) func([]string) []int {
	return func(args []string) []int {
		var out []int
		for _, f := range fixed {
			if f < len(args) {
				out = append(out, f)
			}
		}
		if pos >= len(args) {
			return out
		}
		n, ok := ParseInt(args[pos])
		if !ok || n <= 0 {
			return out
		}
		for i := pos + 1; i < len(args) && int64(i-pos) <= n; i++ {
			out = append(out, i)
		}
		return out
	}
}

// parseNumKeys validates the numkeys argument at pos and returns the key
// names (namespaced) that follow it.
func parseNumKeys(x *Ctx, pos int) ([]string, resp.Value, bool) {
	n, ok := x.Int(pos)
	if !ok {
		return nil, ErrNotInt, false
	}
	if n <= 0 {
		return nil, resp.Err("ERR numkeys should be greater than 0"), false
	}
	if n > int64(x.NArgs()-pos-1) {
		return nil, resp.Err("ERR Number of keys can't be greater than number of args"), false
	}
	return x.Args[pos+1 : pos+1+int(n)], resp.Value{}, true
}

// parseTimeout reads a blocking timeout in seconds (0 = forever).
func parseTimeout(x *Ctx, i int) (time.Duration, resp.Value, bool) {
	f, err := strconv.ParseFloat(x.Arg(i), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, resp.Err("ERR timeout is not a float or out of range"), false
	}
	if f < 0 {
		return 0, resp.Err("ERR timeout is negative"), false
	}
	return time.Duration(f * float64(time.Second)), resp.Value{}, true
}

// userKey converts a stored key back to the form the client sent.
func userKey(x *Ctx, stored string) string {
	u, _ := x.UserKey(stored)
	return u
}
