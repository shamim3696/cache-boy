package command

import (
	"math"
	"strconv"

	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

const maxStringLen = 512 << 20 // Redis' proto-max-bulk-len default

func init() {
	Register(
		&Spec{Name: "get", Arity: 2, Flags: FlagReadOnly, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Returns the value of a key", Handler: getCmd},
		&Spec{Name: "set", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "SET key value [EX|PX|EXAT|PXAT|KEEPTTL] [NX|XX] [GET] [TAGS tag ...]", Handler: setCmd},
		&Spec{Name: "setnx", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Sets a key only if it doesn't exist", Handler: setnxCmd},
		&Spec{Name: "setex", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Sets a key with a TTL in seconds", Handler: setexCmd},
		&Spec{Name: "psetex", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Sets a key with a TTL in milliseconds", Handler: setexCmd},
		&Spec{Name: "getset", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Sets a key and returns the old value", Handler: getsetCmd},
		&Spec{Name: "getdel", Arity: 2, Flags: FlagWrite, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Returns a value and deletes the key", Handler: getdelCmd},
		&Spec{Name: "getex", Arity: -2, Flags: FlagWrite, Group: "string", FirstKey: 1, LastKey: 1, Summary: "GETEX key [EX|PX|EXAT|PXAT|PERSIST]", Handler: getexCmd},
		&Spec{Name: "mget", Arity: -2, Flags: FlagReadOnly, Group: "string", FirstKey: 1, LastKey: -1, Summary: "Returns the values of several keys", Handler: mgetCmd},
		&Spec{Name: "mset", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: -1, KeyStep: 2, Summary: "Sets several keys", Handler: msetCmd},
		&Spec{Name: "msetnx", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: -1, KeyStep: 2, Summary: "Sets several keys if none exists", Handler: msetCmd},
		&Spec{Name: "append", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Appends to a string", Handler: appendCmd},
		&Spec{Name: "strlen", Arity: 2, Flags: FlagReadOnly, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Length of a string", Handler: strlenCmd},
		&Spec{Name: "incr", Arity: 2, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Adds 1 to an integer", Handler: incrCmd},
		&Spec{Name: "decr", Arity: 2, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Subtracts 1 from an integer", Handler: incrCmd},
		&Spec{Name: "incrby", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Adds to an integer", Handler: incrCmd},
		&Spec{Name: "decrby", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Subtracts from an integer", Handler: incrCmd},
		&Spec{Name: "incrbyfloat", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Adds to a float", Handler: incrbyfloatCmd},
		&Spec{Name: "getrange", Arity: 4, Flags: FlagReadOnly, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Returns a substring", Handler: getrangeCmd},
		&Spec{Name: "substr", Arity: 4, Flags: FlagReadOnly, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Returns a substring (old name of GETRANGE)", Handler: getrangeCmd},
		&Spec{Name: "setrange", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Summary: "Overwrites part of a string", Handler: setrangeCmd},
	)
}

// getString returns key's string entry, nil if missing, or false on a wrong type.
func getString(x *Ctx, key string) (*store.Entry, bool) {
	e, err := x.DB().GetKind(key, store.KindString)
	return e, err == nil
}

// replaceString swaps the bytes of an existing string entry, keeping its TTL
// and access statistics.
func replaceString(x *Ctx, key string, e *store.Entry, b []byte) {
	e.Value = store.NewString(b, int(x.DB().CompressThreshold.Load()))
	x.DB().Updated(key)
}

// putString stores b at key, creating or overwriting it, with TTL at (0 = none).
func putString(x *Ctx, key string, b []byte, at int64) {
	e := x.DB().NewStringValue(b)
	e.ExpireAt = at
	x.DB().Put(key, e)
}

func getCmd(x *Ctx) resp.Value {
	e, ok := getString(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if e == nil {
		return resp.Null()
	}
	return resp.Bytes(e.Str().Bytes())
}

// expiryOpt is a parsed EX/PX/EXAT/PXAT option.
type expiryOpt struct {
	at       int64 // absolute unix ms
	set      bool
	relative bool // EX/PX: propagate as PXAT so replay doesn't extend the TTL
}

// parseExpiry handles one EX/PX/EXAT/PXAT option whose value is at i.
func parseExpiry(x *Ctx, opt string, i int) (expiryOpt, resp.Value, bool) {
	n, ok := x.Int(i)
	if !ok {
		return expiryOpt{}, ErrNotInt, false
	}
	if n <= 0 {
		return expiryOpt{}, resp.Errf("ERR invalid expire time in '%s' command", x.Name), false
	}
	var unit, base int64 = 1, 0
	if opt == "ex" || opt == "exat" {
		unit = 1000
	}
	if opt == "ex" || opt == "px" {
		base = x.Now()
	}
	at, ok := mulAdd(n, unit, base)
	if !ok {
		return expiryOpt{}, resp.Errf("ERR invalid expire time in '%s' command", x.Name), false
	}
	return expiryOpt{at: at, set: true, relative: base != 0}, resp.Value{}, true
}

func setCmd(x *Ctx) resp.Value {
	key, val := x.Keys[0], x.Arg(2)
	var nx, xx, get, keepTTL bool
	var exp expiryOpt
	var tags []string
	for i := 3; i < x.NArgs(); i++ {
		switch opt := x.ArgLower(i); opt {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "get":
			get = true
		case "keepttl":
			if exp.set {
				return ErrSyntax
			}
			keepTTL = true
		case "ex", "px", "exat", "pxat":
			if exp.set || keepTTL || i+1 >= x.NArgs() {
				return ErrSyntax
			}
			var bad resp.Value
			var ok bool
			if exp, bad, ok = parseExpiry(x, opt, i+1); !ok {
				return bad
			}
			i++
		case "tags":
			tags = x.Args[i+1:]
			if len(tags) == 0 {
				return ErrSyntax
			}
			i = x.NArgs()
		default:
			return ErrSyntax
		}
	}
	if nx && xx {
		return ErrSyntax
	}
	old := x.DB().Get(key)
	reply := resp.OK
	if get {
		switch {
		case old == nil:
			reply = resp.Null()
		case old.Kind != store.KindString:
			return ErrWrongTyp
		default:
			reply = resp.Bytes(old.Str().Bytes())
		}
	}
	if nx && old != nil || xx && old == nil {
		x.NoChange()
		if get {
			return reply
		}
		return resp.Null()
	}
	var at int64
	switch {
	case exp.set:
		at = exp.at
	case keepTTL && old != nil:
		at = old.ExpireAt
	}
	putString(x, key, []byte(val), at)
	if len(tags) > 0 {
		x.E.Tags.Tag(key, tags...)
	}

	// Replicas get the outcome only: options already resolved, TTL absolute.
	prop := []string{"SET", key, val}
	if exp.set {
		prop = append(prop, "PXAT", strconv.FormatInt(at, 10))
	} else if keepTTL {
		prop = append(prop, "KEEPTTL")
	}
	if len(tags) > 0 {
		prop = append(append(prop, "TAGS"), tags...)
	}
	x.Propagate(prop...)
	return reply
}

func setnxCmd(x *Ctx) resp.Value {
	if x.DB().Get(x.Keys[0]) != nil {
		x.NoChange()
		return resp.Int(0)
	}
	putString(x, x.Keys[0], []byte(x.Arg(2)), 0)
	return resp.Int(1)
}

func setexCmd(x *Ctx) resp.Value {
	opt := "ex"
	if x.Name == "psetex" {
		opt = "px"
	}
	exp, bad, ok := parseExpiry(x, opt, 2)
	if !ok {
		return bad
	}
	putString(x, x.Keys[0], []byte(x.Arg(3)), exp.at)
	x.Propagate("SET", x.Keys[0], x.Arg(3), "PXAT", strconv.FormatInt(exp.at, 10))
	return resp.OK
}

func getsetCmd(x *Ctx) resp.Value {
	old, ok := getString(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	reply := resp.Null()
	if old != nil {
		reply = resp.Bytes(old.Str().Bytes())
	}
	putString(x, x.Keys[0], []byte(x.Arg(2)), 0)
	return reply
}

func getdelCmd(x *Ctx) resp.Value {
	e, ok := getString(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if e == nil {
		x.NoChange()
		return resp.Null()
	}
	x.DB().Delete(x.Keys[0])
	return resp.Bytes(e.Str().Bytes())
}

func getexCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	var exp expiryOpt
	persist := false
	for i := 2; i < x.NArgs(); i++ {
		switch opt := x.ArgLower(i); opt {
		case "persist":
			if exp.set || persist {
				return ErrSyntax
			}
			persist = true
		case "ex", "px", "exat", "pxat":
			if exp.set || persist || i+1 >= x.NArgs() {
				return ErrSyntax
			}
			var bad resp.Value
			var ok bool
			if exp, bad, ok = parseExpiry(x, opt, i+1); !ok {
				return bad
			}
			i++
		default:
			return ErrSyntax
		}
	}
	e, ok := getString(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if e == nil {
		x.NoChange()
		return resp.Null()
	}
	reply := resp.Bytes(e.Str().Bytes())
	switch {
	case exp.set:
		setExpireAt(x, key, exp.at)
	case persist && e.ExpireAt != 0:
		x.DB().SetExpire(key, 0)
		x.Propagate("PERSIST", key)
	default:
		x.NoChange()
	}
	return reply
}

func mgetCmd(x *Ctx) resp.Value {
	out := make([]resp.Value, len(x.Keys))
	for i, k := range x.Keys {
		if e := x.DB().Get(k); e != nil && e.Kind == store.KindString {
			out[i] = resp.Bytes(e.Str().Bytes())
		} else {
			out[i] = resp.Null()
		}
	}
	return resp.Array(out...)
}

// msetCmd serves MSET and MSETNX.
func msetCmd(x *Ctx) resp.Value {
	if x.NArgs()%2 != 1 {
		return wrongArgs(x.Name)
	}
	nx := x.Name == "msetnx"
	if nx {
		for _, k := range x.Keys {
			if x.DB().Get(k) != nil {
				x.NoChange()
				return resp.Int(0)
			}
		}
	}
	for i := 1; i < x.NArgs(); i += 2 {
		putString(x, x.Arg(i), []byte(x.Arg(i+1)), 0)
	}
	if nx {
		return resp.Int(1)
	}
	return resp.OK
}

func appendCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	e, ok := getString(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if e == nil {
		putString(x, key, []byte(x.Arg(2)), 0)
		return resp.Int(int64(len(x.Arg(2))))
	}
	if e.Str().Len()+len(x.Arg(2)) > maxStringLen {
		return resp.Err("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	}
	old := e.Str().Bytes()
	b := make([]byte, 0, len(old)+len(x.Arg(2)))
	b = append(append(b, old...), x.Arg(2)...)
	replaceString(x, key, e, b)
	return resp.Int(int64(len(b)))
}

func strlenCmd(x *Ctx) resp.Value {
	e, ok := getString(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if e == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(e.Str().Len()))
}

// incrCmd serves INCR, DECR, INCRBY and DECRBY.
func incrCmd(x *Ctx) resp.Value {
	var delta int64 = 1
	if x.NArgs() == 3 {
		var ok bool
		if delta, ok = x.Int(2); !ok {
			return ErrNotInt
		}
	}
	if x.Name == "decr" || x.Name == "decrby" {
		if delta == math.MinInt64 {
			return resp.Err("ERR decrement would overflow")
		}
		delta = -delta
	}
	key := x.Keys[0]
	e, ok := getString(x, key)
	if !ok {
		return ErrWrongTyp
	}
	var cur int64
	if e != nil {
		if cur, ok = ParseInt(string(e.Str().Bytes())); !ok {
			return ErrNotInt
		}
	}
	if delta > 0 && cur > math.MaxInt64-delta || delta < 0 && cur < math.MinInt64-delta {
		return ErrOverflow
	}
	cur += delta
	b := []byte(strconv.FormatInt(cur, 10))
	if e == nil {
		putString(x, key, b, 0)
	} else {
		replaceString(x, key, e, b)
	}
	return resp.Int(cur)
}

func incrbyfloatCmd(x *Ctx) resp.Value {
	delta, ok := x.Float(2)
	if !ok {
		return ErrNotFloat
	}
	key := x.Keys[0]
	e, ok := getString(x, key)
	if !ok {
		return ErrWrongTyp
	}
	var cur float64
	if e != nil {
		if cur, ok = ParseFloat(string(e.Str().Bytes())); !ok {
			return ErrNotFloat
		}
	}
	cur += delta
	if math.IsNaN(cur) || math.IsInf(cur, 0) {
		return resp.Err("ERR increment would produce NaN or Infinity")
	}
	s := formatIncrFloat(cur)
	if e == nil {
		putString(x, key, []byte(s), 0)
	} else {
		replaceString(x, key, e, []byte(s))
	}
	// Float rounding may differ elsewhere: replicate the result, not the delta.
	x.Propagate("SET", key, s, "KEEPTTL")
	return resp.Bulk(s)
}

// formatIncrFloat formats an INCRBYFLOAT result. Redis adds in long double,
// so 1.1+2.2 prints as 3.3; rounding to 15 significant digits hides the same
// float64 noise.
func formatIncrFloat(f float64) string {
	r, _ := strconv.ParseFloat(strconv.FormatFloat(f, 'g', 15, 64), 64)
	return strconv.FormatFloat(r, 'f', -1, 64)
}

func getrangeCmd(x *Ctx) resp.Value {
	start, ok1 := x.Int(2)
	end, ok2 := x.Int(3)
	if !ok1 || !ok2 {
		return ErrNotInt
	}
	e, ok := getString(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if e == nil {
		return resp.Bulk("")
	}
	b := e.Str().Bytes()
	n := int64(len(b))
	if start < 0 && end < 0 && start > end {
		return resp.Bulk("")
	}
	if start < 0 {
		start = max(n+start, 0)
	}
	if end < 0 {
		end = max(n+end, 0)
	}
	end = min(end, n-1)
	if start > end || n == 0 {
		return resp.Bulk("")
	}
	return resp.Bytes(b[start : end+1])
}

func setrangeCmd(x *Ctx) resp.Value {
	off, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	if off < 0 {
		return resp.Err("ERR offset is out of range")
	}
	val := x.Arg(3)
	if off+int64(len(val)) > maxStringLen {
		return resp.Err("ERR string exceeds maximum allowed size (proto-max-bulk-len)")
	}
	key := x.Keys[0]
	e, ok := getString(x, key)
	if !ok {
		return ErrWrongTyp
	}
	var old []byte
	if e != nil {
		old = e.Str().Bytes()
	}
	if len(val) == 0 {
		x.NoChange()
		return resp.Int(int64(len(old)))
	}
	b := make([]byte, max(len(old), int(off)+len(val)))
	copy(b, old)
	copy(b[off:], val)
	if e == nil {
		putString(x, key, b, 0)
	} else {
		replaceString(x, key, e, b)
	}
	return resp.Int(int64(len(b)))
}
