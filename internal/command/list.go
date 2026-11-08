package command

import (
	"strconv"
	"time"

	"github.com/shamim3696/cache-boy/internal/datatype/deque"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "lpush", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Prepends elements to a list", Handler: pushCmd},
		&Spec{Name: "rpush", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Appends elements to a list", Handler: pushCmd},
		&Spec{Name: "lpushx", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Prepends elements to an existing list", Handler: pushCmd},
		&Spec{Name: "rpushx", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Appends elements to an existing list", Handler: pushCmd},
		&Spec{Name: "lpop", Arity: -2, Flags: FlagWrite, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Removes and returns the first elements", Handler: popCmd},
		&Spec{Name: "rpop", Arity: -2, Flags: FlagWrite, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Removes and returns the last elements", Handler: popCmd},
		&Spec{Name: "llen", Arity: 2, Flags: FlagReadOnly, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Length of a list", Handler: llenCmd},
		&Spec{Name: "lrange", Arity: 4, Flags: FlagReadOnly, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Returns a range of elements", Handler: lrangeCmd},
		&Spec{Name: "lindex", Arity: 3, Flags: FlagReadOnly, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Returns the element at an index", Handler: lindexCmd},
		&Spec{Name: "lset", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Replaces the element at an index", Handler: lsetCmd},
		&Spec{Name: "linsert", Arity: 5, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 1, Summary: "LINSERT key BEFORE|AFTER pivot element", Handler: linsertCmd},
		&Spec{Name: "lrem", Arity: 4, Flags: FlagWrite, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Removes elements equal to a value", Handler: lremCmd},
		&Spec{Name: "ltrim", Arity: 4, Flags: FlagWrite, Group: "list", FirstKey: 1, LastKey: 1, Summary: "Keeps only a range of elements", Handler: ltrimCmd},
		&Spec{Name: "lpos", Arity: -3, Flags: FlagReadOnly, Group: "list", FirstKey: 1, LastKey: 1, Summary: "LPOS key element [RANK r] [COUNT n] [MAXLEN m]", Handler: lposCmd},
		&Spec{Name: "lmove", Arity: 5, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 2, Summary: "LMOVE src dst LEFT|RIGHT LEFT|RIGHT", Handler: lmoveCmd},
		&Spec{Name: "rpoplpush", Arity: 3, Flags: FlagWrite | FlagDenyOOM, Group: "list", FirstKey: 1, LastKey: 2, Summary: "Moves the last element of src to the head of dst", Handler: lmoveCmd},
		&Spec{Name: "lmpop", Arity: -4, Flags: FlagWrite, Group: "list", KeysFn: numKeys(1), Summary: "LMPOP numkeys key ... LEFT|RIGHT [COUNT n]", Handler: lmpopCmd},
		&Spec{Name: "blpop", Arity: -3, Flags: FlagWrite | FlagBlocking, Group: "list", FirstKey: 1, LastKey: -2, Summary: "Blocking LPOP over several keys", Handler: bpopCmd},
		&Spec{Name: "brpop", Arity: -3, Flags: FlagWrite | FlagBlocking, Group: "list", FirstKey: 1, LastKey: -2, Summary: "Blocking RPOP over several keys", Handler: bpopCmd},
		&Spec{Name: "blmove", Arity: 6, Flags: FlagWrite | FlagDenyOOM | FlagBlocking, Group: "list", FirstKey: 1, LastKey: 2, Summary: "Blocking LMOVE", Handler: lmoveCmd},
		&Spec{Name: "brpoplpush", Arity: 4, Flags: FlagWrite | FlagDenyOOM | FlagBlocking, Group: "list", FirstKey: 1, LastKey: 2, Summary: "Blocking RPOPLPUSH", Handler: lmoveCmd},
		&Spec{Name: "blmpop", Arity: -5, Flags: FlagWrite | FlagBlocking, Group: "list", KeysFn: numKeys(2), Summary: "BLMPOP timeout numkeys key ... LEFT|RIGHT [COUNT n]", Handler: lmpopCmd},
	)
}

func newDeque() sizer { return deque.New() }

// getList returns key's list, nil if missing, or false on a wrong type.
func getList(x *Ctx, key string) (*deque.Deque, bool) {
	e, err := x.DB().GetKind(key, store.KindList)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.List(), true
}

// parseSide reads LEFT or RIGHT; true means left.
func parseSide(s string) (left, ok bool) {
	switch s {
	case "left":
		return true, true
	case "right":
		return false, true
	}
	return false, false
}

func sideName(left bool, l, r string) string {
	if left {
		return l
	}
	return r
}

func popN(d *deque.Deque, left bool, n int) []string {
	out := make([]string, 0, min(n, d.Len()))
	for len(out) < n {
		var s string
		var ok bool
		if left {
			s, ok = d.PopFront()
		} else {
			s, ok = d.PopBack()
		}
		if !ok {
			break
		}
		out = append(out, s)
	}
	return out
}

func pushCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	left := x.Name[0] == 'l'
	if x.Name[len(x.Name)-1] == 'x' {
		if d, ok := getList(x, key); !ok {
			return ErrWrongTyp
		} else if d == nil {
			x.NoChange()
			return resp.Int(0)
		}
	}
	e, ok := getOrCreate(x, key, store.KindList, newDeque)
	if !ok {
		return ErrWrongTyp
	}
	d := e.List()
	for _, v := range x.Args[2:] {
		if left {
			d.PushFront(v)
		} else {
			d.PushBack(v)
		}
	}
	x.DB().Updated(key)
	return resp.Int(int64(d.Len()))
}

func popCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	count := -1
	if x.NArgs() > 3 {
		return ErrSyntax
	}
	if x.NArgs() == 3 {
		n, ok := x.Int(2)
		if !ok || n < 0 {
			return resp.Err("ERR value is out of range, must be positive")
		}
		count = int(min(n, 1<<31))
	}
	d, ok := getList(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		x.NoChange()
		if count < 0 {
			return resp.Null()
		}
		return resp.NullArray()
	}
	n := count
	if n < 0 {
		n = 1
	}
	items := popN(d, x.Name == "lpop", n)
	if len(items) == 0 {
		x.NoChange()
	}
	x.DB().Updated(key)
	if count < 0 {
		return resp.Bulk(items[0])
	}
	return resp.Strings(items)
}

func llenCmd(x *Ctx) resp.Value {
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(d.Len()))
}

// intArgs parses consecutive integer arguments starting at i.
func intArgs(x *Ctx, i, n int) ([]int, bool) {
	out := make([]int, n)
	for j := range out {
		v, ok := x.Int(i + j)
		if !ok {
			return nil, false
		}
		out[j] = clampInt(v)
	}
	return out, true
}

// clampInt converts to int without wrapping on 32-bit platforms.
func clampInt(v int64) int {
	const maxInt = int64(^uint(0) >> 1)
	return int(max(min(v, maxInt), -maxInt))
}

func lrangeCmd(x *Ctx) resp.Value {
	r, ok := intArgs(x, 2, 2)
	if !ok {
		return ErrNotInt
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return resp.Array()
	}
	return resp.Strings(d.Range(r[0], r[1]))
}

func lindexCmd(x *Ctx) resp.Value {
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return resp.Null()
	}
	i, ok := d.Index(clampInt(n))
	if !ok {
		return resp.Null()
	}
	return resp.Bulk(d.At(i))
}

func lsetCmd(x *Ctx) resp.Value {
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return ErrNoKey
	}
	i, ok := d.Index(clampInt(n))
	if !ok {
		return resp.Err("ERR index out of range")
	}
	d.Set(i, x.Arg(3))
	x.DB().Updated(x.Keys[0])
	return resp.OK
}

func linsertCmd(x *Ctx) resp.Value {
	var after bool
	switch x.ArgLower(2) {
	case "before":
	case "after":
		after = true
	default:
		return ErrSyntax
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		x.NoChange()
		return resp.Int(0)
	}
	i := d.Find(x.Arg(3))
	if i < 0 {
		x.NoChange()
		return resp.Int(-1)
	}
	if after {
		i++
	}
	d.Insert(i, x.Arg(4))
	x.DB().Updated(x.Keys[0])
	return resp.Int(int64(d.Len()))
}

func lremCmd(x *Ctx) resp.Value {
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	removed := 0
	if d != nil {
		removed = d.Remove(x.Arg(3), clampInt(n))
	}
	if removed == 0 {
		x.NoChange()
		return resp.Int(0)
	}
	x.DB().Updated(x.Keys[0])
	return resp.Int(int64(removed))
}

func ltrimCmd(x *Ctx) resp.Value {
	r, ok := intArgs(x, 2, 2)
	if !ok {
		return ErrNotInt
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		x.NoChange()
		return resp.OK
	}
	before := d.Len()
	d.Trim(r[0], r[1])
	if d.Len() == before {
		x.NoChange()
	}
	x.DB().Updated(x.Keys[0])
	return resp.OK
}

func lposCmd(x *Ctx) resp.Value {
	rank, count, maxLen := 1, -1, 0
	for i := 3; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		n, ok := x.Int(i + 1)
		if !ok {
			return ErrNotInt
		}
		switch x.ArgLower(i) {
		case "rank":
			if n == 0 {
				return resp.Err("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list")
			}
			if n < -(1<<31) || n > 1<<31 {
				return resp.Err("ERR value is out of range")
			}
			rank = int(n)
		case "count":
			if n < 0 {
				return resp.Err("ERR COUNT can't be negative")
			}
			count = clampInt(n)
		case "maxlen":
			if n < 0 {
				return resp.Err("ERR MAXLEN can't be negative")
			}
			maxLen = clampInt(n)
		default:
			return ErrSyntax
		}
	}
	d, ok := getList(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	var found []resp.Value
	if d != nil {
		want, skip := count, rank-1
		if count == 0 {
			want = d.Len()
		} else if count < 0 {
			want = 1
		}
		step, i := 1, 0
		if rank < 0 {
			step, i, skip = -1, d.Len()-1, -rank-1
		}
		for scanned := 0; i >= 0 && i < d.Len() && len(found) < want; i += step {
			if maxLen > 0 && scanned >= maxLen {
				break
			}
			scanned++
			if d.At(i) != x.Arg(2) {
				continue
			}
			if skip > 0 {
				skip--
				continue
			}
			found = append(found, resp.Int(int64(i)))
		}
	}
	if count >= 0 {
		return resp.Array(found...)
	}
	if len(found) == 0 {
		return resp.Null()
	}
	return found[0]
}

// lmoveCmd serves LMOVE, RPOPLPUSH, BLMOVE and BRPOPLPUSH.
func lmoveCmd(x *Ctx) resp.Value {
	src, dst := x.Keys[0], x.Keys[1]
	from, to := false, true // RPOPLPUSH: right to left
	timeoutAt := 3
	if x.Name == "lmove" || x.Name == "blmove" {
		var ok1, ok2 bool
		from, ok1 = parseSide(x.ArgLower(3))
		to, ok2 = parseSide(x.ArgLower(4))
		if !ok1 || !ok2 {
			return ErrSyntax
		}
		timeoutAt = 5
	}
	blocking := x.Name[0] == 'b'
	var timeout time.Duration
	if blocking {
		var bad resp.Value
		var ok bool
		if timeout, bad, ok = parseTimeout(x, timeoutAt); !ok {
			return bad
		}
	}
	s, moved, bad, ok := lmove(x, src, dst, from, to)
	switch {
	case !ok:
		return bad
	case moved:
		if blocking {
			x.Propagate("LMOVE", src, dst, sideName(from, "LEFT", "RIGHT"), sideName(to, "LEFT", "RIGHT"))
		}
		return resp.Bulk(s)
	case blocking && !x.TimedOut:
		return x.Block(timeout, src)
	}
	x.NoChange()
	if blocking {
		return resp.NullArray()
	}
	return resp.Null()
}

// lmove pops from one end of src and pushes onto one end of dst.
func lmove(x *Ctx, src, dst string, fromLeft, toLeft bool) (string, bool, resp.Value, bool) {
	sd, ok := getList(x, src)
	if !ok {
		return "", false, ErrWrongTyp, false
	}
	if _, ok := getList(x, dst); !ok {
		return "", false, ErrWrongTyp, false
	}
	if sd == nil {
		return "", false, resp.Value{}, true
	}
	v := popN(sd, fromLeft, 1)[0]
	// src is not cleaned up until after the push, so a rotation of a
	// one-element list (src == dst) still finds its entry.
	e, _ := getOrCreate(x, dst, store.KindList, newDeque)
	if toLeft {
		e.List().PushFront(v)
	} else {
		e.List().PushBack(v)
	}
	x.DB().Updated(src)
	x.DB().Updated(dst)
	return v, true, resp.Value{}, true
}

// lmpopCmd serves LMPOP and BLMPOP.
func lmpopCmd(x *Ctx) resp.Value {
	blocking := x.Name == "blmpop"
	pos := 1
	var timeout time.Duration
	if blocking {
		var bad resp.Value
		var ok bool
		if timeout, bad, ok = parseTimeout(x, 1); !ok {
			return bad
		}
		pos = 2
	}
	keys, bad, ok := parseNumKeys(x, pos)
	if !ok {
		return bad
	}
	i := pos + 1 + len(keys)
	left, ok := parseSide(x.ArgLower(i))
	if !ok {
		return ErrSyntax
	}
	count, bad, ok := parseCountOpt(x, i+1)
	if !ok {
		return bad
	}
	for _, k := range keys {
		d, ok := getList(x, k)
		if !ok {
			return ErrWrongTyp
		}
		if d == nil {
			continue
		}
		items := popN(d, left, count)
		x.DB().Updated(k)
		x.Propagate(sideName(left, "LPOP", "RPOP"), k, strconv.Itoa(len(items)))
		return resp.Array(resp.Bulk(userKey(x, k)), resp.Strings(items))
	}
	if blocking && !x.TimedOut {
		return x.Block(timeout, keys...)
	}
	x.NoChange()
	return resp.NullArray()
}

// parseCountOpt reads an optional trailing "COUNT n" (n > 0) at i; default 1.
func parseCountOpt(x *Ctx, i int) (int, resp.Value, bool) {
	switch {
	case i == x.NArgs():
		return 1, resp.Value{}, true
	case i+2 != x.NArgs() || x.ArgLower(i) != "count":
		return 0, ErrSyntax, false
	}
	n, ok := x.Int(i + 1)
	if !ok || n <= 0 {
		return 0, resp.Err("ERR count should be greater than 0"), false
	}
	return clampInt(n), resp.Value{}, true
}

// bpopCmd serves BLPOP and BRPOP.
func bpopCmd(x *Ctx) resp.Value {
	t, bad, ok := parseTimeout(x, x.NArgs()-1)
	if !ok {
		return bad
	}
	left := x.Name == "blpop"
	for _, k := range x.Keys {
		d, ok := getList(x, k)
		if !ok {
			return ErrWrongTyp
		}
		if d == nil {
			continue
		}
		v := popN(d, left, 1)[0]
		x.DB().Updated(k)
		x.Propagate(sideName(left, "LPOP", "RPOP"), k)
		return resp.Array(resp.Bulk(userKey(x, k)), resp.Bulk(v))
	}
	if x.TimedOut {
		x.NoChange()
		return resp.NullArray()
	}
	return x.Block(t)
}
