package command

import (
	"math"
	"sort"
	"strings"

	"github.com/shamim3696/cache-boy/internal/datatype/skiplist"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "zadd", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...", Handler: zaddCmd},
		&Spec{Name: "zincrby", Arity: 4, Flags: FlagWrite | FlagDenyOOM, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Adds to a member's score", Handler: zincrbyCmd},
		&Spec{Name: "zrem", Arity: -3, Flags: FlagWrite, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Removes members", Handler: zremCmd},
		&Spec{Name: "zscore", Arity: 3, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Returns a member's score", Handler: zscoreCmd},
		&Spec{Name: "zmscore", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Returns several members' scores", Handler: zscoreCmd},
		&Spec{Name: "zcard", Arity: 2, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Number of members", Handler: zcardCmd},
		&Spec{Name: "zcount", Arity: 4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Counts members in a score range", Handler: zcountCmd},
		&Spec{Name: "zlexcount", Arity: 4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Counts members in a lexicographic range", Handler: zcountCmd},
		&Spec{Name: "zrank", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "ZRANK key member [WITHSCORE]", Handler: zrankCmd},
		&Spec{Name: "zrevrank", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "ZREVRANK key member [WITHSCORE]", Handler: zrankCmd},
		&Spec{Name: "zrange", Arity: -4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "ZRANGE key start stop [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]", Handler: zrangeCmd},
		&Spec{Name: "zrangestore", Arity: -5, Flags: FlagWrite | FlagDenyOOM, Group: "sortedset", FirstKey: 1, LastKey: 2, Summary: "Stores a ZRANGE result", Handler: zrangeCmd},
		&Spec{Name: "zrevrange", Arity: -4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Members by rank, highest first", Handler: zrangeCmd},
		&Spec{Name: "zrangebyscore", Arity: -4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Members in a score range", Handler: zrangeCmd},
		&Spec{Name: "zrevrangebyscore", Arity: -4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Members in a score range, highest first", Handler: zrangeCmd},
		&Spec{Name: "zrangebylex", Arity: -4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Members in a lexicographic range", Handler: zrangeCmd},
		&Spec{Name: "zrevrangebylex", Arity: -4, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Members in a lexicographic range, reversed", Handler: zrangeCmd},
		&Spec{Name: "zremrangebyrank", Arity: 4, Flags: FlagWrite, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Removes members in a rank range", Handler: zremrangeCmd},
		&Spec{Name: "zremrangebyscore", Arity: 4, Flags: FlagWrite, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Removes members in a score range", Handler: zremrangeCmd},
		&Spec{Name: "zremrangebylex", Arity: 4, Flags: FlagWrite, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Removes members in a lexicographic range", Handler: zremrangeCmd},
		&Spec{Name: "zpopmin", Arity: -2, Flags: FlagWrite, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Removes and returns the lowest-scored members", Handler: zpopCmd},
		&Spec{Name: "zpopmax", Arity: -2, Flags: FlagWrite, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "Removes and returns the highest-scored members", Handler: zpopCmd},
		&Spec{Name: "bzpopmin", Arity: -3, Flags: FlagWrite | FlagBlocking, Group: "sortedset", FirstKey: 1, LastKey: -2, Summary: "Blocking ZPOPMIN over several keys", Handler: bzpopCmd},
		&Spec{Name: "bzpopmax", Arity: -3, Flags: FlagWrite | FlagBlocking, Group: "sortedset", FirstKey: 1, LastKey: -2, Summary: "Blocking ZPOPMAX over several keys", Handler: bzpopCmd},
		&Spec{Name: "zunionstore", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "sortedset", KeysFn: numKeys(2, 1), Summary: "ZUNIONSTORE dst numkeys key ... [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]", Handler: zsetOpCmd},
		&Spec{Name: "zinterstore", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "sortedset", KeysFn: numKeys(2, 1), Summary: "ZINTERSTORE dst numkeys key ... [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX]", Handler: zsetOpCmd},
		&Spec{Name: "zdiffstore", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "sortedset", KeysFn: numKeys(2, 1), Summary: "ZDIFFSTORE dst numkeys key ...", Handler: zsetOpCmd},
		&Spec{Name: "zunion", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", KeysFn: numKeys(1), Summary: "ZUNION numkeys key ... [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]", Handler: zsetOpCmd},
		&Spec{Name: "zinter", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", KeysFn: numKeys(1), Summary: "ZINTER numkeys key ... [WEIGHTS w ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]", Handler: zsetOpCmd},
		&Spec{Name: "zdiff", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", KeysFn: numKeys(1), Summary: "ZDIFF numkeys key ... [WITHSCORES]", Handler: zsetOpCmd},
		&Spec{Name: "zrandmember", Arity: -2, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "ZRANDMEMBER key [count [WITHSCORES]]", Handler: zrandmemberCmd},
		&Spec{Name: "zscan", Arity: -3, Flags: FlagReadOnly, Group: "sortedset", FirstKey: 1, LastKey: 1, Summary: "ZSCAN key cursor [MATCH p] [COUNT n]", Handler: zscanCmd},
	)
}

func newZSet() sizer { return skiplist.New() }

// getZSet returns key's sorted set, nil if missing, or false on a wrong type.
func getZSet(x *Ctx, key string) (*skiplist.ZSet, bool) {
	e, err := x.DB().GetKind(key, store.KindZSet)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.ZSet(), true
}

var (
	errScoreRange = resp.Err("ERR min or max is not a float")
	errLexRange   = resp.Err("ERR min or max not valid string range item")
	errNaNScore   = resp.Err("ERR resulting score is not a number (NaN)")
)

// parseScoreBound parses "1.5", "(1.5", "-inf" or "+inf".
func parseScoreBound(s string) (skiplist.Bound, bool) {
	var b skiplist.Bound
	if strings.HasPrefix(s, "(") {
		b.Exclusive, s = true, s[1:]
	}
	var ok bool
	b.Value, ok = ParseFloat(s)
	return b, ok
}

// parseLexBound parses "-", "+", "[member" or "(member".
func parseLexBound(s string) (skiplist.LexBound, bool) {
	switch {
	case s == "-":
		return skiplist.LexBound{Inf: -1}, true
	case s == "+":
		return skiplist.LexBound{Inf: 1}, true
	case strings.HasPrefix(s, "["):
		return skiplist.LexBound{Value: s[1:]}, true
	case strings.HasPrefix(s, "("):
		return skiplist.LexBound{Value: s[1:], Exclusive: true}, true
	}
	return skiplist.LexBound{}, false
}

// pairsReply is a flat [member, score, ...] array (scores as doubles), or
// just the members.
func pairsReply(ps []skiplist.Pair, withScores bool) resp.Value {
	out := make([]resp.Value, 0, len(ps)*2)
	for _, p := range ps {
		out = append(out, resp.Bulk(p.Member))
		if withScores {
			out = append(out, resp.Double(p.Score))
		}
	}
	return resp.Array(out...)
}

func zaddCmd(x *Ctx) resp.Value {
	var nx, xx, gt, lt, ch, incr bool
	i := 2
opts:
	for ; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "gt":
			gt = true
		case "lt":
			lt = true
		case "ch":
			ch = true
		case "incr":
			incr = true
		default:
			break opts
		}
	}
	n := x.NArgs() - i
	switch {
	case n == 0 || n%2 != 0:
		return ErrSyntax
	case nx && xx:
		return resp.Err("ERR XX and NX options at the same time are not compatible")
	case gt && lt || nx && (gt || lt):
		return resp.Err("ERR GT, LT, and/or NX options at the same time are not compatible")
	case incr && n != 2:
		return resp.Err("ERR INCR option supports a single increment-element pair")
	}
	scores := make([]float64, n/2)
	for j := range scores {
		var ok bool
		if scores[j], ok = x.Float(i + 2*j); !ok {
			return ErrNotFloat
		}
	}

	key := x.Keys[0]
	if z, ok := getZSet(x, key); !ok {
		return ErrWrongTyp
	} else if z == nil && xx {
		x.NoChange()
		if incr {
			return resp.Null()
		}
		return resp.Int(0)
	}
	e, _ := getOrCreate(x, key, store.KindZSet, newZSet)
	z := e.ZSet()
	added, updated := 0, 0
	var result float64
	applied := false
	for j, score := range scores {
		m := x.Arg(i + 2*j + 1)
		cur, exists := z.Score(m)
		if exists {
			if nx {
				continue
			}
			if incr {
				score += cur
				if math.IsNaN(score) {
					x.DB().Updated(key)
					return errNaNScore
				}
			}
			if gt && score <= cur || lt && score >= cur {
				continue
			}
			if score != cur {
				z.Add(m, score)
				updated++
			}
		} else {
			if xx {
				continue
			}
			z.Add(m, score)
			added++
		}
		result, applied = score, true
	}
	x.DB().Updated(key)
	if added+updated == 0 {
		x.NoChange()
	}
	if incr {
		if !applied {
			return resp.Null()
		}
		return resp.Double(result)
	}
	if ch {
		return resp.Int(int64(added + updated))
	}
	return resp.Int(int64(added))
}

func zincrbyCmd(x *Ctx) resp.Value {
	delta, ok := x.Float(2)
	if !ok {
		return ErrNotFloat
	}
	key, m := x.Keys[0], x.Arg(3)
	e, ok := getOrCreate(x, key, store.KindZSet, newZSet)
	if !ok {
		return ErrWrongTyp
	}
	cur, _ := e.ZSet().Score(m)
	score := cur + delta
	if math.IsNaN(score) {
		x.DB().Updated(key)
		return errNaNScore
	}
	e.ZSet().Add(m, score)
	x.DB().Updated(key)
	return resp.Double(score)
}

func zremCmd(x *Ctx) resp.Value {
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	n := 0
	if z != nil {
		for _, m := range x.Args[2:] {
			if z.Remove(m) {
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

// zscoreCmd serves ZSCORE and ZMSCORE.
func zscoreCmd(x *Ctx) resp.Value {
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	out := make([]resp.Value, 0, x.NArgs()-2)
	for _, m := range x.Args[2:] {
		if s, ok := zscore(z, m); ok {
			out = append(out, resp.Double(s))
		} else {
			out = append(out, resp.Null())
		}
	}
	if x.Name == "zscore" {
		return out[0]
	}
	return resp.Array(out...)
}

func zscore(z *skiplist.ZSet, m string) (float64, bool) {
	if z == nil {
		return 0, false
	}
	return z.Score(m)
}

func zcardCmd(x *Ctx) resp.Value {
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if z == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(z.Len()))
}

// zcountCmd serves ZCOUNT and ZLEXCOUNT.
func zcountCmd(x *Ctx) resp.Value {
	r := zrange{by: 's', start: x.Arg(2), stop: x.Arg(3), count: -1}
	if x.Name == "zlexcount" {
		r.by = 'l'
	}
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if r.by == 's' {
		from, ok1 := parseScoreBound(r.start)
		to, ok2 := parseScoreBound(r.stop)
		if !ok1 || !ok2 {
			return errScoreRange
		}
		if z == nil {
			return resp.Int(0)
		}
		return resp.Int(int64(z.CountByScore(from, to)))
	}
	ps, bad, ok := r.run(z)
	if !ok {
		return bad
	}
	return resp.Int(int64(len(ps)))
}

func zrankCmd(x *Ctx) resp.Value {
	withScore := false
	switch {
	case x.NArgs() == 4 && x.ArgLower(3) == "withscore":
		withScore = true
	case x.NArgs() != 3:
		return ErrSyntax
	}
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	var rank int
	found := z != nil
	if found {
		rank, found = z.Rank(x.Arg(2), x.Name == "zrevrank")
	}
	switch {
	case !found && withScore:
		return resp.NullArray()
	case !found:
		return resp.Null()
	case withScore:
		s, _ := z.Score(x.Arg(2))
		return resp.Array(resp.Int(int64(rank)), resp.Double(s))
	}
	return resp.Int(int64(rank))
}

// zrange is a parsed range query shared by the ZRANGE family, ZRANGESTORE,
// ZREMRANGEBY* and ZLEXCOUNT.
type zrange struct {
	by          byte   // 'r' rank, 's' score, 'l' lex
	rev         bool   // with score/lex, start is then the upper bound
	start, stop string // the two range arguments, as given
	offset      int
	count       int // < 0: no limit
	limit       bool
	withScores  bool
}

// zrangeModes holds the fixed part of each command's query.
var zrangeModes = map[string]zrange{
	"zrange":           {by: 'r'},
	"zrangestore":      {by: 'r'},
	"zrevrange":        {by: 'r', rev: true},
	"zrangebyscore":    {by: 's'},
	"zrevrangebyscore": {by: 's', rev: true},
	"zrangebylex":      {by: 'l'},
	"zrevrangebylex":   {by: 'l', rev: true},
	"zremrangebyrank":  {by: 'r'},
	"zremrangebyscore": {by: 's'},
	"zremrangebylex":   {by: 'l'},
}

// parseOpts reads options from i on. Only ZRANGE and ZRANGESTORE (full)
// accept BYSCORE, BYLEX and REV; WITHSCORES is refused when storing.
func (r *zrange) parseOpts(x *Ctx, i int, full, allowWith bool) (resp.Value, bool) {
	for ; i < x.NArgs(); i++ {
		switch opt := x.ArgLower(i); {
		case opt == "withscores" && allowWith:
			r.withScores = true
		case opt == "limit" && i+2 < x.NArgs():
			off, ok1 := x.Int(i + 1)
			cnt, ok2 := x.Int(i + 2)
			if !ok1 || !ok2 {
				return ErrNotInt, false
			}
			r.offset, r.count, r.limit = clampInt(off), clampInt(cnt), true
			i += 2
		case opt == "byscore" && full && r.by == 'r':
			r.by = 's'
		case opt == "bylex" && full && r.by == 'r':
			r.by = 'l'
		case opt == "rev" && full:
			r.rev = true
		default:
			return ErrSyntax, false
		}
	}
	if r.limit && r.by == 'r' {
		return resp.Err("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"), false
	}
	if r.withScores && r.by == 'l' {
		return resp.Err("ERR syntax error, WITHSCORES not supported in combination with BYLEX"), false
	}
	return resp.Value{}, true
}

// run executes the query; z may be nil (missing key).
func (r *zrange) run(z *skiplist.ZSet) ([]skiplist.Pair, resp.Value, bool) {
	lo, hi := r.start, r.stop
	if r.rev && r.by != 'r' {
		lo, hi = hi, lo
	}
	var out []skiplist.Pair
	switch r.by {
	case 'r':
		start, ok1 := ParseInt(r.start)
		stop, ok2 := ParseInt(r.stop)
		if !ok1 || !ok2 {
			return nil, ErrNotInt, false
		}
		if z != nil {
			out = z.RangeByRank(clampInt(start), clampInt(stop), r.rev)
		}
	case 's':
		from, ok1 := parseScoreBound(lo)
		to, ok2 := parseScoreBound(hi)
		if !ok1 || !ok2 {
			return nil, errScoreRange, false
		}
		if z != nil && r.offset >= 0 {
			out = z.RangeByScore(from, to, r.rev, r.offset, r.count)
		}
	case 'l':
		from, ok1 := parseLexBound(lo)
		to, ok2 := parseLexBound(hi)
		if !ok1 || !ok2 {
			return nil, errLexRange, false
		}
		if z != nil && r.offset >= 0 {
			out = z.RangeByLex(from, to, r.rev, r.offset, r.count)
		}
	}
	return out, resp.Value{}, true
}

// zrangeCmd serves ZRANGE, ZRANGESTORE and the legacy range commands.
func zrangeCmd(x *Ctx) resp.Value {
	r := zrangeModes[x.Name]
	r.count = -1
	storing := x.Name == "zrangestore"
	at := 2 // index of start
	if storing {
		at = 3
	}
	r.start, r.stop = x.Arg(at), x.Arg(at+1)
	full := x.Name == "zrange" || storing
	if bad, ok := r.parseOpts(x, at+2, full, !storing); !ok {
		return bad
	}
	src := x.Keys[len(x.Keys)-1]
	z, ok := getZSet(x, src)
	if !ok {
		return ErrWrongTyp
	}
	ps, bad, ok := r.run(z)
	if !ok {
		return bad
	}
	if !storing {
		return pairsReply(ps, r.withScores)
	}
	return storeZSet(x, x.Keys[0], ps)
}

// storeZSet replaces dst with the pairs (deleting it if there are none) and
// replies with the count.
func storeZSet(x *Ctx, dst string, ps []skiplist.Pair) resp.Value {
	if len(ps) == 0 {
		if !x.DB().Delete(dst) {
			x.NoChange()
		}
		return resp.Int(0)
	}
	z := skiplist.New()
	for _, p := range ps {
		z.Add(p.Member, p.Score)
	}
	x.DB().Put(dst, store.NewEntry(store.KindZSet, z))
	return resp.Int(int64(len(ps)))
}

func zremrangeCmd(x *Ctx) resp.Value {
	r := zrangeModes[x.Name]
	r.start, r.stop, r.count = x.Arg(2), x.Arg(3), -1
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	ps, bad, ok := r.run(z)
	if !ok {
		return bad
	}
	if len(ps) == 0 {
		x.NoChange()
		return resp.Int(0)
	}
	for _, p := range ps {
		z.Remove(p.Member)
	}
	x.DB().Updated(x.Keys[0])
	return resp.Int(int64(len(ps)))
}

func zpopCmd(x *Ctx) resp.Value {
	count := 1
	switch {
	case x.NArgs() == 3:
		n, ok := x.Int(2)
		if !ok || n < 0 {
			return resp.Err("ERR value is out of range, must be positive")
		}
		count = clampInt(n)
	case x.NArgs() > 3:
		return ErrSyntax
	}
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if z == nil || count == 0 {
		x.NoChange()
		return resp.Array()
	}
	ps := z.Pop(count, x.Name == "zpopmax")
	x.DB().Updated(x.Keys[0])
	return pairsReply(ps, true)
}

// bzpopCmd serves BZPOPMIN and BZPOPMAX.
func bzpopCmd(x *Ctx) resp.Value {
	t, bad, ok := parseTimeout(x, x.NArgs()-1)
	if !ok {
		return bad
	}
	isMax := x.Name == "bzpopmax"
	for _, k := range x.Keys {
		z, ok := getZSet(x, k)
		if !ok {
			return ErrWrongTyp
		}
		if z == nil {
			continue
		}
		p := z.Pop(1, isMax)[0]
		x.DB().Updated(k)
		x.Propagate(strings.ToUpper(x.Name[1:]), k)
		return resp.Array(resp.Bulk(userKey(x, k)), resp.Bulk(p.Member), resp.Double(p.Score))
	}
	if x.TimedOut {
		x.NoChange()
		return resp.NullArray()
	}
	return x.Block(t)
}

// zsetOpCmd serves ZUNION, ZINTER, ZDIFF and their STORE variants.
func zsetOpCmd(x *Ctx) resp.Value {
	op, storing := strings.CutSuffix(x.Name[1:], "store")
	pos := 1
	if storing {
		pos = 2
	}
	keys, bad, ok := parseNumKeys(x, pos)
	if !ok {
		return bad
	}
	weights := make([]float64, len(keys))
	for i := range weights {
		weights[i] = 1
	}
	agg, withScores := "sum", false
	for i := pos + 1 + len(keys); i < x.NArgs(); i++ {
		switch opt := x.ArgLower(i); {
		case opt == "weights" && op != "diff" && i+len(keys) < x.NArgs():
			for j := range weights {
				if weights[j], ok = x.Float(i + 1 + j); !ok {
					return resp.Err("ERR weight value is not a float")
				}
			}
			i += len(keys)
		case opt == "aggregate" && op != "diff" && i+1 < x.NArgs():
			agg = x.ArgLower(i + 1)
			if agg != "sum" && agg != "min" && agg != "max" {
				return ErrSyntax
			}
			i++
		case opt == "withscores" && !storing:
			withScores = true
		default:
			return ErrSyntax
		}
	}

	inputs := make([]map[string]float64, len(keys))
	for i, k := range keys {
		e := x.DB().Get(k)
		switch {
		case e == nil:
		case e.Kind == store.KindZSet:
			m := make(map[string]float64, e.ZSet().Len())
			e.ZSet().ForEach(func(p skiplist.Pair) bool { m[p.Member] = p.Score; return true })
			inputs[i] = m
		case e.Kind == store.KindSet: // sets count as score 1, like in Redis
			m := make(map[string]float64, e.Set().Len())
			for _, s := range e.Set().Members() {
				m[s] = 1
			}
			inputs[i] = m
		default:
			return ErrWrongTyp
		}
	}

	res := zsetOp(op, agg, inputs, weights)
	ps := make([]skiplist.Pair, 0, len(res))
	for m, s := range res {
		ps = append(ps, skiplist.Pair{Member: m, Score: s})
	}
	sort.Slice(ps, func(i, j int) bool {
		if ps[i].Score != ps[j].Score {
			return ps[i].Score < ps[j].Score
		}
		return ps[i].Member < ps[j].Member
	})
	if storing {
		return storeZSet(x, x.Keys[0], ps)
	}
	return pairsReply(ps, withScores)
}

func zsetOp(op, agg string, inputs []map[string]float64, weights []float64) map[string]float64 {
	res := map[string]float64{}
	switch op {
	case "union":
		for i, in := range inputs {
			for m, s := range in {
				s = noNaN(s * weights[i])
				if cur, ok := res[m]; ok {
					s = aggregate(agg, cur, s)
				}
				res[m] = s
			}
		}
	case "inter":
	members:
		for m, s := range inputs[0] {
			s = noNaN(s * weights[0])
			for i, in := range inputs[1:] {
				o, ok := in[m]
				if !ok {
					continue members
				}
				s = aggregate(agg, s, noNaN(o*weights[i+1]))
			}
			res[m] = s
		}
	case "diff":
	diff:
		for m, s := range inputs[0] {
			for _, in := range inputs[1:] {
				if _, ok := in[m]; ok {
					continue diff
				}
			}
			res[m] = s
		}
	}
	return res
}

// noNaN maps NaN (inf*0, inf-inf) to 0, as Redis does when combining scores.
func noNaN(f float64) float64 {
	if math.IsNaN(f) {
		return 0
	}
	return f
}

func aggregate(agg string, a, b float64) float64 {
	switch agg {
	case "min":
		return math.Min(a, b)
	case "max":
		return math.Max(a, b)
	}
	return noNaN(a + b)
}

func zrandmemberCmd(x *Ctx) resp.Value {
	count, hasCount, withScores, bad, ok := parseRandCount(x, "withscores")
	if !ok {
		return bad
	}
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if z == nil {
		if hasCount {
			return resp.Array()
		}
		return resp.Null()
	}
	all := z.RangeByRank(0, -1, false)
	if !hasCount {
		return resp.Bulk(all[randomPicks(len(all), -1)[0]].Member)
	}
	var ps []skiplist.Pair
	for _, i := range randomPicks(len(all), count) {
		ps = append(ps, all[i])
	}
	return pairsReply(ps, withScores)
}

func zscanCmd(x *Ctx) resp.Value {
	_, o, bad, ok := parseScan(x, 2, false, false)
	if !ok {
		return bad
	}
	z, ok := getZSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	var items []resp.Value
	if z != nil {
		z.ForEach(func(p skiplist.Pair) bool {
			if o.matches(p.Member) {
				items = append(items, resp.Bulk(p.Member), resp.Bulk(resp.FormatFloat(p.Score)))
			}
			return true
		})
	}
	return scanReply(0, items)
}
