package command

import (
	"math"
	"sort"
	"strings"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "sadd", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Adds members to a set", Handler: saddCmd},
		&Spec{Name: "srem", Arity: -3, Flags: FlagWrite, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Removes members from a set", Handler: sremCmd},
		&Spec{Name: "smembers", Arity: 2, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Returns every member", Handler: smembersCmd},
		&Spec{Name: "sismember", Arity: 3, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Whether a value is a member", Handler: sismemberCmd},
		&Spec{Name: "smismember", Arity: -3, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Whether each value is a member", Handler: sismemberCmd},
		&Spec{Name: "scard", Arity: 2, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Number of members", Handler: scardCmd},
		&Spec{Name: "spop", Arity: -2, Flags: FlagWrite, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Removes and returns random members", Handler: spopCmd},
		&Spec{Name: "srandmember", Arity: -2, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Summary: "Returns random members", Handler: srandmemberCmd},
		&Spec{Name: "smove", Arity: 4, Flags: FlagWrite, Group: "set", FirstKey: 1, LastKey: 2, Summary: "Moves a member to another set", Handler: smoveCmd},
		&Spec{Name: "sinter", Arity: -2, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: -1, Summary: "Intersection of sets", Handler: setOpCmd},
		&Spec{Name: "sunion", Arity: -2, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: -1, Summary: "Union of sets", Handler: setOpCmd},
		&Spec{Name: "sdiff", Arity: -2, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: -1, Summary: "Members of the first set not in the others", Handler: setOpCmd},
		&Spec{Name: "sinterstore", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "set", FirstKey: 1, LastKey: -1, Summary: "Stores an intersection", Handler: setOpCmd},
		&Spec{Name: "sunionstore", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "set", FirstKey: 1, LastKey: -1, Summary: "Stores a union", Handler: setOpCmd},
		&Spec{Name: "sdiffstore", Arity: -3, Flags: FlagWrite | FlagDenyOOM, Group: "set", FirstKey: 1, LastKey: -1, Summary: "Stores a difference", Handler: setOpCmd},
		&Spec{Name: "sintercard", Arity: -3, Flags: FlagReadOnly, Group: "set", KeysFn: numKeys(1), Summary: "SINTERCARD numkeys key ... [LIMIT n]", Handler: sintercardCmd},
		&Spec{Name: "sscan", Arity: -3, Flags: FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Summary: "SSCAN key cursor [MATCH p] [COUNT n]", Handler: sscanCmd},
	)
}

func newSet() sizer { return store.NewSet() }

// getSet returns key's set, nil if missing, or false on a wrong type.
func getSet(x *Ctx, key string) (*store.Set, bool) {
	e, err := x.DB().GetKind(key, store.KindSet)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.Set(), true
}

func sortedMembers(s *store.Set) []string {
	if s == nil {
		return []string{}
	}
	return sortStrings(s.Members())
}

func saddCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	e, ok := getOrCreate(x, key, store.KindSet, newSet)
	if !ok {
		return ErrWrongTyp
	}
	n := 0
	for _, m := range x.Args[2:] {
		if e.Set().Add(m) {
			n++
		}
	}
	if n == 0 {
		x.NoChange()
	}
	x.DB().Updated(key)
	return resp.Int(int64(n))
}

func sremCmd(x *Ctx) resp.Value {
	s, ok := getSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	n := 0
	if s != nil {
		for _, m := range x.Args[2:] {
			if s.Remove(m) {
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

func smembersCmd(x *Ctx) resp.Value {
	s, ok := getSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	return resp.StringSet(sortedMembers(s))
}

// sismemberCmd serves SISMEMBER and SMISMEMBER.
func sismemberCmd(x *Ctx) resp.Value {
	s, ok := getSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	out := make([]resp.Value, 0, x.NArgs()-2)
	for _, m := range x.Args[2:] {
		out = append(out, intBool(s != nil && s.Has(m)))
	}
	if x.Name == "sismember" {
		return out[0]
	}
	return resp.Array(out...)
}

func scardCmd(x *Ctx) resp.Value {
	s, ok := getSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if s == nil {
		return resp.Int(0)
	}
	return resp.Int(int64(s.Len()))
}

func spopCmd(x *Ctx) resp.Value {
	if x.NArgs() > 3 {
		return ErrSyntax
	}
	count := int64(-1)
	if x.NArgs() == 3 {
		n, ok := x.Int(2)
		if !ok || n < 0 {
			return resp.Err("ERR value is out of range, must be positive")
		}
		count = n
	}
	key := x.Keys[0]
	s, ok := getSet(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if s == nil {
		x.NoChange()
		if count < 0 {
			return resp.Null()
		}
		return resp.StringSet(nil)
	}
	n := int(min(max(count, 1), math.MaxInt32))
	if count == 0 {
		n = 0
	}
	var popped []string
	for len(popped) < n {
		m, ok := s.Random()
		if !ok {
			break
		}
		s.Remove(m)
		popped = append(popped, m)
	}
	if len(popped) == 0 {
		x.NoChange()
		return resp.StringSet(nil)
	}
	x.DB().Updated(key)
	// The choice is random: replicas must remove exactly these members.
	x.Propagate(append([]string{"SREM", key}, popped...)...)
	if count < 0 {
		return resp.Bulk(popped[0])
	}
	return resp.StringSet(popped)
}

func srandmemberCmd(x *Ctx) resp.Value {
	count, hasCount, _, bad, ok := parseRandCount(x, "")
	if !ok {
		return bad
	}
	s, ok := getSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if !hasCount {
		if s == nil {
			return resp.Null()
		}
		m, _ := s.Random()
		return resp.Bulk(m)
	}
	members := sortedMembers(s)
	out := make([]string, 0, len(members))
	for _, i := range randomPicks(len(members), count) {
		out = append(out, members[i])
	}
	return resp.Strings(out)
}

func smoveCmd(x *Ctx) resp.Value {
	src, dst, m := x.Keys[0], x.Keys[1], x.Arg(3)
	ss, ok := getSet(x, src)
	if !ok {
		return ErrWrongTyp
	}
	if _, ok := getSet(x, dst); !ok {
		return ErrWrongTyp
	}
	if ss == nil || !ss.Has(m) {
		x.NoChange()
		return resp.Int(0)
	}
	if src == dst {
		x.NoChange()
		return resp.Int(1)
	}
	ss.Remove(m)
	e, _ := getOrCreate(x, dst, store.KindSet, newSet)
	e.Set().Add(m)
	x.DB().Updated(src)
	x.DB().Updated(dst)
	return resp.Int(1)
}

// setOp computes SINTER/SUNION/SDIFF over keys. Missing keys are empty sets.
func setOp(x *Ctx, op string, keys []string) (map[string]struct{}, bool) {
	sets := make([]*store.Set, len(keys))
	for i, k := range keys {
		s, ok := getSet(x, k)
		if !ok {
			return nil, false
		}
		sets[i] = s
	}
	out := map[string]struct{}{}
	switch op {
	case "union":
		for _, s := range sets {
			if s != nil {
				s.Range(func(m string) bool { out[m] = struct{}{}; return true })
			}
		}
	case "inter":
		// Iterate the smallest set; any missing key makes the result empty.
		sort.Slice(sets, func(i, j int) bool { return setLen(sets[i]) < setLen(sets[j]) })
		if sets[0] == nil {
			return out, true
		}
		sets[0].Range(func(m string) bool {
			for _, s := range sets[1:] {
				if !s.Has(m) {
					return true
				}
			}
			out[m] = struct{}{}
			return true
		})
	case "diff":
		if sets[0] == nil {
			return out, true
		}
		sets[0].Range(func(m string) bool {
			for _, s := range sets[1:] {
				if s != nil && s.Has(m) {
					return true
				}
			}
			out[m] = struct{}{}
			return true
		})
	}
	return out, true
}

func setLen(s *store.Set) int {
	if s == nil {
		return 0
	}
	return s.Len()
}

func memberList(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return sortStrings(out)
}

// setOpCmd serves SINTER, SUNION, SDIFF and their STORE variants.
func setOpCmd(x *Ctx) resp.Value {
	op, toKey := strings.CutSuffix(x.Name[1:], "store")
	keys := x.Keys
	if toKey {
		keys = x.Keys[1:]
	}
	res, ok := setOp(x, op, keys)
	if !ok {
		return ErrWrongTyp
	}
	if !toKey {
		return resp.StringSet(memberList(res))
	}
	dst := x.Keys[0]
	if len(res) == 0 {
		if !x.DB().Delete(dst) {
			x.NoChange()
		}
		return resp.Int(0)
	}
	s := store.NewSet()
	for m := range res {
		s.Add(m)
	}
	x.DB().Put(dst, store.NewEntry(store.KindSet, s))
	return resp.Int(int64(len(res)))
}

func sintercardCmd(x *Ctx) resp.Value {
	keys, bad, ok := parseNumKeys(x, 1)
	if !ok {
		return bad
	}
	limit := int64(0)
	i := 2 + len(keys)
	switch {
	case i == x.NArgs():
	case i+2 == x.NArgs() && x.ArgLower(i) == "limit":
		if limit, ok = x.Int(i + 1); !ok || limit < 0 {
			return resp.Err("ERR LIMIT can't be negative")
		}
	default:
		return ErrSyntax
	}
	res, ok := setOp(x, "inter", keys)
	if !ok {
		return ErrWrongTyp
	}
	n := int64(len(res))
	if limit > 0 {
		n = min(n, limit)
	}
	return resp.Int(n)
}

func sscanCmd(x *Ctx) resp.Value {
	_, o, bad, ok := parseScan(x, 2, false, false)
	if !ok {
		return bad
	}
	s, ok := getSet(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	var items []resp.Value
	for _, m := range sortedMembers(s) {
		if o.matches(m) {
			items = append(items, resp.Bulk(m))
		}
	}
	return scanReply(0, items)
}
