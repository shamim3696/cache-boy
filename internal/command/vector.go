package command

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"

	"cache-boy/internal/datatype/vector"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "vadd", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "vector", FirstKey: 1, LastKey: 1, Summary: "VADD key id v1 ... vn [ATTR json]", Handler: vaddCmd},
		&Spec{Name: "vsearch", Arity: -4, Flags: FlagReadOnly, Group: "vector", FirstKey: 1, LastKey: 1, Summary: "VSEARCH key K v1 ... vn [FILTER attr=value]", Handler: vsearchCmd},
		&Spec{Name: "vget", Arity: 3, Flags: FlagReadOnly, Group: "vector", FirstKey: 1, LastKey: 1, Summary: "Returns a stored vector", Handler: vgetCmd},
		&Spec{Name: "vdel", Arity: 3, Flags: FlagWrite, Group: "vector", FirstKey: 1, LastKey: 1, Summary: "Removes a vector", Handler: vdelCmd},
		&Spec{Name: "vcard", Arity: 2, Flags: FlagReadOnly, Group: "vector", FirstKey: 1, LastKey: 1, Summary: "Number of vectors", Handler: vcardCmd},
		&Spec{Name: "vdim", Arity: 2, Flags: FlagReadOnly, Group: "vector", FirstKey: 1, LastKey: 1, Summary: "Vector dimension", Handler: vcardCmd},
	)
}

// getVectors returns key's index, nil if missing, or false on a wrong type.
func getVectors(x *Ctx, key string) (*vector.Index, bool) {
	e, err := x.DB().GetKind(key, store.KindVector)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.Vectors(), true
}

// parseVector reads float components up to (not including) the first
// argument equal to stopWord, and returns the index after the last one.
func parseVector(x *Ctx, i int, stopWord string) ([]float32, int, resp.Value, bool) {
	var v []float32
	for ; i < x.NArgs() && x.ArgLower(i) != stopWord; i++ {
		f, err := strconv.ParseFloat(x.Arg(i), 32)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return nil, i, resp.Err("ERR invalid vector component"), false
		}
		v = append(v, float32(f))
	}
	if len(v) == 0 {
		return nil, i, resp.Err(vector.ErrEmpty.Error()), false
	}
	return v, i, resp.Value{}, true
}

func vaddCmd(x *Ctx) resp.Value {
	key, id := x.Keys[0], x.Arg(2)
	v, i, bad, ok := parseVector(x, 3, "attr")
	if !ok {
		return bad
	}
	var attr string
	switch {
	case i == x.NArgs():
	case i+2 == x.NArgs():
		attr = x.Arg(i + 1)
		if !json.Valid([]byte(attr)) {
			return resp.Err("ERR ATTR is not valid JSON")
		}
	default:
		return ErrSyntax
	}
	idx, ok := getVectors(x, key)
	if !ok {
		return ErrWrongTyp
	}
	created := idx == nil
	if created {
		idx = vector.New()
	}
	isNew, err := idx.Add(id, v, attr)
	if err != nil {
		return resp.Err(err.Error())
	}
	if created {
		x.DB().Put(key, store.NewEntry(store.KindVector, idx))
	} else {
		x.DB().Updated(key)
	}
	return intBool(isNew)
}

func vsearchCmd(x *Ctx) resp.Value {
	k, ok := x.Int(2)
	if !ok || k < 0 {
		return resp.Err("ERR K must be a non-negative integer")
	}
	q, i, bad, ok := parseVector(x, 3, "filter")
	if !ok {
		return bad
	}
	var filter func(id, attr string) bool
	switch {
	case i == x.NArgs():
	case i+2 == x.NArgs():
		field, want, found := strings.Cut(x.Arg(i+1), "=")
		if !found || field == "" {
			return resp.Err("ERR FILTER must be attr=value")
		}
		filter = func(_, attr string) bool { return attrMatches(attr, field, want) }
	default:
		return ErrSyntax
	}
	idx, ok := getVectors(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if idx == nil {
		return resp.Array()
	}
	res, err := idx.Search(q, clampInt(k), filter)
	if err != nil {
		return resp.Err(err.Error())
	}
	out := make([]resp.Value, len(res))
	for j, r := range res {
		out[j] = resp.Array(resp.Bulk(r.ID), resp.Double(float64(r.Score)))
	}
	return resp.Array(out...)
}

// attrMatches reports whether the JSON object attr has a top-level field
// equal to want, as a string or as a number (so "2" matches 2.0).
func attrMatches(attr, field, want string) bool {
	if attr == "" {
		return false
	}
	dec := json.NewDecoder(strings.NewReader(attr))
	dec.UseNumber()
	var obj map[string]any
	if dec.Decode(&obj) != nil {
		return false
	}
	switch v := obj[field].(type) {
	case string:
		return v == want
	case json.Number:
		if v.String() == want {
			return true
		}
		a, err1 := v.Float64()
		b, err2 := strconv.ParseFloat(want, 64)
		return err1 == nil && err2 == nil && a == b
	}
	return false
}

func vgetCmd(x *Ctx) resp.Value {
	idx, ok := getVectors(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if idx == nil {
		return resp.Null()
	}
	v, _, found := idx.Get(x.Arg(2))
	if !found {
		return resp.Null()
	}
	out := make([]string, len(v))
	for i, f := range v {
		out[i] = strconv.FormatFloat(float64(f), 'f', -1, 32)
	}
	return resp.Strings(out)
}

func vdelCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	idx, ok := getVectors(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if idx == nil || !idx.Remove(x.Arg(2)) {
		x.NoChange()
		return resp.Int(0)
	}
	// store.DB.Updated only drops empty lists, hashes, sets and zsets.
	if idx.Len() == 0 {
		x.DB().Delete(key)
	} else {
		x.DB().Updated(key)
	}
	return resp.Int(1)
}

// vcardCmd serves VCARD and VDIM.
func vcardCmd(x *Ctx) resp.Value {
	idx, ok := getVectors(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if idx == nil {
		return resp.Int(0)
	}
	if x.Name == "vdim" {
		return resp.Int(int64(idx.Dim()))
	}
	return resp.Int(int64(idx.Len()))
}
