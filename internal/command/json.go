package command

import (
	"strconv"
	"strings"

	"github.com/shamim3696/cache-boy/internal/datatype/jsondoc"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "json.set", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.SET key path json [NX|XX]", Handler: jsonSetCmd},
		&Spec{Name: "json.get", Arity: -2, Flags: FlagReadOnly, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.GET key [path ...]", Handler: jsonGetCmd},
		&Spec{Name: "json.del", Arity: -2, Flags: FlagWrite, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.DEL key [path]", Handler: jsonDelCmd},
		&Spec{Name: "json.type", Arity: -2, Flags: FlagReadOnly, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.TYPE key [path]", Handler: jsonTypeCmd},
		&Spec{Name: "json.numincrby", Arity: 4, Flags: FlagWrite, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.NUMINCRBY key path n", Handler: jsonNumIncrByCmd},
		&Spec{Name: "json.arrappend", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.ARRAPPEND key path json ...", Handler: jsonArrAppendCmd},
		&Spec{Name: "json.arrlen", Arity: -2, Flags: FlagReadOnly, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.ARRLEN key [path]", Handler: jsonLenCmd},
		&Spec{Name: "json.objkeys", Arity: -2, Flags: FlagReadOnly, Group: "json", FirstKey: 1, LastKey: 1, Summary: "JSON.OBJKEYS key [path]", Handler: jsonLenCmd},
		&Spec{Name: "json.mget", Arity: -3, Flags: FlagReadOnly, Group: "json", FirstKey: 1, LastKey: -2, Summary: "JSON.MGET key ... path", Handler: jsonMGetCmd},
	)
}

var errJSONNoKey = resp.Err("ERR could not perform this operation on a key that doesn't exist")

// getJSON returns key's document, nil if missing, or false on a wrong type.
func getJSON(x *Ctx, key string) (*jsondoc.Doc, bool) {
	e, err := x.DB().GetKind(key, store.KindJSON)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.JSON(), true
}

func isRootPath(p string) bool {
	p = strings.TrimSpace(p)
	return p == "$" || p == "." || p == ""
}

// optPath returns the optional path argument at i, defaulting to the root.
func optPath(x *Ctx, i int) string {
	if i < x.NArgs() {
		return x.Arg(i)
	}
	return "$"
}

func jsonErr(err error) resp.Value { return resp.Err(err.Error()) }

func jsonSetCmd(x *Ctx) resp.Value {
	key, path, text := x.Keys[0], x.Arg(2), x.Arg(3)
	var nx, xx bool
	switch {
	case x.NArgs() == 4:
	case x.NArgs() == 5 && x.ArgLower(4) == "nx":
		nx = true
	case x.NArgs() == 5 && x.ArgLower(4) == "xx":
		xx = true
	default:
		return ErrSyntax
	}
	d, ok := getJSON(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		if !isRootPath(path) {
			return resp.Err("ERR new objects must be created at the root")
		}
		if xx {
			x.NoChange()
			return resp.Null()
		}
		nd, err := jsondoc.Parse(text)
		if err != nil {
			return jsonErr(err)
		}
		x.DB().Put(key, store.NewEntry(store.KindJSON, nd))
		return resp.OK
	}
	matches, err := d.Get(path)
	if err != nil {
		return jsonErr(err)
	}
	if nx && len(matches) > 0 || xx && len(matches) == 0 {
		x.NoChange()
		return resp.Null()
	}
	n, err := d.Set(path, text)
	if err != nil {
		return jsonErr(err)
	}
	if n == 0 {
		x.NoChange()
		return resp.Null()
	}
	x.DB().Updated(key)
	return resp.OK
}

// jsonText renders the JSON.GET reply for paths: the document itself for no
// path or the root, the matches as an array for one path, and an object of
// path → matches for several.
func jsonText(d *jsondoc.Doc, paths []string) (string, error) {
	if len(paths) == 0 || len(paths) == 1 && isRootPath(paths[0]) {
		return d.String(), nil
	}
	if len(paths) == 1 {
		m, err := matchesOf(d, paths[0])
		if err != nil {
			return "", err
		}
		return jsondoc.Encode(m), nil
	}
	obj := make(map[string]any, len(paths))
	for _, p := range paths {
		m, err := matchesOf(d, p)
		if err != nil {
			return "", err
		}
		obj[p] = m
	}
	return jsondoc.Encode(obj), nil
}

// matchesOf is Doc.Get with an empty (not nil) result, so it encodes as [].
func matchesOf(d *jsondoc.Doc, path string) ([]any, error) {
	m, err := d.Get(path)
	if m == nil {
		m = []any{}
	}
	return m, err
}

func jsonGetCmd(x *Ctx) resp.Value {
	d, ok := getJSON(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return resp.Null()
	}
	s, err := jsonText(d, x.Args[2:])
	if err != nil {
		return jsonErr(err)
	}
	return resp.Bulk(s)
}

func jsonDelCmd(x *Ctx) resp.Value {
	if x.NArgs() > 3 {
		return ErrSyntax
	}
	key := x.Keys[0]
	d, ok := getJSON(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		x.NoChange()
		return resp.Int(0)
	}
	n, err := d.Del(optPath(x, 2))
	switch {
	case err != nil:
		return jsonErr(err)
	case n < 0:
		x.DB().Delete(key)
		return resp.Int(1)
	case n == 0:
		x.NoChange()
		return resp.Int(0)
	}
	x.DB().Updated(key)
	return resp.Int(int64(n))
}

func jsonTypeCmd(x *Ctx) resp.Value {
	if x.NArgs() > 3 {
		return ErrSyntax
	}
	d, ok := getJSON(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return resp.Null()
	}
	matches, err := d.Get(optPath(x, 2))
	if err != nil {
		return jsonErr(err)
	}
	types := make([]string, len(matches))
	for i, v := range matches {
		types[i] = jsondoc.TypeOf(v)
	}
	return resp.Strings(types)
}

func jsonNumIncrByCmd(x *Ctx) resp.Value {
	delta, ok := x.Float(3)
	if !ok {
		return ErrNotFloat
	}
	key := x.Keys[0]
	d, ok := getJSON(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return errJSONNoKey
	}
	vals, err := d.NumIncrBy(x.Arg(2), delta)
	if err != nil {
		return jsonErr(err)
	}
	x.DB().Updated(key)
	// Same number formatting as the values stored in the document.
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = strconv.FormatFloat(v, 'f', -1, 64)
	}
	return resp.Bulk("[" + strings.Join(parts, ",") + "]")
}

func jsonArrAppendCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	d, ok := getJSON(x, key)
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return errJSONNoKey
	}
	lens, err := d.ArrAppend(x.Arg(2), x.Args[3:]...)
	if err != nil {
		return jsonErr(err)
	}
	x.DB().Updated(key)
	out := make([]resp.Value, len(lens))
	for i, n := range lens {
		out[i] = resp.Int(int64(n))
	}
	return resp.Array(out...)
}

// jsonLenCmd serves JSON.ARRLEN and JSON.OBJKEYS; matches of another JSON
// type yield null.
func jsonLenCmd(x *Ctx) resp.Value {
	if x.NArgs() > 3 {
		return ErrSyntax
	}
	d, ok := getJSON(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if d == nil {
		return resp.Null()
	}
	matches, err := d.Get(optPath(x, 2))
	if err != nil {
		return jsonErr(err)
	}
	out := make([]resp.Value, len(matches))
	for i, v := range matches {
		out[i] = resp.Null()
		switch t := v.(type) {
		case []any:
			if x.Name == "json.arrlen" {
				out[i] = resp.Int(int64(len(t)))
			}
		case map[string]any:
			if x.Name == "json.objkeys" {
				out[i] = resp.Strings(jsondoc.Keys(t))
			}
		}
	}
	return resp.Array(out...)
}

func jsonMGetCmd(x *Ctx) resp.Value {
	path := x.Arg(x.NArgs() - 1)
	out := make([]resp.Value, len(x.Keys))
	for i, k := range x.Keys {
		out[i] = resp.Null()
		e := x.DB().Get(k)
		if e == nil || e.Kind != store.KindJSON {
			continue
		}
		s, err := jsonText(e.JSON(), []string{path})
		if err != nil {
			return jsonErr(err)
		}
		out[i] = resp.Bulk(s)
	}
	return resp.Array(out...)
}
