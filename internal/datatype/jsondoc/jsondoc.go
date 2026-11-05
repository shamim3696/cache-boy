// Package jsondoc stores a JSON document and supports a JSONPath subset:
//
//	$            root
//	.name        object member
//	["name"]     object member (any characters)
//	[3] [-1]     array index (negative counts from the end)
//	[*] .*       every child
//
// Paths may also be written without "$" (".a.b" or "a.b").
package jsondoc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

var (
	ErrPath     = errors.New("ERR invalid JSON path")
	ErrNotFound = errors.New("ERR path does not exist")
	ErrType     = errors.New("ERR wrong JSON type for this operation")
)

type Doc struct {
	root any
	size int64
}

// Parse creates a document from JSON text.
func Parse(text string) (*Doc, error) {
	v, err := decode(text)
	if err != nil {
		return nil, err
	}
	d := &Doc{root: v}
	d.resize()
	return d, nil
}

func (d *Doc) MemSize() int64 { return d.size + 48 }

// Root returns the decoded value (map[string]any, []any, string, float64/json.Number, bool, nil).
func (d *Doc) Root() any { return d.root }

// String serializes the whole document.
func (d *Doc) String() string { return Encode(d.root) }

// Get returns every value matching path.
func (d *Doc) Get(path string) ([]any, error) {
	steps, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	var out []any
	walk(d.root, steps, func(v any, _ func(any)) { out = append(out, v) })
	return out, nil
}

// Set replaces every match of path with the JSON value text. If the path
// doesn't match but its parent is an object, the member is created.
// Returns the number of values set.
func (d *Doc) Set(path, valueText string) (int, error) {
	val, err := decode(valueText)
	if err != nil {
		return 0, err
	}
	steps, err := parsePath(path)
	if err != nil {
		return 0, err
	}
	if len(steps) == 0 {
		d.root = val
		d.resize()
		return 1, nil
	}
	n := 0
	walk(d.root, steps, func(_ any, set func(any)) { set(clone(val)); n++ })
	if n == 0 {
		last := steps[len(steps)-1]
		if last.kind != stepKey {
			return 0, ErrNotFound
		}
		walk(d.root, steps[:len(steps)-1], func(parent any, _ func(any)) {
			if m, ok := parent.(map[string]any); ok {
				m[last.key] = clone(val)
				n++
			}
		})
	}
	if n > 0 {
		d.resize()
	}
	return n, nil
}

// Del removes every match of path. Deleting the root returns -1 (caller should delete the key).
func (d *Doc) Del(path string) (int, error) {
	steps, err := parsePath(path)
	if err != nil {
		return 0, err
	}
	if len(steps) == 0 {
		return -1, nil
	}
	last := steps[len(steps)-1]
	n := 0
	walk(d.root, steps[:len(steps)-1], func(parent any, set func(any)) {
		switch p := parent.(type) {
		case map[string]any:
			if last.kind == stepWild {
				n += len(p)
				clear(p)
			} else if _, ok := p[last.key]; ok && last.kind == stepKey {
				delete(p, last.key)
				n++
			}
		case []any:
			if last.kind == stepWild {
				n += len(p)
				set([]any{})
			} else if last.kind == stepIndex {
				if i, ok := normIndex(last.index, len(p)); ok {
					set(append(p[:i:i], p[i+1:]...))
					n++
				}
			}
		}
	})
	if n > 0 {
		d.resize()
	}
	return n, nil
}

// NumIncrBy adds delta to every number at path and returns the new values.
func (d *Doc) NumIncrBy(path string, delta float64) ([]float64, error) {
	steps, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	var out []float64
	var typeErr bool
	apply := func(v any, set func(any)) {
		f, ok := toFloat(v)
		if !ok {
			typeErr = true
			return
		}
		f += delta
		set(json.Number(strconv.FormatFloat(f, 'f', -1, 64)))
		out = append(out, f)
	}
	if len(steps) == 0 {
		apply(d.root, func(v any) { d.root = v })
	} else {
		walk(d.root, steps, apply)
	}
	if typeErr && len(out) == 0 {
		return nil, ErrType
	}
	if len(out) == 0 {
		return nil, ErrNotFound
	}
	d.resize()
	return out, nil
}

// ArrAppend appends JSON values to every array at path; returns new lengths.
func (d *Doc) ArrAppend(path string, valueTexts ...string) ([]int, error) {
	vals := make([]any, len(valueTexts))
	for i, t := range valueTexts {
		v, err := decode(t)
		if err != nil {
			return nil, err
		}
		vals[i] = v
	}
	steps, err := parsePath(path)
	if err != nil {
		return nil, err
	}
	var out []int
	apply := func(v any, set func(any)) {
		arr, ok := v.([]any)
		if !ok {
			return
		}
		for _, x := range vals {
			arr = append(arr, clone(x))
		}
		set(arr)
		out = append(out, len(arr))
	}
	if len(steps) == 0 {
		apply(d.root, func(v any) { d.root = v })
	} else {
		walk(d.root, steps, apply)
	}
	if len(out) == 0 {
		return nil, ErrType
	}
	d.resize()
	return out, nil
}

// TypeOf returns the JSON type name of a value.
func TypeOf(v any) string {
	switch x := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return "integer"
		}
		return "number"
	case float64:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return "unknown"
}

// Encode serializes a value with sorted object keys.
func Encode(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.Encode(v)
	return strings.TrimRight(buf.String(), "\n")
}

// Keys returns the sorted member names of an object value.
func Keys(v any) []string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- internals ---

type stepKind int

const (
	stepKey stepKind = iota
	stepIndex
	stepWild
)

type step struct {
	kind  stepKind
	key   string
	index int
}

func parsePath(p string) ([]step, error) {
	p = strings.TrimSpace(p)
	switch {
	case p == "" || p == "$" || p == ".":
		return nil, nil
	case strings.HasPrefix(p, "$"):
		p = p[1:]
	case !strings.HasPrefix(p, ".") && !strings.HasPrefix(p, "["):
		p = "." + p
	}
	var steps []step
	for len(p) > 0 {
		switch p[0] {
		case '.':
			p = p[1:]
			end := strings.IndexAny(p, ".[")
			if end == -1 {
				end = len(p)
			}
			name := p[:end]
			if name == "" {
				return nil, ErrPath
			}
			if name == "*" {
				steps = append(steps, step{kind: stepWild})
			} else {
				steps = append(steps, step{kind: stepKey, key: name})
			}
			p = p[end:]
		case '[':
			end := strings.IndexByte(p, ']')
			if end == -1 {
				return nil, ErrPath
			}
			inner := strings.TrimSpace(p[1:end])
			p = p[end+1:]
			switch {
			case inner == "*":
				steps = append(steps, step{kind: stepWild})
			case len(inner) >= 2 && (inner[0] == '"' || inner[0] == '\''):
				steps = append(steps, step{kind: stepKey, key: inner[1 : len(inner)-1]})
			default:
				i, err := strconv.Atoi(inner)
				if err != nil {
					return nil, ErrPath
				}
				steps = append(steps, step{kind: stepIndex, index: i})
			}
		default:
			return nil, ErrPath
		}
	}
	return steps, nil
}

// walk calls fn for every value matching steps. set replaces that value in its parent.
func walk(v any, steps []step, fn func(v any, set func(any))) {
	var rec func(v any, set func(any), steps []step)
	rec = func(v any, set func(any), steps []step) {
		if len(steps) == 0 {
			fn(v, set)
			return
		}
		s := steps[0]
		switch x := v.(type) {
		case map[string]any:
			switch s.kind {
			case stepKey:
				if child, ok := x[s.key]; ok {
					rec(child, func(nv any) { x[s.key] = nv }, steps[1:])
				}
			case stepWild:
				for _, k := range Keys(x) {
					k := k
					rec(x[k], func(nv any) { x[k] = nv }, steps[1:])
				}
			}
		case []any:
			switch s.kind {
			case stepIndex:
				if i, ok := normIndex(s.index, len(x)); ok {
					rec(x[i], func(nv any) { x[i] = nv }, steps[1:])
				}
			case stepWild:
				for i := range x {
					i := i
					rec(x[i], func(nv any) { x[i] = nv }, steps[1:])
				}
			}
		}
	}
	rec(v, func(any) {}, steps)
}

func normIndex(i, n int) (int, bool) {
	if i < 0 {
		i += n
	}
	return i, i >= 0 && i < n
}

func decode(text string) (any, error) {
	dec := json.NewDecoder(strings.NewReader(text))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("ERR invalid JSON: %v", err)
	}
	if dec.More() {
		return nil, errors.New("ERR invalid JSON: trailing data")
	}
	return v, nil
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case float64:
		return x, true
	}
	return 0, false
}

func clone(v any) any {
	switch x := v.(type) {
	case map[string]any:
		m := make(map[string]any, len(x))
		for k, e := range x {
			m[k] = clone(e)
		}
		return m
	case []any:
		a := make([]any, len(x))
		for i, e := range x {
			a[i] = clone(e)
		}
		return a
	}
	return v
}

func (d *Doc) resize() { d.size = int64(len(Encode(d.root))) * 2 }
