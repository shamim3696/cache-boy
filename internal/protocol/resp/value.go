// Package resp implements the Redis serialization protocol, versions 2 and 3.
// Spec: https://redis.io/docs/latest/develop/reference/protocol-spec/
package resp

import (
	"fmt"
	"strconv"
)

type Type byte

const (
	TypeSimple  Type = '+'
	TypeError   Type = '-'
	TypeInteger Type = ':'
	TypeBulk    Type = '$'
	TypeArray   Type = '*'
	// RESP3 types. When talking to a RESP2 client they are down-converted.
	TypeNull   Type = '_'
	TypeBool   Type = '#'
	TypeDouble Type = ','
	TypeMap    Type = '%'
	TypeSet    Type = '~'
	TypePush   Type = '>'

	// Internal, never sent on the wire.
	typeMulti Type = 1 // several replies written back to back (e.g. SUBSCRIBE a b)
	typeNone  Type = 2 // no reply at all
)

// Value is any RESP value. Which fields are used depends on Type.
type Value struct {
	Type  Type
	Str   string  // simple, error, bulk
	Int   int64   // integer
	Float float64 // double
	Bool  bool    // bool
	Elems []Value // array, set, push; map stores key, value, key, value...
	// nullArray makes a RESP2 null encode as *-1 instead of $-1.
	nullArray bool
}

var OK = Simple("OK")

func Simple(s string) Value   { return Value{Type: TypeSimple, Str: s} }
func Bulk(s string) Value     { return Value{Type: TypeBulk, Str: s} }
func Bytes(b []byte) Value    { return Value{Type: TypeBulk, Str: string(b)} }
func Int(n int64) Value       { return Value{Type: TypeInteger, Int: n} }
func Double(f float64) Value  { return Value{Type: TypeDouble, Float: f} }
func Bool(b bool) Value       { return Value{Type: TypeBool, Bool: b} }
func Null() Value             { return Value{Type: TypeNull} }
func NullArray() Value        { return Value{Type: TypeNull, nullArray: true} }
func Array(vs ...Value) Value { return Value{Type: TypeArray, Elems: nonNil(vs)} }
func SetOf(vs ...Value) Value { return Value{Type: TypeSet, Elems: nonNil(vs)} }
func Push(vs ...Value) Value  { return Value{Type: TypePush, Elems: nonNil(vs)} }
func Multi(vs ...Value) Value { return Value{Type: typeMulti, Elems: vs} }
func None() Value             { return Value{Type: typeNone} }
func (v Value) IsNone() bool  { return v.Type == typeNone }
func (v Value) IsMulti() bool { return v.Type == typeMulti }
func (v Value) IsError() bool { return v.Type == TypeError }
func (v Value) IsNull() bool  { return v.Type == TypeNull }

// Err returns an error reply. msg should start with an error code such as
// "ERR" or "WRONGTYPE"; if it doesn't, "ERR " is added.
func Err(msg string) Value {
	if !hasCode(msg) {
		msg = "ERR " + msg
	}
	return Value{Type: TypeError, Str: msg}
}

func Errf(format string, args ...any) Value { return Err(fmt.Sprintf(format, args...)) }

// Map builds a map from alternating key/value elements.
func Map(kv ...Value) Value {
	if len(kv)%2 != 0 {
		panic("resp.Map: odd number of elements")
	}
	return Value{Type: TypeMap, Elems: nonNil(kv)}
}

// Strings builds an array of bulk strings.
func Strings(ss []string) Value {
	vs := make([]Value, len(ss))
	for i, s := range ss {
		vs[i] = Bulk(s)
	}
	return Array(vs...)
}

// StringSet builds a RESP3 set (array in RESP2) of bulk strings.
func StringSet(ss []string) Value {
	v := Strings(ss)
	v.Type = TypeSet
	return v
}

// FormatFloat formats a double the way Redis does in RESP2 replies.
func FormatFloat(f float64) string {
	switch {
	case f != f:
		return "nan"
	case f > 1e308:
		return "inf"
	case f < -1e308:
		return "-inf"
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// String returns the value as a plain string (bulk/simple/integer/double); handy in tests and clients.
func (v Value) String() string {
	switch v.Type {
	case TypeSimple, TypeBulk, TypeError:
		return v.Str
	case TypeInteger:
		return strconv.FormatInt(v.Int, 10)
	case TypeDouble:
		return FormatFloat(v.Float)
	case TypeBool:
		if v.Bool {
			return "1"
		}
		return "0"
	case TypeNull:
		return ""
	}
	return fmt.Sprintf("%v", v.Elems)
}

func hasCode(msg string) bool {
	for i := 0; i < len(msg); i++ {
		c := msg[i]
		if c == ' ' {
			return i > 0
		}
		if c < 'A' || c > 'Z' {
			return false
		}
	}
	return len(msg) > 0
}

func nonNil(vs []Value) []Value {
	if vs == nil {
		return []Value{}
	}
	return vs
}
