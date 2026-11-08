// Package apitest provides minimal stand-ins for data commands (GET, SET,
// TYPE...) so the API, WebSocket and client packages can be tested before
// the real implementations are merged. Each command is registered only if no
// command of that name exists, so the real ones always win. Only tests
// import this package.
package apitest

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

var once sync.Once

// Register adds the fallback commands (idempotent).
func Register() {
	once.Do(func() {
		for _, s := range specs() {
			if command.Lookup(s.Name) == nil {
				command.Register(s)
			}
		}
	})
}

func specs() []*command.Spec {
	return []*command.Spec{
		{Name: "get", Arity: 2, Flags: command.FlagReadOnly, Group: "string", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			e := x.DB().Get(x.Keys[0])
			if e == nil {
				return resp.Null()
			}
			if e.Kind != store.KindString {
				return command.ErrWrongTyp
			}
			return resp.Bytes(e.Str().Bytes())
		}},
		{Name: "set", Arity: -3, Flags: command.FlagWrite | command.FlagDenyOOM, Group: "string", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			en := x.DB().NewStringValue([]byte(x.Arg(2)))
			for i := 3; i < x.NArgs(); i++ {
				switch x.ArgLower(i) {
				case "ex", "px":
					n, ok := x.Int(i + 1)
					if !ok || n <= 0 {
						return command.ErrNotInt
					}
					d := time.Duration(n) * time.Millisecond
					if x.ArgLower(i) == "ex" {
						d = time.Duration(n) * time.Second
					}
					en.ExpireAt = time.Now().Add(d).UnixMilli()
					i++
				case "nx":
					if x.DB().Get(x.Keys[0]) != nil {
						return resp.Null()
					}
				case "xx":
					if x.DB().Get(x.Keys[0]) == nil {
						return resp.Null()
					}
				case "tags":
					i = x.NArgs()
				default:
					return command.ErrSyntax
				}
			}
			x.DB().Put(x.Keys[0], en)
			return resp.OK
		}},
		{Name: "del", Arity: -2, Flags: command.FlagWrite, Group: "keyspace", FirstKey: 1, LastKey: -1, Handler: func(x *command.Ctx) resp.Value {
			n := 0
			for _, k := range x.Keys {
				if x.DB().Peek(k) != nil && x.DB().Delete(k) {
					n++
				}
			}
			return resp.Int(int64(n))
		}},
		{Name: "type", Arity: 2, Flags: command.FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			e := x.DB().Peek(x.Keys[0])
			if e == nil {
				return resp.Simple("none")
			}
			return resp.Simple(e.Kind.String())
		}},
		{Name: "pttl", Arity: 2, Flags: command.FlagReadOnly, Group: "keyspace", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			e := x.DB().Peek(x.Keys[0])
			switch {
			case e == nil:
				return resp.Int(-2)
			case e.ExpireAt == 0:
				return resp.Int(-1)
			}
			return resp.Int(max(e.ExpireAt-time.Now().UnixMilli(), 0))
		}},
		{Name: "incr", Arity: 2, Flags: command.FlagWrite, Group: "string", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			var n int64
			if e := x.DB().Get(x.Keys[0]); e != nil {
				v, err := strconv.ParseInt(string(e.Str().Bytes()), 10, 64)
				if err != nil {
					return command.ErrNotInt
				}
				n = v
			}
			n++
			x.DB().Put(x.Keys[0], x.DB().NewStringValue([]byte(strconv.FormatInt(n, 10))))
			return resp.Int(n)
		}},
		{Name: "hset", Arity: -4, Flags: command.FlagWrite, Group: "hash", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			e := x.DB().Get(x.Keys[0])
			if e == nil {
				e = store.NewEntry(store.KindHash, store.NewHash())
				x.DB().Put(x.Keys[0], e)
			}
			n := 0
			for i := 2; i+1 < x.NArgs(); i += 2 {
				if e.Hash().Set(x.Arg(i), x.Arg(i+1)) {
					n++
				}
			}
			x.DB().Updated(x.Keys[0])
			return resp.Int(int64(n))
		}},
		{Name: "hgetall", Arity: 2, Flags: command.FlagReadOnly, Group: "hash", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			e := x.DB().Get(x.Keys[0])
			if e == nil {
				return resp.Map()
			}
			var kv []resp.Value
			for k, v := range e.Hash().Map() {
				kv = append(kv, resp.Bulk(k), resp.Bulk(v))
			}
			return resp.Map(kv...)
		}},
		{Name: "smembers", Arity: 2, Flags: command.FlagReadOnly, Group: "set", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			return resp.StringSet(nil)
		}},
		// test.block waits until the client goes away (a stand-in for BLPOP key 0).
		{Name: "test.block", Arity: 2, Flags: command.FlagBlocking, Group: "list", FirstKey: 1, LastKey: 1, Handler: func(x *command.Ctx) resp.Value {
			if x.TimedOut {
				return resp.NullArray()
			}
			return x.Block(0)
		}},
		// test.echoargs returns its arguments joined by spaces.
		{Name: "test.echoargs", Arity: -1, Group: "server", Handler: func(x *command.Ctx) resp.Value {
			return resp.Bulk(strings.Join(x.Args[1:], " "))
		}},
	}
}
