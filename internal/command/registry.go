// Package command holds the command table and the dispatcher (Engine.Execute).
//
// Each command group lives in its own file and registers its commands from
// init(). A handler receives a *Ctx with the parsed arguments; the dispatcher
// has already checked arity, auth/ACL, MULTI queuing, namespaces, cluster
// slots, memory limits, and locked the shards of the command's keys.
package command

import (
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"cache-boy/internal/protocol/resp"
)

type Flag uint32

const (
	FlagWrite       Flag = 1 << iota // modifies data: emits key events, goes to AOF/replicas
	FlagReadOnly                     // reads data: client-side caching remembers its keys
	FlagAdmin                        // server administration
	FlagNoAuth                       // allowed before AUTH (AUTH, HELLO, QUIT)
	FlagAllKeys                      // locks every shard (KEYS, FLUSHALL, EXEC, EVAL...)
	FlagBlocking                     // may wait for a key to change (BLPOP)
	FlagNoMulti                      // runs immediately even inside MULTI (EXEC, DISCARD, WATCH)
	FlagPubSub                       // allowed in RESP2 subscriber mode
	FlagDenyOOM                      // may grow memory: refused/evicts when over maxmemory
	FlagNoPropagate                  // never written to AOF/replicas (EXEC, EVAL propagate their effects)
	FlagNoScript                     // not callable from scripts
	FlagLoading                      // allowed while loading data
)

// Spec describes one command.
type Spec struct {
	Name  string // lowercase
	Arity int    // Redis style, counting the name: N = exactly N args, -N = at least N
	Flags Flag
	Group string // string, list, hash, set, sortedset, json, timeseries, vector, keyspace,
	// pubsub, transaction, connection, server, scripting, cluster, features...

	// Key positions in Args (Args[0] is the command name). LastKey -1 means
	// the last argument, -2 the one before. FirstKey 0 means no keys.
	FirstKey, LastKey, KeyStep int
	// KeysFn overrides the positions above for commands like EVAL or ZUNION
	// where key positions depend on a numkeys argument.
	KeysFn func(args []string) []int

	Summary string
	Handler func(*Ctx) resp.Value

	calls atomic.Int64
}

var registry = map[string]*Spec{}

// Register adds commands to the table. Call it from init().
func Register(specs ...*Spec) {
	for _, s := range specs {
		s.Name = strings.ToLower(s.Name)
		if _, dup := registry[s.Name]; dup {
			panic("command registered twice: " + s.Name)
		}
		if s.KeyStep == 0 {
			s.KeyStep = 1
		}
		registry[s.Name] = s
	}
}

// Lookup returns the spec of a command (case-insensitive) or nil.
func Lookup(name string) *Spec { return registry[strings.ToLower(name)] }

// All returns every command, sorted by name.
func All() []*Spec {
	out := make([]*Spec, 0, len(registry))
	for _, s := range registry {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Spec) arityOK(n int) bool {
	if s.Arity >= 0 {
		return n == s.Arity
	}
	return n >= -s.Arity
}

// KeyIndexes returns the positions of key arguments in args.
func (s *Spec) KeyIndexes(args []string) []int {
	if s.KeysFn != nil {
		return s.KeysFn(args)
	}
	if s.FirstKey <= 0 || s.FirstKey >= len(args) {
		return nil
	}
	last := s.LastKey
	if last < 0 {
		last += len(args)
	}
	if last >= len(args) {
		last = len(args) - 1
	}
	var out []int
	for i := s.FirstKey; i <= last; i += s.KeyStep {
		out = append(out, i)
	}
	return out
}

// Categories returns ACL categories for the command.
func (s *Spec) Categories() []string {
	cats := []string{s.Group}
	if s.Flags&FlagWrite != 0 {
		cats = append(cats, "write")
	}
	if s.Flags&FlagReadOnly != 0 {
		cats = append(cats, "read")
	}
	if s.Flags&FlagAdmin != 0 {
		cats = append(cats, "admin", "dangerous")
	}
	if s.Flags&FlagBlocking != 0 {
		cats = append(cats, "blocking")
	}
	if s.Flags&FlagAllKeys != 0 && s.Flags&FlagAdmin == 0 && s.Group == "keyspace" {
		cats = append(cats, "slow")
	}
	return cats
}

// FlagNames lists flags for COMMAND INFO.
func (s *Spec) FlagNames() []string {
	names := []string{}
	for _, f := range []struct {
		f    Flag
		name string
	}{{FlagWrite, "write"}, {FlagReadOnly, "readonly"}, {FlagAdmin, "admin"}, {FlagNoAuth, "no_auth"},
		{FlagBlocking, "blocking"}, {FlagPubSub, "pubsub"}, {FlagDenyOOM, "denyoom"}, {FlagNoScript, "noscript"}} {
		if s.Flags&f.f != 0 {
			names = append(names, f.name)
		}
	}
	return names
}

// Calls returns how many times the command ran.
func (s *Spec) Calls() int64 { return s.calls.Load() }

// --- shared replies and argument parsing helpers ---

var (
	ErrSyntax   = resp.Err("ERR syntax error")
	ErrNotInt   = resp.Err("ERR value is not an integer or out of range")
	ErrNotFloat = resp.Err("ERR value is not a valid float")
	ErrWrongTyp = resp.Err("WRONGTYPE Operation against a key holding the wrong kind of value")
	ErrNoKey    = resp.Err("ERR no such key")
	ErrOverflow = resp.Err("ERR increment or decrement would overflow")
)

func ParseInt(s string) (int64, bool) {
	n, err := strconv.ParseInt(s, 10, 64)
	return n, err == nil
}

func ParseFloat(s string) (float64, bool) {
	switch strings.ToLower(s) {
	case "inf", "+inf":
		return posInf, true
	case "-inf":
		return negInf, true
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f != f { // reject NaN
		return 0, false
	}
	return f, true
}

func wrongArgs(name string) resp.Value {
	return resp.Errf("ERR wrong number of arguments for '%s' command", name)
}

func sortStrings(s []string) []string {
	sort.Strings(s)
	if s == nil {
		return []string{}
	}
	return s
}
