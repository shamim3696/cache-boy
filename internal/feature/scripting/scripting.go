// Package scripting runs server-side JavaScript (EVAL/EVALSHA) with goja.
//
// The package doesn't know the command engine: Run receives a Call function
// that executes one command and returns its reply. Atomicity (holding every
// shard lock while the script runs) is the caller's job.
//
// Inside a script: KEYS and ARGV (arrays of strings), cb.call(cmd, ...args)
// (throws on an error reply), cb.pcall (returns {err: msg} instead) and
// cb.log(...). The script body is wrapped in a function, so `return` works.
package scripting

import (
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"

	"cache-boy/internal/protocol/resp"
)

type script struct {
	src  string
	prog *goja.Program
}

// Scripts is the SCRIPT LOAD cache (sha1 → compiled script).
type Scripts struct {
	mu    sync.RWMutex
	cache map[string]*script
}

func New() *Scripts { return &Scripts{cache: map[string]*script{}} }

// SHA1 returns the lowercase hex sha1 of src.
func SHA1(src string) string {
	h := sha1.Sum([]byte(src))
	return hex.EncodeToString(h[:])
}

// Load compiles src, caches it and returns its sha1.
func (s *Scripts) Load(src string) (string, error) {
	_, sha, err := s.compile(src)
	return sha, err
}

func (s *Scripts) compile(src string) (*script, string, error) {
	sha := SHA1(src)
	s.mu.RLock()
	sc := s.cache[sha]
	s.mu.RUnlock()
	if sc != nil {
		return sc, sha, nil
	}
	prog, err := goja.Compile("script_"+sha[:8], "(function(){\n"+src+"\n})()", false)
	if err != nil {
		return nil, sha, err
	}
	sc = &script{src: src, prog: prog}
	s.mu.Lock()
	s.cache[sha] = sc
	s.mu.Unlock()
	return sc, sha, nil
}

// Exists reports whether sha is cached (case-insensitive).
func (s *Scripts) Exists(sha string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cache[strings.ToLower(sha)] != nil
}

// Flush empties the cache.
func (s *Scripts) Flush() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache = map[string]*script{}
}

// Len returns the number of cached scripts.
func (s *Scripts) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.cache)
}

// SHAs returns every cached sha1, sorted.
func (s *Scripts) SHAs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.cache))
	for k := range s.cache {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Env is what a script run gets from the server.
type Env struct {
	Keys, Argv []string
	// Call runs one command (args[0] is its name) and returns the reply.
	Call func(args []string) resp.Value
	// Log receives cb.log output (default: the standard logger).
	Log func(msg string)
	// Timeout interrupts the script (0 = none).
	Timeout time.Duration
}

// ErrNoScript is returned by RunSHA for an unknown sha1.
var ErrNoScript = errors.New("NOSCRIPT No matching script. Please use EVAL.")

// Eval compiles (and caches) src, then runs it.
func (s *Scripts) Eval(src string, env Env) resp.Value {
	sc, _, err := s.compile(src)
	if err != nil {
		return resp.Errf("ERR Error compiling script: %s", oneLine(err.Error()))
	}
	return run(sc, env)
}

// EvalSHA runs a cached script.
func (s *Scripts) EvalSHA(sha string, env Env) resp.Value {
	s.mu.RLock()
	sc := s.cache[strings.ToLower(sha)]
	s.mu.RUnlock()
	if sc == nil {
		return resp.Err(ErrNoScript.Error())
	}
	return run(sc, env)
}

// replyError carries an error reply thrown by cb.call through the JS stack,
// so the script's caller gets the original error (e.g. WRONGTYPE).
const errMarker = "__cb_err"

type timeoutSignal struct{}

func run(sc *script, env Env) (out resp.Value) {
	vm := goja.New()
	logf := env.Log
	if logf == nil {
		logf = func(msg string) { log.Printf("script: %s", msg) }
	}

	set := func(name string, v any) {
		if err := vm.Set(name, v); err != nil {
			panic(err)
		}
	}
	set("KEYS", strArray(vm, env.Keys))
	set("ARGV", strArray(vm, env.Argv))

	call := func(protected bool) func(goja.FunctionCall) goja.Value {
		return func(fc goja.FunctionCall) goja.Value {
			if len(fc.Arguments) == 0 {
				panic(vm.NewTypeError("cb.call needs at least a command name"))
			}
			args := make([]string, len(fc.Arguments))
			for i, a := range fc.Arguments {
				args[i] = argString(a)
			}
			reply := env.Call(args)
			if reply.IsError() {
				if protected {
					o := vm.NewObject()
					o.Set("err", reply.Str)
					return o
				}
				e := vm.NewGoError(errors.New(reply.Str))
				e.Set(errMarker, reply.Str)
				panic(e)
			}
			return toJS(vm, reply)
		}
	}
	cb := vm.NewObject()
	cb.Set("call", call(false))
	cb.Set("pcall", call(true))
	cb.Set("log", func(fc goja.FunctionCall) goja.Value {
		parts := make([]string, len(fc.Arguments))
		for i, a := range fc.Arguments {
			parts[i] = a.String()
		}
		logf(strings.Join(parts, " "))
		return goja.Undefined()
	})
	set("cb", cb)

	if env.Timeout > 0 {
		t := time.AfterFunc(env.Timeout, func() { vm.Interrupt(timeoutSignal{}) })
		defer t.Stop()
	}
	v, err := vm.RunProgram(sc.prog)
	if err != nil {
		return errorReply(err)
	}
	return fromJS(vm, v, 0)
}

func errorReply(err error) resp.Value {
	var ie *goja.InterruptedError
	if errors.As(err, &ie) {
		if _, ok := ie.Value().(timeoutSignal); ok {
			return resp.Err("ERR script timed out (script-timeout exceeded)")
		}
		return resp.Errf("ERR script interrupted: %v", ie.Value())
	}
	var ex *goja.Exception
	if errors.As(err, &ex) {
		if o, ok := ex.Value().(*goja.Object); ok {
			if m := o.Get(errMarker); m != nil && !goja.IsUndefined(m) {
				return resp.Err(m.String())
			}
		}
		return resp.Errf("ERR Error running script: %s", oneLine(ex.Value().String()))
	}
	return resp.Errf("ERR Error running script: %s", oneLine(err.Error()))
}

func oneLine(s string) string {
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}

func strArray(vm *goja.Runtime, ss []string) *goja.Object {
	items := make([]any, len(ss))
	for i, s := range ss {
		items[i] = s
	}
	return vm.NewArray(items...)
}

// argString converts a cb.call argument to a command argument.
func argString(v goja.Value) string {
	switch x := v.Export().(type) {
	case int64:
		return strconv.FormatInt(x, 10)
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1<<53 {
			return strconv.FormatInt(int64(x), 10)
		}
		return resp.FormatFloat(x)
	case bool:
		if x {
			return "1"
		}
		return "0"
	}
	return v.String()
}

// toJS converts a command reply to a JavaScript value.
func toJS(vm *goja.Runtime, v resp.Value) goja.Value {
	switch v.Type {
	case resp.TypeSimple, resp.TypeBulk:
		return vm.ToValue(v.Str)
	case resp.TypeInteger:
		return vm.ToValue(v.Int)
	case resp.TypeDouble:
		return vm.ToValue(v.Float)
	case resp.TypeBool:
		return vm.ToValue(v.Bool)
	case resp.TypeNull:
		return goja.Null()
	case resp.TypeError:
		o := vm.NewObject()
		o.Set("err", v.Str)
		return o
	case resp.TypeMap:
		o := vm.NewObject()
		for i := 0; i+1 < len(v.Elems); i += 2 {
			o.Set(v.Elems[i].String(), toJS(vm, v.Elems[i+1]))
		}
		return o
	case resp.TypeArray, resp.TypeSet, resp.TypePush:
		items := make([]any, len(v.Elems))
		for i, e := range v.Elems {
			items[i] = toJS(vm, e)
		}
		return vm.NewArray(items...)
	}
	return goja.Null()
}

const maxDepth = 64

// fromJS converts a script's return value to a reply.
func fromJS(vm *goja.Runtime, v goja.Value, depth int) resp.Value {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		return resp.Null()
	}
	if depth > maxDepth {
		return resp.Err("ERR reply nested too deeply")
	}
	switch x := v.Export().(type) {
	case string:
		return resp.Bulk(x)
	case int64:
		return resp.Int(x)
	case float64:
		if x == math.Trunc(x) && !math.IsInf(x, 0) && math.Abs(x) < 1<<63 {
			return resp.Int(int64(x))
		}
		return resp.Bulk(resp.FormatFloat(x))
	case bool:
		if x {
			return resp.Int(1)
		}
		return resp.Null()
	}
	o, ok := v.(*goja.Object)
	if !ok {
		return resp.Bulk(v.String())
	}
	if o.ClassName() == "Array" {
		n := int(o.Get("length").ToInteger())
		out := make([]resp.Value, n)
		for i := 0; i < n; i++ {
			out[i] = fromJS(vm, o.Get(strconv.Itoa(i)), depth+1)
		}
		return resp.Array(out...)
	}
	if e := o.Get("err"); e != nil && !goja.IsUndefined(e) {
		return resp.Err(e.String())
	}
	if s := o.Get("ok"); s != nil && !goja.IsUndefined(s) {
		return resp.Simple(s.String())
	}
	if o.ClassName() == "Error" {
		return resp.Errf("ERR %s", oneLine(o.String()))
	}
	// Any other object: a map of its own enumerable properties.
	keys := o.Keys()
	kv := make([]resp.Value, 0, 2*len(keys))
	for _, k := range keys {
		kv = append(kv, resp.Bulk(k), fromJS(vm, o.Get(k), depth+1))
	}
	return resp.Map(kv...)
}

// String describes the scripts cache (for INFO).
func (s *Scripts) String() string { return fmt.Sprintf("scripts=%d", s.Len()) }
