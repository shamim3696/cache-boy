package command

import (
	"log"
	"strconv"
	"strings"
	"time"

	"cache-boy/internal/feature/scripting"
	"cache-boy/internal/protocol/resp"
)

// Server-side JavaScript (feature/scripting). EVAL holds every shard lock
// while the script runs, so scripts are atomic. EVAL itself is never
// propagated: the commands it runs propagate their own effects.
//
// KEYS seen by the script are user keys (namespace removed), because the
// commands run by cb.call are namespaced again by the dispatcher.

func init() {
	const f = FlagAllKeys | FlagNoPropagate | FlagNoScript
	Register(
		&Spec{Name: "eval", Arity: -3, Flags: f, Group: "scripting", KeysFn: evalKeys,
			Summary: "EVAL script numkeys [key ...] [arg ...] — runs JavaScript atomically", Handler: evalCmd},
		&Spec{Name: "evalsha", Arity: -3, Flags: f, Group: "scripting", KeysFn: evalKeys,
			Summary: "EVALSHA sha1 numkeys [key ...] [arg ...] — runs a loaded script", Handler: evalCmd},
		&Spec{Name: "script", Arity: -2, Flags: FlagNoScript, Group: "scripting",
			Summary: "SCRIPT LOAD script | EXISTS sha1 [sha1 ...] | FLUSH", Handler: scriptCmd},
	)
}

// evalKeys returns the key positions of EVAL/EVALSHA (after numkeys).
func evalKeys(args []string) []int {
	if len(args) < 3 {
		return nil
	}
	n, err := strconv.Atoi(args[2])
	if err != nil || n <= 0 || 3+n > len(args) {
		return nil
	}
	out := make([]int, n)
	for i := range out {
		out[i] = 3 + i
	}
	return out
}

func evalCmd(x *Ctx) resp.Value {
	n, ok := x.Int(2)
	if !ok {
		return ErrNotInt
	}
	if n < 0 {
		return resp.Err("ERR Number of keys can't be negative")
	}
	if 3+n > int64(x.NArgs()) {
		return resp.Err("ERR Number of keys can't be greater than number of args")
	}
	keys := make([]string, n)
	for i := range keys {
		k := x.Args[3+i]
		if u, ok := x.UserKey(k); ok {
			k = u
		}
		keys[i] = k
	}
	env := scripting.Env{
		Keys: keys,
		Argv: append([]string(nil), x.Args[3+n:]...),
		Call: func(args []string) resp.Value { return scriptCall(x, args) },
		Log:  func(msg string) { log.Printf("script (client %d): %s", x.C.ID, msg) },
	}
	if ms := x.E.Cfg.Get().ScriptTimeoutMs; ms > 0 {
		env.Timeout = time.Duration(ms) * time.Millisecond
	}
	if x.Name == "evalsha" {
		return x.E.Scripts.EvalSHA(x.Arg(1), env)
	}
	return x.E.Scripts.Eval(x.Arg(1), env)
}

// scriptCall runs one cb.call/cb.pcall command. Every shard is locked.
func scriptCall(x *Ctx, args []string) resp.Value {
	spec := Lookup(args[0])
	if spec == nil {
		return resp.Errf("ERR unknown command '%s'", args[0])
	}
	if spec.Flags&FlagNoScript != 0 || spec.Name == "multi" {
		return resp.Errf("ERR This command is not allowed from script: '%s'", strings.ToLower(args[0]))
	}
	return x.E.ExecLocked(x.C, args)
}

func scriptCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "load":
		if x.NArgs() != 3 {
			return wrongArgs("script|load")
		}
		sha, err := x.E.Scripts.Load(x.Arg(2))
		if err != nil {
			return resp.Errf("ERR Error compiling script: %s", strings.ReplaceAll(err.Error(), "\n", " "))
		}
		return resp.Bulk(sha)
	case "exists":
		if x.NArgs() < 3 {
			return wrongArgs("script|exists")
		}
		out := make([]resp.Value, 0, x.NArgs()-2)
		for _, sha := range x.Args[2:] {
			out = append(out, featInt01(x.E.Scripts.Exists(sha)))
		}
		return resp.Array(out...)
	case "flush":
		if x.NArgs() > 3 || (x.NArgs() == 3 && x.ArgLower(2) != "async" && x.ArgLower(2) != "sync") {
			return ErrSyntax
		}
		x.E.Scripts.Flush()
		return resp.OK
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try SCRIPT LOAD|EXISTS|FLUSH.", x.Arg(1))
}
