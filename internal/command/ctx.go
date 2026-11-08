package command

import (
	"strings"
	"time"

	"github.com/shamim3696/cache-boy/internal/feature/namespace"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

// Ctx is passed to command handlers.
type Ctx struct {
	E    *Engine
	C    *Client
	Args []string // Args[0] is the command name as sent; key args are already namespaced
	Name string   // lowercase command name
	Keys []string // key arguments (namespaced), locked by the dispatcher

	// InExec is true inside MULTI/EXEC and scripts (all shards already locked).
	InExec bool

	// Blocking support (see Block).
	WouldBlock   bool
	BlockTimeout time.Duration // 0 = forever
	BlockOn      []string      // keys to wait on; defaults to Keys
	// TimedOut is set when the handler is re-run after the block timeout.
	TimedOut bool

	spec      *Spec
	propagate [][]string
	noChange  bool
}

func (x *Ctx) resetAttempt() {
	x.WouldBlock = false
	x.propagate = nil
	x.noChange = false
}

func (x *Ctx) DB() *store.DB { return x.E.DB }

// Arg returns argument i ("" if out of range).
func (x *Ctx) Arg(i int) string {
	if i < len(x.Args) {
		return x.Args[i]
	}
	return ""
}

// ArgLower returns argument i in lower case (for option names).
func (x *Ctx) ArgLower(i int) string { return strings.ToLower(x.Arg(i)) }

func (x *Ctx) NArgs() int { return len(x.Args) }

func (x *Ctx) Int(i int) (int64, bool)     { return ParseInt(x.Arg(i)) }
func (x *Ctx) Float(i int) (float64, bool) { return ParseFloat(x.Arg(i)) }

// Now returns the current time in unix milliseconds.
func (x *Ctx) Now() int64 { return time.Now().UnixMilli() }

// Block asks the dispatcher to wait until one of keys (default: the command's
// keys) is written or timeout passes, then call the handler again. When the
// timeout passes the handler runs once more with TimedOut set and must return
// a final reply. Typical use:
//
//	if v, ok := tryPop(); ok { return v }
//	if ctx.TimedOut { return resp.NullArray() }
//	return ctx.Block(timeout)
func (x *Ctx) Block(timeout time.Duration, keys ...string) resp.Value {
	x.WouldBlock = true
	x.BlockTimeout = timeout
	if len(keys) > 0 {
		x.BlockOn = keys
	}
	return resp.None()
}

// Propagate replaces what is written to AOF/replicas for this command (may
// be called several times to emit several commands). Use it to make
// non-deterministic or relative commands safe to replay, e.g. turning
// "EXPIRE k 10" into "PEXPIREAT k <abs ms>".
func (x *Ctx) Propagate(args ...string) {
	x.propagate = append(x.propagate, args)
}

// NoChange tells the dispatcher the write command changed nothing (SETNX on
// an existing key, DEL of a missing key): no key events, nothing propagated.
func (x *Ctx) NoChange() { x.noChange = true }

// Emit sends a key event for a key that isn't one of the command's key
// arguments (e.g. keys deleted by TAG.INVALIDATE or FLUSHALL).
func (x *Ctx) Emit(key string) {
	x.E.DB.Emit(store.Event{Key: key, Op: x.Name, ClientID: x.C.ID})
}

// PropagateNow writes a command to AOF/replicas immediately. For FlagAllKeys
// commands that touch keys not in their arguments; call it while still
// holding the locks (i.e. inside the handler).
func (x *Ctx) PropagateNow(args ...string) { x.E.propagateRaw(args) }

// UserKey converts a stored key to what the client sees (namespace removed).
// ok is false if the key belongs to another namespace.
func (x *Ctx) UserKey(stored string) (string, bool) { return namespace.Strip(x.C.Namespace, stored) }

// StoredKey converts a client key to the stored key (namespace added). Only
// needed for keys that are not declared key arguments.
func (x *Ctx) StoredKey(user string) string { return namespace.Prefix(x.C.Namespace) + user }

// Proto returns the client's protocol version (2 or 3).
func (x *Ctx) Proto() int { return x.C.Proto() }
