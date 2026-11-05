package command

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cache-boy/internal/auth"
	"cache-boy/internal/blocking"
	"cache-boy/internal/config"
	"cache-boy/internal/eviction"
	"cache-boy/internal/expiry"
	"cache-boy/internal/feature/history"
	"cache-boy/internal/feature/lock"
	"cache-boy/internal/feature/namespace"
	"cache-boy/internal/feature/queue"
	"cache-boy/internal/feature/ratelimit"
	"cache-boy/internal/feature/scripting"
	"cache-boy/internal/feature/singleflight"
	"cache-boy/internal/feature/tags"
	"cache-boy/internal/feature/watch"
	"cache-boy/internal/observability"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/pubsub"
	"cache-boy/internal/store"
	"cache-boy/internal/tracking"
	"cache-boy/internal/transaction"
)

var (
	posInf = math.Inf(1)
	negInf = math.Inf(-1)
)

// Engine owns the dataset and every subsystem, and executes commands.
type Engine struct {
	Cfg      *config.Store
	DB       *store.DB
	Auth     *auth.Store
	PubSub   *pubsub.Hub
	Tx       *transaction.Manager
	Blocking *blocking.Queue
	Tracking *tracking.Table
	Evictor  *eviction.Evictor
	Metrics  *observability.Metrics
	Slowlog  *observability.Slowlog
	Monitor  *observability.Monitor

	Tags      *tags.Index
	RateLimit *ratelimit.Limiter
	Locks     *lock.Manager
	Queues    *queue.Manager
	Watch     *watch.Hub
	Flight    *singleflight.Group
	History   *history.Store
	Scripts   *scripting.Scripts

	// Optional subsystems, set by main before serving (nil = disabled).
	Persist Persister
	Repl    Replicator
	Cluster ClusterRouter

	StartTime time.Time
	RunID     string

	propagators atomic.Pointer[[]Propagator]
	dumpers     []Dumper
	loading     atomic.Bool

	clientsMu sync.RWMutex
	clients   map[int64]*Client
	nextID    atomic.Int64
	onClose   []func(*Client)

	stop     chan struct{}
	stopOnce sync.Once
}

func New(cfg *config.Store) *Engine {
	c := cfg.Get()
	db := store.New(c.Shards)
	db.CompressThreshold.Store(int64(c.CompressionThreshold))
	ps := pubsub.New()
	e := &Engine{
		Cfg:       cfg,
		DB:        db,
		Auth:      auth.New(c.RequirePass),
		PubSub:    ps,
		Tx:        transaction.New(db),
		Blocking:  blocking.New(db),
		Tracking:  tracking.New(db),
		Evictor:   eviction.New(db, cfg),
		Metrics:   observability.NewMetrics(),
		Slowlog:   observability.NewSlowlog(),
		Monitor:   observability.NewMonitor(),
		Tags:      tags.New(db),
		RateLimit: ratelimit.New(),
		Locks:     lock.New(),
		Queues:    queue.New(),
		Watch:     watch.New(db, ps),
		Flight:    singleflight.New(db),
		History:   history.New(db, cfg),
		Scripts:   scripting.New(),
		StartTime: time.Now(),
		RunID:     randomHex(20),
		clients:   map[int64]*Client{},
		stop:      make(chan struct{}),
	}
	e.propagators.Store(&[]Propagator{})
	for _, m := range []any{e.Tags, e.RateLimit, e.Locks, e.Queues, e.Watch, e.Flight, e.History} {
		if d, ok := m.(Dumper); ok {
			e.dumpers = append(e.dumpers, d)
		}
	}
	return e
}

// Start launches background jobs (active expiry).
func (e *Engine) Start() {
	go expiry.Run(e.DB, e.Cfg, e.stop)
}

// Stop ends background jobs and wakes blocked clients.
func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		close(e.stop)
		e.Watch.Close()
	})
}

// Stopping is closed when the server shuts down.
func (e *Engine) Stopping() <-chan struct{} { return e.stop }

// AddPropagator registers a receiver of write commands (AOF, replication).
func (e *Engine) AddPropagator(p Propagator) {
	for {
		old := e.propagators.Load()
		next := append(append([]Propagator{}, *old...), p)
		if e.propagators.CompareAndSwap(old, &next) {
			return
		}
	}
}

// RemovePropagator unregisters p.
func (e *Engine) RemovePropagator(p Propagator) {
	for {
		old := e.propagators.Load()
		next := make([]Propagator, 0, len(*old))
		for _, x := range *old {
			if x != p {
				next = append(next, x)
			}
		}
		if e.propagators.CompareAndSwap(old, &next) {
			return
		}
	}
}

// SetLoading marks the engine as replaying AOF/snapshot data: writes are not
// propagated and memory limits are not enforced.
func (e *Engine) SetLoading(b bool) { e.loading.Store(b) }
func (e *Engine) Loading() bool     { return e.loading.Load() }

// DumpModules emits commands that recreate the state feature modules keep
// outside the keyspace (queues, tags, webhooks...). Snapshots store keys in
// binary form and append these commands. The caller must hold DB.LockAll().
func (e *Engine) DumpModules(emit func(args []string)) {
	for _, d := range e.dumpers {
		d.Dump(emit)
	}
}

// --- clients ---

// NewClient registers a client. kind is "tcp", "http" or "ws".
func (e *Engine) NewClient(kind, addr string) *Client {
	c := newClient(e.nextID.Add(1), kind, addr)
	if e.Auth.AutoLogin() {
		c.User = auth.DefaultUser
	}
	e.clientsMu.Lock()
	e.clients[c.ID] = c
	e.clientsMu.Unlock()
	e.Metrics.ConnectedClients.Add(1)
	e.Metrics.TotalConnections.Add(1)
	return c
}

// InternalClient returns an authenticated client that is not listed in
// CLIENT LIST, for AOF loading, scripts and the replication link.
// With master=true it may write even on a read-only replica.
func (e *Engine) InternalClient(kind string, master bool) *Client {
	c := newClient(e.nextID.Add(1), kind, kind)
	c.User = auth.DefaultUser
	c.IsMaster = master
	return c
}

// RemoveClient cleans up after a disconnect.
func (e *Engine) RemoveClient(c *Client) {
	c.Close()
	e.clientsMu.Lock()
	_, ok := e.clients[c.ID]
	delete(e.clients, c.ID)
	e.clientsMu.Unlock()
	if ok {
		e.Metrics.ConnectedClients.Add(-1)
	}
	e.PubSub.RemoveAll(c)
	e.Tracking.Disable(c)
	e.Tx.Unwatch(c.Watcher)
	e.Monitor.Remove(c)
	for _, fn := range e.onClose {
		fn(c)
	}
}

// OnClientClose registers cleanup for feature modules. Call before serving.
func (e *Engine) OnClientClose(fn func(*Client)) { e.onClose = append(e.onClose, fn) }

// Clients returns connected clients sorted by ID.
func (e *Engine) Clients() []*Client {
	e.clientsMu.RLock()
	out := make([]*Client, 0, len(e.clients))
	for _, c := range e.clients {
		out = append(out, c)
	}
	e.clientsMu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func (e *Engine) ClientByID(id int64) *Client {
	e.clientsMu.RLock()
	defer e.clientsMu.RUnlock()
	return e.clients[id]
}

// --- execution ---

// Execute runs one command for a client and returns the reply.
func (e *Engine) Execute(c *Client, args []string) resp.Value {
	if len(args) == 0 {
		return resp.None()
	}
	start := time.Now()
	c.lastActive.Store(start.UnixMilli())
	name := strings.ToLower(args[0])
	c.lastCmd.Store(&name)

	spec := registry[name]
	if spec == nil {
		e.failMulti(c)
		return resp.Errf("ERR unknown command '%s', with args beginning with: %s", args[0], quoteArgs(args[1:]))
	}
	if !spec.arityOK(len(args)) {
		e.failMulti(c)
		return wrongArgs(name)
	}
	if !c.Authenticated() && spec.Flags&FlagNoAuth == 0 {
		return resp.Err("NOAUTH Authentication required.")
	}
	if c.Multi != nil && spec.Flags&FlagNoMulti == 0 {
		c.Multi.Commands = append(c.Multi.Commands, append([]string(nil), args...))
		return resp.Simple("QUEUED")
	}
	if c.Proto() == 2 && spec.Flags&FlagPubSub == 0 && e.PubSub.Count(c) > 0 {
		return resp.Errf("ERR Can't execute '%s': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context", name)
	}

	ctx := &Ctx{E: e, C: c, Args: args, Name: name, spec: spec}
	v := e.run(ctx, false)

	dur := time.Since(start)
	spec.calls.Add(1)
	e.Metrics.Observe(name, dur, v.IsError())
	if spec.Flags&FlagBlocking == 0 {
		cfg := e.Cfg.Get()
		e.Slowlog.Record(cfg.SlowlogLogSlowerThan, cfg.SlowlogMaxLen, dur, args, c.Addr, c.Name())
	}
	if e.Monitor.Active() {
		e.Monitor.Feed(c.ID, c.Addr, args)
	}
	return v
}

// ExecLocked runs a command while the caller already holds DB.LockAll()
// (inside EXEC and scripts). Blocking commands behave as if they timed out.
func (e *Engine) ExecLocked(c *Client, args []string) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR empty command")
	}
	name := strings.ToLower(args[0])
	spec := registry[name]
	if spec == nil {
		return resp.Errf("ERR unknown command '%s'", args[0])
	}
	if !spec.arityOK(len(args)) {
		return wrongArgs(name)
	}
	ctx := &Ctx{E: e, C: c, Args: args, Name: name, spec: spec, InExec: true}
	v := e.run(ctx, true)
	spec.calls.Add(1)
	e.Metrics.Observe(name, 0, v.IsError())
	return v
}

func (e *Engine) run(ctx *Ctx, locked bool) resp.Value {
	spec, c := ctx.spec, ctx.C

	idx := spec.KeyIndexes(ctx.Args)
	keys := make([]string, len(idx))
	for i, j := range idx {
		keys[i] = ctx.Args[j]
	}
	if !c.IsMaster && c.Authenticated() { // unauthenticated clients only reach FlagNoAuth commands
		if err := e.Auth.Check(c.User, spec.Name, spec.Categories(), keys); err != nil {
			return resp.Err(err.Error())
		}
	}
	if c.Namespace != "" {
		p := namespace.Prefix(c.Namespace)
		for i, j := range idx {
			ctx.Args[j] = p + ctx.Args[j]
			keys[i] = ctx.Args[j]
		}
	}
	ctx.Keys = keys

	if e.Cluster != nil && len(keys) > 0 && !c.IsMaster && e.Cluster.Enabled() {
		if err := e.Cluster.Check(keys); err != nil {
			return resp.Err(err.Error())
		}
	}
	write := spec.Flags&FlagWrite != 0
	if write && !c.IsMaster && e.Repl != nil && e.Repl.IsReplica() {
		return resp.Err("READONLY You can't write against a read only replica.")
	}
	if !locked && spec.Flags&FlagDenyOOM != 0 && !e.loading.Load() {
		if err := e.Evictor.Free(); err != nil {
			return resp.Err(err.Error())
		}
	}

	var deadline time.Time
	for {
		unlock := func() {}
		if !locked {
			if spec.Flags&FlagAllKeys != 0 {
				unlock = e.DB.LockAll()
			} else {
				unlock = e.DB.Lock(keys...)
			}
		}
		ctx.resetAttempt()
		v := e.call(ctx)

		if ctx.WouldBlock && !locked && !ctx.TimedOut {
			if deadline.IsZero() && ctx.BlockTimeout > 0 {
				deadline = time.Now().Add(ctx.BlockTimeout)
			}
			waitKeys := ctx.BlockOn
			if waitKeys == nil {
				waitKeys = keys
			}
			w := e.Blocking.Register(waitKeys...)
			unlock()
			timedOut := e.wait(c, w, deadline)
			e.Blocking.Unregister(w)
			if c.Closed() {
				return resp.None()
			}
			ctx.TimedOut = timedOut
			continue
		}
		if ctx.WouldBlock { // can't block inside MULTI/scripts: act as a timeout
			ctx.resetAttempt()
			ctx.TimedOut = true
			v = e.call(ctx)
			if ctx.WouldBlock {
				v = resp.NullArray()
			}
		}
		if !v.IsError() {
			if write {
				e.afterWrite(ctx)
			}
			if c.Tracking && spec.Flags&FlagReadOnly != 0 && len(keys) > 0 {
				e.Tracking.Remember(c, keys)
			}
		}
		unlock()
		return v
	}
}

func (e *Engine) call(ctx *Ctx) (v resp.Value) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("panic in command %q: %v\n%s", ctx.Name, r, debug.Stack())
			v = resp.Errf("ERR internal error in '%s': %v", ctx.Name, r)
		}
	}()
	return ctx.spec.Handler(ctx)
}

func (e *Engine) wait(c *Client, w *blocking.Waiter, deadline time.Time) (timedOut bool) {
	var timer <-chan time.Time
	if !deadline.IsZero() {
		d := time.Until(deadline)
		if d <= 0 {
			return true
		}
		t := time.NewTimer(d)
		defer t.Stop()
		timer = t.C
	}
	select {
	case <-w.C:
		return false
	case <-timer:
		return true
	case <-c.Done():
		return true
	case <-e.stop:
		return true
	}
}

// afterWrite emits key events and propagates the command. Runs under the
// command's shard locks so AOF/replica order matches execution order per key.
func (e *Engine) afterWrite(ctx *Ctx) {
	if ctx.noChange {
		return
	}
	for _, k := range ctx.Keys {
		e.DB.Emit(store.Event{Key: k, Op: ctx.Name, ClientID: ctx.C.ID})
	}
	if ctx.spec.Flags&FlagNoPropagate != 0 || e.loading.Load() {
		return
	}
	props := *e.propagators.Load()
	if len(props) == 0 {
		return
	}
	cmds := ctx.propagate
	if cmds == nil {
		cmds = [][]string{ctx.Args}
	}
	for _, p := range props {
		for _, cmd := range cmds {
			p.Propagate(cmd)
		}
	}
}

func (e *Engine) failMulti(c *Client) {
	if c.Multi != nil {
		c.Multi.Failed = true
	}
}

// propagateExtra sends a command to AOF/replicas from code that isn't a
// normal write handler (used by Ctx.Propagate for FlagAllKeys commands etc.).
func (e *Engine) propagateRaw(args []string) {
	if e.loading.Load() {
		return
	}
	for _, p := range *e.propagators.Load() {
		p.Propagate(args)
	}
}

func quoteArgs(args []string) string {
	var b strings.Builder
	for i, a := range args {
		if i == 8 {
			break
		}
		if len(a) > 32 {
			a = a[:32]
		}
		fmt.Fprintf(&b, "'%s' ", a)
	}
	return b.String()
}

func randomHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}
