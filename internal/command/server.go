package command

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"cache-boy/internal/config"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/tracking"
)

// Version is the cache-boy release. redis_version in INFO is what client
// libraries check for feature support.
const (
	Version      = "0.1.0"
	RedisVersion = "7.2.0"
)

func init() {
	Register(
		&Spec{Name: "ping", Arity: -1, Flags: FlagPubSub, Group: "connection", Summary: "Returns PONG, or the message", Handler: pingCmd},
		&Spec{Name: "echo", Arity: 2, Group: "connection", Summary: "Returns the message", Handler: func(x *Ctx) resp.Value { return resp.Bulk(x.Arg(1)) }},
		&Spec{Name: "hello", Arity: -1, Flags: FlagNoAuth | FlagPubSub | FlagNoMulti, Group: "connection", Summary: "Handshake: pick RESP2/RESP3, authenticate, set name", Handler: helloCmd},
		&Spec{Name: "quit", Arity: -1, Flags: FlagNoAuth | FlagPubSub | FlagNoMulti, Group: "connection", Summary: "Closes the connection", Handler: quitCmd},
		&Spec{Name: "reset", Arity: 1, Flags: FlagNoAuth | FlagPubSub | FlagNoMulti, Group: "connection", Summary: "Resets the connection state", Handler: resetCmd},
		&Spec{Name: "select", Arity: 2, Group: "connection", Summary: "Only DB 0 exists; use NAMESPACE for isolation", Handler: selectCmd},
		&Spec{Name: "client", Arity: -2, Group: "connection", Summary: "Client connection management (ID, LIST, KILL, SETNAME, TRACKING...)", Handler: clientCmd},
		&Spec{Name: "config", Arity: -2, Flags: FlagAdmin, Group: "server", Summary: "CONFIG GET/SET/RESETSTAT", Handler: configCmd},
		&Spec{Name: "info", Arity: -1, Group: "server", Summary: "Server information and statistics", Handler: infoCmd},
		&Spec{Name: "dbsize", Arity: 1, Flags: FlagReadOnly, Group: "keyspace", Summary: "Number of keys", Handler: func(x *Ctx) resp.Value { return resp.Int(x.DB().Len()) }},
		&Spec{Name: "flushall", Arity: -1, Flags: FlagWrite | FlagAllKeys, Group: "keyspace", Summary: "Deletes every key", Handler: flushCmd},
		&Spec{Name: "flushdb", Arity: -1, Flags: FlagWrite | FlagAllKeys, Group: "keyspace", Summary: "Deletes every key", Handler: flushCmd},
		&Spec{Name: "time", Arity: 1, Group: "server", Summary: "Server time [seconds, microseconds]", Handler: timeCmd},
		&Spec{Name: "command", Arity: -1, Group: "server", Summary: "Command table introspection", Handler: commandCmd},
		&Spec{Name: "shutdown", Arity: -1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Saves (unless NOSAVE) and stops the server", Handler: shutdownCmd},
		&Spec{Name: "debug", Arity: -2, Flags: FlagAdmin, Group: "server", Summary: "DEBUG SLEEP seconds", Handler: debugCmd},
		&Spec{Name: "memory", Arity: -2, Flags: FlagReadOnly, Group: "server", Summary: "MEMORY USAGE key | STATS", Handler: memoryCmd,
			KeysFn: func(args []string) []int {
				if len(args) >= 3 && strings.EqualFold(args[1], "usage") {
					return []int{2}
				}
				return nil
			}},
		&Spec{Name: "slowlog", Arity: -2, Flags: FlagAdmin, Group: "server", Summary: "SLOWLOG GET [n] | LEN | RESET", Handler: slowlogCmd},
		&Spec{Name: "monitor", Arity: 1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Streams every command the server runs", Handler: monitorCmd},
	)
}

func pingCmd(x *Ctx) resp.Value {
	if x.Proto() == 2 && x.E.PubSub.Count(x.C) > 0 {
		return resp.Array(resp.Bulk("pong"), resp.Bulk(x.Arg(1)))
	}
	if x.NArgs() > 2 {
		return wrongArgs("ping")
	}
	if x.NArgs() == 2 {
		return resp.Bulk(x.Arg(1))
	}
	return resp.Simple("PONG")
}

func helloCmd(x *Ctx) resp.Value {
	proto := x.Proto()
	i := 1
	if x.NArgs() > 1 {
		p, ok := x.Int(1)
		if !ok || p < 2 || p > 3 {
			return resp.Err("NOPROTO unsupported protocol version")
		}
		proto = int(p)
		i = 2
	}
	for ; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "auth":
			if i+2 >= x.NArgs() {
				return ErrSyntax
			}
			if err := x.E.Auth.Authenticate(x.Arg(i+1), x.Arg(i+2)); err != nil {
				return resp.Err(err.Error())
			}
			x.C.User = x.Arg(i + 1)
			i += 2
		case "setname":
			if i+1 >= x.NArgs() {
				return ErrSyntax
			}
			x.C.SetName(x.Arg(i + 1))
			i++
		default:
			return ErrSyntax
		}
	}
	if !x.C.Authenticated() {
		return resp.Err("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
	}
	x.C.SetProto(proto)
	role := "master"
	if x.E.Repl != nil && x.E.Repl.IsReplica() {
		role = "replica"
	}
	return resp.Map(
		resp.Bulk("server"), resp.Bulk("cache-boy"),
		resp.Bulk("version"), resp.Bulk(RedisVersion),
		resp.Bulk("cacheboy_version"), resp.Bulk(Version),
		resp.Bulk("proto"), resp.Int(int64(proto)),
		resp.Bulk("id"), resp.Int(x.C.ID),
		resp.Bulk("mode"), resp.Bulk(map[bool]string{true: "cluster", false: "standalone"}[x.E.Cluster != nil && x.E.Cluster.Enabled()]),
		resp.Bulk("role"), resp.Bulk(role),
		resp.Bulk("modules"), resp.Array(),
	)
}

func quitCmd(x *Ctx) resp.Value {
	x.C.CloseAfterReply = true
	return resp.OK
}

func resetCmd(x *Ctx) resp.Value {
	c := x.C
	c.Multi = nil
	x.E.Tx.Unwatch(c.Watcher)
	c.Watcher = nil
	x.E.PubSub.RemoveAll(c)
	x.E.Tracking.Disable(c)
	c.Tracking = false
	c.Namespace = ""
	c.SetProto(2)
	c.SetName("")
	if x.E.Auth.AutoLogin() {
		c.User = "default"
	} else {
		c.User = ""
	}
	return resp.Simple("RESET")
}

func selectCmd(x *Ctx) resp.Value {
	if x.Arg(1) != "0" {
		return resp.Err("ERR DB index is out of range (cache-boy has one database; use NAMESPACE USE <name> to isolate keys)")
	}
	return resp.OK
}

func clientCmd(x *Ctx) resp.Value {
	c := x.C
	switch x.ArgLower(1) {
	case "id":
		return resp.Int(c.ID)
	case "setname":
		if x.NArgs() != 3 {
			return wrongArgs("client|setname")
		}
		if strings.ContainsAny(x.Arg(2), " \n") {
			return resp.Err("ERR Client names cannot contain spaces, newlines or special characters.")
		}
		c.SetName(x.Arg(2))
		return resp.OK
	case "getname":
		if c.Name() == "" {
			return resp.Null()
		}
		return resp.Bulk(c.Name())
	case "setinfo", "no-evict", "no-touch":
		return resp.OK
	case "info":
		return resp.Bulk(clientLine(c))
	case "list":
		var b strings.Builder
		for _, cl := range x.E.Clients() {
			b.WriteString(clientLine(cl))
			b.WriteByte('\n')
		}
		return resp.Bulk(b.String())
	case "kill":
		return clientKill(x)
	case "tracking":
		return clientTracking(x)
	case "trackinginfo":
		flags := []string{"off"}
		if c.Tracking {
			flags = []string{"on"}
		}
		return resp.Map(resp.Bulk("flags"), resp.Strings(flags), resp.Bulk("redirect"), resp.Int(-1), resp.Bulk("prefixes"), resp.Array())
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try CLIENT HELP.", x.Arg(1))
}

func clientLine(c *Client) string {
	ns := c.Namespace
	if ns == "" {
		ns = "default"
	}
	return fmt.Sprintf("id=%d addr=%s name=%s age=%d idle=%d kind=%s user=%s namespace=%s resp=%d cmd=%s",
		c.ID, c.Addr, c.Name(), int(time.Since(c.Created).Seconds()), int(c.Idle().Seconds()),
		c.Kind, c.User, ns, c.Proto(), c.LastCommand())
}

func clientKill(x *Ctx) resp.Value {
	if x.NArgs() == 3 { // old form: CLIENT KILL addr
		for _, cl := range x.E.Clients() {
			if cl.Addr == x.Arg(2) {
				cl.Close()
				return resp.OK
			}
		}
		return resp.Err("ERR No such client")
	}
	killed := 0
	for i := 2; i+1 < x.NArgs(); i += 2 {
		val := x.Arg(i + 1)
		for _, cl := range x.E.Clients() {
			match := false
			switch x.ArgLower(i) {
			case "id":
				match = strconv.FormatInt(cl.ID, 10) == val
			case "addr":
				match = cl.Addr == val
			case "user":
				match = cl.User == val
			default:
				return ErrSyntax
			}
			if match {
				cl.Close()
				killed++
			}
		}
	}
	return resp.Int(int64(killed))
}

// CLIENT TRACKING ON|OFF [BCAST] [PREFIX p ...] [NOLOOP]
func clientTracking(x *Ctx) resp.Value {
	if x.NArgs() < 3 {
		return wrongArgs("client|tracking")
	}
	switch x.ArgLower(2) {
	case "off":
		x.E.Tracking.Disable(x.C)
		x.C.Tracking = false
		return resp.OK
	case "on":
	default:
		return ErrSyntax
	}
	if x.Proto() < 3 {
		return resp.Err("ERR client-side caching needs RESP3: send HELLO 3 first (invalidations arrive as push messages)")
	}
	var opts tracking.Options
	for i := 3; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "bcast":
			opts.BCast = true
		case "noloop":
			opts.NoLoop = true
		case "prefix":
			if i+1 >= x.NArgs() {
				return ErrSyntax
			}
			opts.Prefixes = append(opts.Prefixes, x.StoredKey(x.Arg(i+1)))
			i++
		case "optin", "optout", "redirect":
			return resp.Errf("ERR CLIENT TRACKING %s is not supported by cache-boy", strings.ToUpper(x.Arg(i)))
		default:
			return ErrSyntax
		}
	}
	if len(opts.Prefixes) > 0 && !opts.BCast {
		return resp.Err("ERR PREFIX option requires BCAST mode to be enabled")
	}
	x.E.Tracking.Enable(x.C, opts)
	x.C.Tracking = true
	return resp.OK
}

func configCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "get":
		if x.NArgs() < 3 {
			return wrongArgs("config|get")
		}
		cfg := x.E.Cfg.Get()
		seen := map[string]bool{}
		var kv []resp.Value
		for _, pattern := range x.Args[2:] {
			for _, p := range cfg.Params(pattern) {
				if !seen[p[0]] {
					seen[p[0]] = true
					kv = append(kv, resp.Bulk(p[0]), resp.Bulk(p[1]))
				}
			}
		}
		return resp.Map(kv...)
	case "set":
		if x.NArgs() < 4 || x.NArgs()%2 != 0 {
			return wrongArgs("config|set")
		}
		for i := 2; i+1 < x.NArgs(); i += 2 {
			if err := x.E.SetConfig(x.Arg(i), x.Arg(i+1)); err != nil {
				return resp.Errf("ERR CONFIG SET failed: %v", err)
			}
		}
		return resp.OK
	case "resetstat":
		x.E.Metrics.ResetCommands()
		x.E.Slowlog.Reset()
		return resp.OK
	case "rewrite":
		return resp.Err("ERR CONFIG REWRITE is not supported; edit configs/cache-boy.conf instead")
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try CONFIG HELP.", x.Arg(1))
}

// SetConfig changes a parameter and applies side effects.
func (e *Engine) SetConfig(name, value string) error {
	name = strings.ToLower(name)
	switch name {
	case "bind", "port", "http-port", "shards", "cluster-enabled", "cluster-node-id", "cluster-node", "raft-port", "raft-peer", "appendonly", "appendfilename", "dir":
		return fmt.Errorf("'%s' can only be set at startup", name)
	}
	if err := e.Cfg.Set(name, value); err != nil {
		return err
	}
	c := e.Cfg.Get()
	switch name {
	case "requirepass":
		e.Auth.SetDefaultPassword(c.RequirePass)
	case "compression-threshold":
		e.DB.CompressThreshold.Store(int64(c.CompressionThreshold))
	case "replicaof", "slaveof":
		if e.Repl == nil {
			return fmt.Errorf("replication is not available")
		}
		return e.Repl.ReplicaOf(strings.Replace(c.ReplicaOf, " ", ":", 1))
	}
	return nil
}

func flushCmd(x *Ctx) resp.Value {
	if x.NArgs() > 2 || (x.NArgs() == 2 && x.ArgLower(1) != "async" && x.ArgLower(1) != "sync") {
		return ErrSyntax
	}
	x.DB().Flush()
	return resp.OK
}

func timeCmd(x *Ctx) resp.Value {
	now := time.Now()
	return resp.Array(resp.Bulk(strconv.FormatInt(now.Unix(), 10)), resp.Bulk(strconv.Itoa(now.Nanosecond()/1000)))
}

func commandCmd(x *Ctx) resp.Value {
	if x.NArgs() == 1 {
		all := All()
		out := make([]resp.Value, len(all))
		for i, s := range all {
			out[i] = commandInfo(s)
		}
		return resp.Array(out...)
	}
	switch x.ArgLower(1) {
	case "count":
		return resp.Int(int64(len(registry)))
	case "list":
		all := All()
		names := make([]string, len(all))
		for i, s := range all {
			names[i] = s.Name
		}
		return resp.Strings(names)
	case "info":
		var out []resp.Value
		for _, n := range x.Args[2:] {
			if s := Lookup(n); s != nil {
				out = append(out, commandInfo(s))
			} else {
				out = append(out, resp.NullArray())
			}
		}
		return resp.Array(out...)
	case "docs":
		var kv []resp.Value
		names := x.Args[2:]
		if len(names) == 0 {
			for _, s := range All() {
				names = append(names, s.Name)
			}
		}
		for _, n := range names {
			if s := Lookup(n); s != nil {
				kv = append(kv, resp.Bulk(s.Name), resp.Map(resp.Bulk("summary"), resp.Bulk(s.Summary), resp.Bulk("group"), resp.Bulk(s.Group)))
			}
		}
		return resp.Map(kv...)
	case "getkeys":
		if x.NArgs() < 3 {
			return wrongArgs("command|getkeys")
		}
		s := Lookup(x.Arg(2))
		if s == nil {
			return resp.Err("ERR Invalid command specified")
		}
		args := x.Args[2:]
		var keys []string
		for _, i := range s.KeyIndexes(args) {
			keys = append(keys, args[i])
		}
		return resp.Strings(keys)
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try COMMAND HELP.", x.Arg(1))
}

func commandInfo(s *Spec) resp.Value {
	flags := s.FlagNames()
	cats := s.Categories()
	for i, c := range cats {
		cats[i] = "@" + c
	}
	return resp.Array(
		resp.Bulk(s.Name), resp.Int(int64(s.Arity)), resp.StringSet(flags),
		resp.Int(int64(s.FirstKey)), resp.Int(int64(s.LastKey)), resp.Int(int64(s.KeyStep)),
		resp.StringSet(cats),
	)
}

// OnShutdown is set by main; it saves (if asked) and exits the process.
var OnShutdown func(save bool) error

func shutdownCmd(x *Ctx) resp.Value {
	save := x.E.Persist != nil
	for _, a := range x.Args[1:] {
		switch strings.ToLower(a) {
		case "nosave":
			save = false
		case "save":
			save = true
		case "now", "force", "abort":
		default:
			return ErrSyntax
		}
	}
	if OnShutdown == nil {
		os.Exit(0)
	}
	if err := OnShutdown(save); err != nil {
		return resp.Errf("ERR Errors trying to SHUTDOWN: %v", err)
	}
	return resp.None()
}

func debugCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "sleep":
		f, ok := x.Float(2)
		if !ok {
			return ErrNotFloat
		}
		time.Sleep(time.Duration(f * float64(time.Second)))
		return resp.OK
	case "gc":
		runtime.GC()
		return resp.OK
	}
	return resp.Errf("ERR unknown DEBUG subcommand '%s'", x.Arg(1))
}

func memoryCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "usage":
		if x.NArgs() < 3 {
			return wrongArgs("memory|usage")
		}
		e := x.DB().Peek(x.Arg(2))
		if e == nil {
			return resp.Null()
		}
		return resp.Int(e.Size())
	case "stats":
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return resp.Map(
			resp.Bulk("dataset.bytes"), resp.Int(x.DB().UsedMemory()),
			resp.Bulk("keys.count"), resp.Int(x.DB().Len()),
			resp.Bulk("heap.alloc"), resp.Int(int64(ms.HeapAlloc)),
			resp.Bulk("heap.sys"), resp.Int(int64(ms.HeapSys)),
			resp.Bulk("gc.count"), resp.Int(int64(ms.NumGC)),
		)
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try MEMORY HELP.", x.Arg(1))
}

func slowlogCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "get":
		n := 10
		if x.NArgs() > 2 {
			v, ok := x.Int(2)
			if !ok {
				return ErrNotInt
			}
			n = int(v)
		}
		entries := x.E.Slowlog.Get(n)
		out := make([]resp.Value, len(entries))
		for i, en := range entries {
			out[i] = resp.Array(
				resp.Int(en.ID), resp.Int(en.Time.Unix()), resp.Int(en.Duration.Microseconds()),
				resp.Strings(en.Args), resp.Bulk(en.ClientAddr), resp.Bulk(en.ClientName),
			)
		}
		return resp.Array(out...)
	case "len":
		return resp.Int(int64(x.E.Slowlog.Len()))
	case "reset":
		x.E.Slowlog.Reset()
		return resp.OK
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try SLOWLOG HELP.", x.Arg(1))
}

func monitorCmd(x *Ctx) resp.Value {
	x.E.Monitor.Add(x.C)
	return resp.OK
}

// --- INFO ---

func infoCmd(x *Ctx) resp.Value {
	sections := map[string]bool{}
	for _, a := range x.Args[1:] {
		sections[strings.ToLower(a)] = true
	}
	all := len(sections) == 0 || sections["all"] || sections["everything"] || sections["default"]
	var b strings.Builder
	for _, sec := range x.E.InfoSections() {
		if !all && !sections[sec.Name] {
			continue
		}
		if b.Len() > 0 {
			b.WriteString("\r\n")
		}
		fmt.Fprintf(&b, "# %s\r\n", strings.ToUpper(sec.Name[:1])+sec.Name[1:])
		for _, kv := range sec.Fields {
			fmt.Fprintf(&b, "%s:%s\r\n", kv[0], kv[1])
		}
	}
	return resp.Bulk(b.String())
}

type InfoSection struct {
	Name   string
	Fields [][2]string
}

// InfoSections returns INFO data (also used by the HTTP API and dashboard).
func (e *Engine) InfoSections() []InfoSection {
	cfg := e.Cfg.Get()
	m := e.Metrics
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	uptime := time.Since(e.StartTime)
	i64 := func(n int64) string { return strconv.FormatInt(n, 10) }

	role := "master"
	if e.Repl != nil && e.Repl.IsReplica() {
		role = "slave"
	}
	secs := []InfoSection{
		{"server", [][2]string{
			{"redis_version", RedisVersion},
			{"cacheboy_version", Version},
			{"redis_mode", map[bool]string{true: "cluster", false: "standalone"}[e.Cluster != nil && e.Cluster.Enabled()]},
			{"os", runtime.GOOS + " " + runtime.GOARCH},
			{"go_version", runtime.Version()},
			{"process_id", strconv.Itoa(os.Getpid())},
			{"run_id", e.RunID},
			{"tcp_port", strconv.Itoa(cfg.Port)},
			{"http_port", strconv.Itoa(cfg.HTTPPort)},
			{"uptime_in_seconds", i64(int64(uptime.Seconds()))},
			{"uptime_in_days", i64(int64(uptime.Hours() / 24))},
			{"hz", strconv.Itoa(cfg.HZ)},
			{"shards", strconv.Itoa(e.DB.NumShards())},
			{"cpu_cores_used", strconv.Itoa(runtime.GOMAXPROCS(0))},
		}},
		{"clients", [][2]string{
			{"connected_clients", i64(m.ConnectedClients.Load())},
			{"blocked_clients", strconv.Itoa(e.Blocking.Blocked())},
			{"tracking_clients_keys", strconv.Itoa(e.Tracking.TrackedKeys())},
			{"pubsub_channels", strconv.Itoa(len(e.PubSub.Channels("")))},
			{"pubsub_patterns", strconv.Itoa(e.PubSub.NumPat())},
			{"maxclients", strconv.Itoa(cfg.MaxClients)},
		}},
		{"memory", [][2]string{
			{"used_memory", i64(e.DB.UsedMemory())},
			{"used_memory_human", humanBytes(e.DB.UsedMemory())},
			{"used_memory_heap", i64(int64(ms.HeapAlloc))},
			{"used_memory_heap_human", humanBytes(int64(ms.HeapAlloc))},
			{"used_memory_sys", i64(int64(ms.Sys))},
			{"maxmemory", i64(cfg.MaxMemory)},
			{"maxmemory_human", humanBytes(cfg.MaxMemory)},
			{"maxmemory_policy", cfg.MaxMemoryPolicy},
			{"compression_threshold", strconv.Itoa(cfg.CompressionThreshold)},
			{"gc_cycles", strconv.Itoa(int(ms.NumGC))},
		}},
	}
	persist := [][2]string{{"loading", boolInt(e.Loading())}, {"rdb_changes_since_last_save", i64(e.DB.Dirty())}}
	if e.Persist != nil {
		persist = append(persist, e.Persist.Info()...)
	}
	secs = append(secs, InfoSection{"persistence", persist})

	hits, misses := e.DB.Hits.Load(), e.DB.Misses.Load()
	ratio := 0.0
	if hits+misses > 0 {
		ratio = float64(hits) / float64(hits+misses)
	}
	secs = append(secs, InfoSection{"stats", [][2]string{
		{"total_connections_received", i64(m.TotalConnections.Load())},
		{"total_commands_processed", i64(m.TotalCommands.Load())},
		{"instantaneous_ops_per_sec", "0"},
		{"total_net_input_bytes", i64(m.NetInputBytes.Load())},
		{"total_net_output_bytes", i64(m.NetOutputBytes.Load())},
		{"rejected_connections", i64(m.RejectedConns.Load())},
		{"expired_keys", i64(e.DB.Expired.Load())},
		{"evicted_keys", i64(e.DB.Evicted.Load())},
		{"keyspace_hits", i64(hits)},
		{"keyspace_misses", i64(misses)},
		{"keyspace_hit_ratio", strconv.FormatFloat(ratio, 'f', 4, 64)},
	}})

	repl := [][2]string{{"role", role}}
	if e.Repl != nil {
		repl = append(repl, e.Repl.Info()...)
	} else {
		repl = append(repl, [2]string{"connected_slaves", "0"})
	}
	secs = append(secs, InfoSection{"replication", repl})

	var cmdstats [][2]string
	for _, cs := range m.Commands() {
		per := 0.0
		if cs.Calls > 0 {
			per = float64(cs.Usec) / float64(cs.Calls)
		}
		cmdstats = append(cmdstats, [2]string{"cmdstat_" + cs.Name,
			fmt.Sprintf("calls=%d,usec=%d,usec_per_call=%.2f,failed_calls=%d", cs.Calls, cs.Usec, per, cs.Errors)})
	}
	secs = append(secs, InfoSection{"commandstats", cmdstats})

	cluster := [][2]string{{"cluster_enabled", boolInt(e.Cluster != nil && e.Cluster.Enabled())}}
	if e.Cluster != nil && e.Cluster.Enabled() {
		cluster = append(cluster, e.Cluster.Info()...)
	}
	secs = append(secs, InfoSection{"cluster", cluster})

	keyspace := [][2]string{}
	if n := e.DB.Len(); n > 0 {
		keyspace = append(keyspace, [2]string{"db0", fmt.Sprintf("keys=%d,expires=0,avg_ttl=0", n)})
	}
	secs = append(secs, InfoSection{"keyspace", keyspace})
	return secs
}

func humanBytes(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.2fG", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.2fM", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.2fK", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%dB", n)
}

func boolInt(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

var _ = config.Default // config is used by SetConfig via e.Cfg
