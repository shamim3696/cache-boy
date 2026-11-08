package httpapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shamim3696/cache-boy/internal/api/apiutil"
	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/feature/namespace"
	"github.com/shamim3696/cache-boy/internal/glob"
	"github.com/shamim3696/cache-boy/internal/observability"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

// POST /v1/command  ["SET","a","1"] → {"result": ...} | {"error": ...}
func (a *api) command(w http.ResponseWriter, r *http.Request, c *command.Client) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	args, err := apiutil.ParseCommand(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ERR "+err.Error())
		return
	}
	writeResult(w, a.exec(c, args...))
}

// POST /v1/pipeline  [[...],[...]] → [{"result":...},{"error":...}]
func (a *api) pipeline(w http.ResponseWriter, r *http.Request, c *command.Client) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	cmds, err := apiutil.ParseCommands(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ERR "+err.Error())
		return
	}
	b := []byte{'['}
	for i, args := range cmds {
		if i > 0 {
			b = append(b, ',')
		}
		v := a.exec(c, args...)
		if v.IsError() {
			b = append(b, `{"error":`...)
			b = apiutil.AppendString(b, v.Str)
		} else {
			b = append(b, `{"result":`...)
			b = apiutil.AppendJSON(b, v)
		}
		b = append(b, '}')
	}
	b = append(b, ']')
	writeJSON(w, http.StatusOK, json.RawMessage(b))
}

// POST /v1/multi  [[...],[...]] → {"result":[...]} | {"error": ...}
// Runs MULTI, the commands and EXEC on this request's client.
func (a *api) multi(w http.ResponseWriter, r *http.Request, c *command.Client) {
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	cmds, err := apiutil.ParseCommands(body)
	if err != nil {
		writeError(w, http.StatusBadRequest, "ERR "+err.Error())
		return
	}
	for _, args := range cmds {
		switch lower(args[0]) {
		case "multi", "exec", "discard", "watch":
			writeError(w, http.StatusBadRequest, "ERR "+strings.ToUpper(args[0])+" is not allowed in /v1/multi")
			return
		}
	}
	if v := a.exec(c, "MULTI"); v.IsError() {
		writeResult(w, v)
		return
	}
	var firstErr string
	for _, args := range cmds {
		if v := a.exec(c, args...); v.IsError() && firstErr == "" {
			firstErr = v.Str
		}
	}
	v := a.exec(c, "EXEC")
	if v.IsError() && firstErr != "" {
		v = resp.Err(v.Str + " First error: " + firstErr)
	}
	writeResult(w, v)
}

// GET /v1/keys/{key} → {"key","type","ttl_ms","value"}, 404 if missing.
func (a *api) getKey(w http.ResponseWriter, r *http.Request, c *command.Client) {
	key := r.PathValue("key")
	t := a.exec(c, "TYPE", key)
	if t.IsError() {
		writeResult(w, t)
		return
	}
	typ := t.String()
	if typ == "none" || typ == "" {
		writeError(w, http.StatusNotFound, "ERR no such key")
		return
	}
	ttl := a.exec(c, "PTTL", key)
	if ttl.IsError() {
		writeResult(w, ttl)
		return
	}
	if ttl.Int == -2 { // deleted between the two calls
		writeError(w, http.StatusNotFound, "ERR no such key")
		return
	}
	val, verr := a.typedValue(c, key, typ)
	if verr.IsError() {
		writeResult(w, verr)
		return
	}
	b := []byte(`{"key":`)
	b = apiutil.AppendString(b, key)
	b = append(b, `,"type":`...)
	b = apiutil.AppendString(b, typ)
	b = append(b, `,"ttl_ms":`...)
	b = strconv.AppendInt(b, ttl.Int, 10)
	b = append(b, `,"value":`...)
	b = append(b, val...)
	b = append(b, '}')
	writeJSON(w, http.StatusOK, json.RawMessage(b))
}

// typedValue fetches a key's value as JSON according to its type. The
// second result is an error reply (or a zero Value).
func (a *api) typedValue(c *command.Client, key, typ string) (json.RawMessage, resp.Value) {
	var v resp.Value
	switch typ {
	case "string":
		v = a.exec(c, "GET", key)
	case "hash":
		v = a.exec(c, "HGETALL", key)
		if !v.IsError() && v.Type != resp.TypeMap {
			v = resp.Map(pairs(v.Elems)...)
		}
	case "list":
		v = a.exec(c, "LRANGE", key, "0", "-1")
	case "set":
		v = a.exec(c, "SMEMBERS", key)
	case "zset":
		v = a.exec(c, "ZRANGE", key, "0", "-1", "WITHSCORES")
		if v.IsError() {
			return nil, v
		}
		return zsetJSON(v), resp.Value{}
	case "json", "ReJSON-RL":
		v = a.exec(c, "JSON.GET", key)
		if v.IsError() {
			return nil, v
		}
		if v.IsNull() {
			return json.RawMessage("null"), resp.Value{}
		}
		if json.Valid([]byte(v.Str)) {
			return json.RawMessage(v.Str), resp.Value{}
		}
	case "timeseries", "TSDB-TYPE":
		v = a.exec(c, "TS.RANGE", key, "-", "+")
	case "vectorset", "vector":
		n := a.exec(c, "VCARD", key)
		d := a.exec(c, "VDIM", key)
		if n.IsError() {
			return nil, n
		}
		return json.RawMessage(fmt.Sprintf(`{"count":%s,"dim":%s}`, apiutil.JSON(n), apiutil.JSON(d))), resp.Value{}
	default:
		return json.RawMessage("null"), resp.Value{}
	}
	if v.IsError() {
		return nil, v
	}
	return apiutil.JSON(v), resp.Value{}
}

// pairs turns a flat [k, v, k, v...] array into map elements (RESP2 style replies).
func pairs(elems []resp.Value) []resp.Value {
	if len(elems)%2 != 0 {
		elems = elems[:len(elems)-1]
	}
	return elems
}

// zsetJSON renders ZRANGE WITHSCORES (flat or as [member, score] pairs) as
// [{"member","score"}].
func zsetJSON(v resp.Value) json.RawMessage {
	type ms struct{ m, s resp.Value }
	var items []ms
	if len(v.Elems) > 0 && len(v.Elems[0].Elems) == 2 {
		for _, p := range v.Elems {
			if len(p.Elems) == 2 {
				items = append(items, ms{p.Elems[0], p.Elems[1]})
			}
		}
	} else {
		for i := 0; i+1 < len(v.Elems); i += 2 {
			items = append(items, ms{v.Elems[i], v.Elems[i+1]})
		}
	}
	b := []byte{'['}
	for i, it := range items {
		if i > 0 {
			b = append(b, ',')
		}
		b = append(b, `{"member":`...)
		b = apiutil.AppendString(b, it.m.String())
		b = append(b, `,"score":`...)
		score := it.s.Float
		if it.s.Type != resp.TypeDouble {
			score, _ = command.ParseFloat(it.s.String())
		}
		b = apiutil.AppendFloat(b, score)
		b = append(b, '}')
	}
	return append(b, ']')
}

// PUT /v1/keys/{key}?ttl=<seconds>, raw body → SET key body [EX ttl]
func (a *api) putKey(w http.ResponseWriter, r *http.Request, c *command.Client) {
	key := r.PathValue("key")
	args := []string{"SET", key, ""}
	if s := r.URL.Query().Get("ttl"); s != "" {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "ERR ttl must be a positive number of seconds")
			return
		}
		args = append(args, "EX", s)
	}
	body, ok := readBody(w, r)
	if !ok {
		return
	}
	args[2] = string(body)
	writeResult(w, a.exec(c, args...))
}

// DELETE /v1/keys/{key} → {"result": <deleted count>}
func (a *api) deleteKey(w http.ResponseWriter, r *http.Request, c *command.Client) {
	writeResult(w, a.exec(c, "DEL", r.PathValue("key")))
}

// allowed checks the client's ACL for a command that the API runs on its
// behalf without going through Execute.
func (a *api) allowed(c *command.Client, cmd string, keys ...string) error {
	cats := []string{"server", "admin", "dangerous"}
	if s := command.Lookup(cmd); s != nil {
		cats = s.Categories()
	} else if cmd == "scan" {
		cats = []string{"keyspace", "read"}
	}
	return a.e.Auth.Check(c.User, cmd, cats, keys)
}

func (a *api) denied(w http.ResponseWriter, c *command.Client, cmd string) bool {
	if err := a.allowed(c, cmd); err != nil {
		writeError(w, http.StatusForbidden, err.Error())
		return true
	}
	return false
}

type scanKey struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	TTLMs int64  `json:"ttl_ms"`
}

// GET /v1/scan?pattern=*&cursor=0&count=100&type=
func (a *api) scan(w http.ResponseWriter, r *http.Request, c *command.Client) {
	if a.denied(w, c, "scan") {
		return
	}
	q := r.URL.Query()
	pattern := q.Get("pattern")
	if pattern == "" {
		pattern = "*"
	}
	var cursor uint64
	if s := q.Get("cursor"); s != "" {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ERR invalid cursor")
			return
		}
		cursor = n
	}
	count := 100
	if s := q.Get("count"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			writeError(w, http.StatusBadRequest, "ERR count must be a positive integer")
			return
		}
		count = min(n, 10000)
	}
	typ := lower(q.Get("type"))
	ns := c.Namespace
	user, _ := a.e.Auth.User(c.User)
	checkKeys := user == nil || !user.AllKeys
	now := time.Now().UnixMilli()

	keys := []scanKey{}
	unlock := a.e.DB.LockAll()
	next, _ := a.e.DB.Scan(cursor, count, func(stored string, en *store.Entry) bool {
		k, ok := namespace.Strip(ns, stored)
		if !ok || !glob.Match(pattern, k) {
			return false
		}
		kind := en.Kind.String()
		if typ != "" && typ != kind {
			return false
		}
		if checkKeys && a.e.Auth.Check(c.User, "scan", []string{"keyspace", "read"}, []string{stored}) != nil {
			return false
		}
		ttl := int64(-1)
		if en.ExpireAt > 0 {
			ttl = max(en.ExpireAt-now, 0)
		}
		keys = append(keys, scanKey{k, kind, ttl})
		return true
	})
	unlock()

	writeJSON(w, http.StatusOK, struct {
		Cursor string    `json:"cursor"`
		Keys   []scanKey `json:"keys"`
	}{strconv.FormatUint(next, 10), keys})
}

// GET /v1/info → {section: {field: value}}
func (a *api) info(w http.ResponseWriter, r *http.Request, c *command.Client) {
	if a.denied(w, c, "info") {
		return
	}
	out := map[string]map[string]string{}
	want := r.URL.Query().Get("section")
	for _, sec := range a.e.InfoSections() {
		if want != "" && !strings.EqualFold(want, sec.Name) {
			continue
		}
		m := make(map[string]string, len(sec.Fields))
		for _, kv := range sec.Fields {
			m[kv[0]] = kv[1]
		}
		out[sec.Name] = m
	}
	writeJSON(w, http.StatusOK, out)
}

type slowEntry struct {
	ID         int64    `json:"id"`
	Time       int64    `json:"time"`
	DurationUs int64    `json:"duration_us"`
	Args       []string `json:"args"`
	ClientAddr string   `json:"client_addr"`
	ClientName string   `json:"client_name"`
}

// GET /v1/slowlog?count=N → [{"id","time","duration_us","args","client_addr","client_name"}]
func (a *api) slowlog(w http.ResponseWriter, r *http.Request, c *command.Client) {
	if a.denied(w, c, "slowlog") {
		return
	}
	n := 128
	if s := r.URL.Query().Get("count"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil {
			writeError(w, http.StatusBadRequest, "ERR count must be an integer")
			return
		}
		n = v
	}
	entries := a.e.Slowlog.Get(n)
	out := make([]slowEntry, len(entries))
	for i, en := range entries {
		out[i] = slowEntry{en.ID, en.Time.Unix(), en.Duration.Microseconds(), en.Args, en.ClientAddr, en.ClientName}
	}
	writeJSON(w, http.StatusOK, out)
}

type clientInfo struct {
	ID        int64  `json:"id"`
	Addr      string `json:"addr"`
	Name      string `json:"name"`
	Kind      string `json:"kind"`
	User      string `json:"user"`
	Namespace string `json:"namespace"`
	Resp      int    `json:"resp"`
	AgeS      int64  `json:"age_s"`
	IdleS     int64  `json:"idle_s"`
	Cmd       string `json:"cmd"`
}

// GET /v1/clients → [{"id","addr","name","kind","user","namespace","resp","age_s","idle_s","cmd"}]
func (a *api) clients(w http.ResponseWriter, r *http.Request, c *command.Client) {
	if a.denied(w, c, "client") {
		return
	}
	out := []clientInfo{}
	for _, cl := range a.e.Clients() {
		ns := cl.Namespace
		if ns == "" {
			ns = "default"
		}
		out = append(out, clientInfo{cl.ID, cl.Addr, cl.Name(), cl.Kind, cl.User, ns, cl.Proto(),
			int64(time.Since(cl.Created).Seconds()), int64(cl.Idle().Seconds()), cl.LastCommand()})
	}
	writeJSON(w, http.StatusOK, out)
}

type cmdStat struct {
	Name        string  `json:"name"`
	Calls       int64   `json:"calls"`
	FailedCalls int64   `json:"failed_calls"`
	Usec        int64   `json:"usec"`
	UsecPerCall float64 `json:"usec_per_call"`
}

// GET /v1/commandstats → [{"name","calls","failed_calls","usec","usec_per_call"}]
func (a *api) commandStats(w http.ResponseWriter, r *http.Request, c *command.Client) {
	if a.denied(w, c, "info") {
		return
	}
	out := []cmdStat{}
	for _, s := range a.e.Metrics.Commands() {
		per := 0.0
		if s.Calls > 0 {
			per = float64(s.Usec) / float64(s.Calls)
		}
		out = append(out, cmdStat{s.Name, s.Calls, s.Errors, s.Usec, per})
	}
	writeJSON(w, http.StatusOK, out)
}

// GET /metrics (Prometheus text format, public)
func (a *api) metrics(w http.ResponseWriter, r *http.Request) {
	e := a.e
	g := func(name, help string, v float64) observability.Gauge {
		return observability.Gauge{Name: name, Help: help, Value: v}
	}
	gauges := []observability.Gauge{
		g("used_memory_bytes", "Estimated bytes used by keys and values.", float64(e.DB.UsedMemory())),
		g("keys", "Number of keys.", float64(e.DB.Len())),
		g("connected_clients", "Connected clients.", float64(e.Metrics.ConnectedClients.Load())),
		g("keyspace_hits_total", "Successful key lookups.", float64(e.DB.Hits.Load())),
		g("keyspace_misses_total", "Failed key lookups.", float64(e.DB.Misses.Load())),
		g("expired_keys_total", "Keys removed because they expired.", float64(e.DB.Expired.Load())),
		g("evicted_keys_total", "Keys evicted because of maxmemory.", float64(e.DB.Evicted.Load())),
		g("blocked_clients", "Clients waiting in blocking commands.", float64(e.Blocking.Blocked())),
		g("uptime_seconds", "Seconds since the server started.", time.Since(e.StartTime).Seconds()),
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	e.Metrics.WritePrometheus(w, gauges)
}
