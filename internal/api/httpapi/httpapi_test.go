package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"cache-boy/internal/api/apitest"
	"cache-boy/internal/command"
	"cache-boy/internal/config"
	"cache-boy/internal/feature/namespace"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

func init() { apitest.Register() }

func newServer(t *testing.T) (*command.Engine, *httptest.Server) {
	t.Helper()
	e := command.New(config.NewStore(config.Default()))
	t.Cleanup(e.Stop)
	ts := httptest.NewServer(New(e))
	t.Cleanup(ts.Close)
	return e, ts
}

type reply struct {
	status int
	body   string
	json   map[string]any
}

func call(t *testing.T, ts *httptest.Server, method, path, body string, hdr ...string) reply {
	t.Helper()
	req, err := http.NewRequest(method, ts.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	r := reply{status: res.StatusCode, body: string(b)}
	json.Unmarshal(b, &r.json)
	return r
}

func expect(t *testing.T, r reply, status int, body string) {
	t.Helper()
	if r.status != status || strings.TrimSpace(r.body) != body {
		t.Fatalf("got %d %s, want %d %s", r.status, r.body, status, body)
	}
}

func put(e *command.Engine, key string, en *store.Entry) {
	unlock := e.DB.Lock(key)
	e.DB.Put(key, en)
	unlock()
}

func TestCommand(t *testing.T) {
	_, ts := newServer(t)
	expect(t, call(t, ts, "POST", "/v1/command", `["PING"]`), 200, `{"result":"PONG"}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["ECHO", 42]`), 200, `{"result":"42"}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["ECHO", 1.5e3]`), 200, `{"result":"1.5e3"}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["SET","a","1"]`), 200, `{"result":"OK"}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["GET","a"]`), 200, `{"result":"1"}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["GET","missing"]`), 200, `{"result":null}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["DBSIZE"]`), 200, `{"result":1}`)
	expect(t, call(t, ts, "POST", "/v1/command", `["HELLO","3"]`).trimMap(t, "proto"), 200, `3`)

	r := call(t, ts, "POST", "/v1/command", `["NOSUCH"]`)
	if r.status != 200 || !strings.HasPrefix(r.json["error"].(string), "ERR unknown command") {
		t.Fatalf("unknown command: %d %s", r.status, r.body)
	}
	for _, bad := range []string{``, `{}`, `[]`, `[true]`, `[["a"]]`, `["a",null]`, `not json`} {
		if r := call(t, ts, "POST", "/v1/command", bad); r.status != 400 || r.json["error"] == nil {
			t.Fatalf("body %q: got %d %s", bad, r.status, r.body)
		}
	}
	if r := call(t, ts, "GET", "/v1/command", ``); r.status != 404 && r.status != 405 {
		t.Fatalf("GET /v1/command: %d", r.status)
	}
	if r := call(t, ts, "GET", "/v1/nope", ``); r.status != 404 || r.json["error"] == nil {
		t.Fatalf("unknown endpoint: %d %s", r.status, r.body)
	}
}

// trimMap extracts one field of a map result, for compact assertions.
func (r reply) trimMap(t *testing.T, field string) reply {
	t.Helper()
	m, ok := r.json["result"].(map[string]any)
	if !ok {
		t.Fatalf("not a map result: %s", r.body)
	}
	b, _ := json.Marshal(m[field])
	r.body = string(b)
	return r
}

func TestPipelineAndMulti(t *testing.T) {
	_, ts := newServer(t)
	expect(t, call(t, ts, "POST", "/v1/pipeline", `[["SET","a","1"],["GET","a"],["NOSUCH"],["ECHO","x"]]`), 200,
		`[{"result":"OK"},{"result":"1"},{"error":"ERR unknown command 'NOSUCH', with args beginning with: "},{"result":"x"}]`)
	expect(t, call(t, ts, "POST", "/v1/multi", `[["SET","b","2"],["GET","b"],["PING"]]`), 200, `{"result":["OK","2","PONG"]}`)
	r := call(t, ts, "POST", "/v1/multi", `[["SET","c","3"],["NOSUCH"]]`)
	if r.status != 200 || !strings.HasPrefix(r.json["error"].(string), "EXECABORT") {
		t.Fatalf("multi with bad command: %s", r.body)
	}
	expect(t, call(t, ts, "POST", "/v1/command", `["GET","c"]`), 200, `{"result":null}`)
	if r := call(t, ts, "POST", "/v1/multi", `[["EXEC"]]`); r.status != 400 {
		t.Fatalf("EXEC in multi: %d", r.status)
	}
	if r := call(t, ts, "POST", "/v1/pipeline", `["PING"]`); r.status != 400 {
		t.Fatalf("flat pipeline: %d", r.status)
	}
}

func TestKeys(t *testing.T) {
	e, ts := newServer(t)
	expect(t, call(t, ts, "PUT", "/v1/keys/greeting", `hello "world"`), 200, `{"result":"OK"}`)
	expect(t, call(t, ts, "GET", "/v1/keys/greeting", ``), 200, `{"key":"greeting","type":"string","ttl_ms":-1,"value":"hello \"world\""}`)

	expect(t, call(t, ts, "PUT", "/v1/keys/a%2Fb?ttl=100", `x`), 200, `{"result":"OK"}`)
	r := call(t, ts, "GET", "/v1/keys/a%2Fb", ``)
	if r.status != 200 || r.json["key"] != "a/b" || r.json["ttl_ms"].(float64) <= 99000 {
		t.Fatalf("key with slash and ttl: %s", r.body)
	}
	if r := call(t, ts, "PUT", "/v1/keys/x?ttl=abc", `x`); r.status != 400 {
		t.Fatalf("bad ttl: %d", r.status)
	}

	h := store.NewHash()
	h.Set("f", "v")
	put(e, "h", store.NewEntry(store.KindHash, h))
	expect(t, call(t, ts, "GET", "/v1/keys/h", ``), 200, `{"key":"h","type":"hash","ttl_ms":-1,"value":{"f":"v"}}`)

	r = call(t, ts, "GET", "/v1/keys/missing", ``)
	if r.status != 404 || r.json["error"] == nil {
		t.Fatalf("missing key: %d %s", r.status, r.body)
	}
	expect(t, call(t, ts, "DELETE", "/v1/keys/greeting", ``), 200, `{"result":1}`)
	expect(t, call(t, ts, "DELETE", "/v1/keys/greeting", ``), 200, `{"result":0}`)

	// Namespaced access.
	expect(t, call(t, ts, "PUT", "/v1/keys/k", `ns-value`, NamespaceHeader, "tenant"), 200, `{"result":"OK"}`)
	if e.DB.Peek(namespace.Prefix("tenant")+"k") == nil {
		t.Fatal("namespaced key not stored with prefix")
	}
	if r := call(t, ts, "GET", "/v1/keys/k", ``); r.status != 404 {
		t.Fatalf("namespaced key visible in default namespace: %s", r.body)
	}
	if r := call(t, ts, "GET", "/v1/keys/k", ``, NamespaceHeader, strings.Repeat("n", 65)); r.status != 400 {
		t.Fatalf("invalid namespace: %d", r.status)
	}
}

func TestZSetJSON(t *testing.T) {
	flat := resp.Array(resp.Bulk("a"), resp.Bulk("1.5"), resp.Bulk("b"), resp.Bulk("inf"))
	if got := string(zsetJSON(flat)); got != `[{"member":"a","score":1.5},{"member":"b","score":"inf"}]` {
		t.Fatalf("flat: %s", got)
	}
	nested := resp.Array(resp.Array(resp.Bulk("a"), resp.Double(2)))
	if got := string(zsetJSON(nested)); got != `[{"member":"a","score":2}]` {
		t.Fatalf("nested: %s", got)
	}
}

func TestScan(t *testing.T) {
	e, ts := newServer(t)
	for _, k := range []string{"user:1", "user:2", "order:1"} {
		put(e, k, e.DB.NewStringValue([]byte("v")))
	}
	put(e, namespace.Prefix("app")+"user:9", e.DB.NewStringValue([]byte("v")))
	h := store.NewEntry(store.KindHash, store.NewHash())
	h.Hash().Set("a", "b")
	h.ExpireAt = time.Now().Add(time.Hour).UnixMilli()
	put(e, "user:h", h)

	scanAll := func(query string, hdr ...string) map[string]map[string]any {
		out := map[string]map[string]any{}
		cursor := "0"
		for i := 0; ; i++ {
			r := call(t, ts, "GET", "/v1/scan?cursor="+cursor+"&count=1&"+query, ``, hdr...)
			if r.status != 200 {
				t.Fatalf("scan: %d %s", r.status, r.body)
			}
			for _, k := range r.json["keys"].([]any) {
				m := k.(map[string]any)
				out[m["key"].(string)] = m
			}
			cursor = r.json["cursor"].(string)
			if cursor == "0" || i > 1000 {
				return out
			}
		}
	}
	if got := scanAll("pattern=user:*"); len(got) != 3 || got["user:h"]["type"] != "hash" || got["user:h"]["ttl_ms"].(float64) <= 0 || got["user:1"]["ttl_ms"].(float64) != -1 {
		t.Fatalf("scan user:*: %v", got)
	}
	if got := scanAll("type=hash"); len(got) != 1 {
		t.Fatalf("scan type=hash: %v", got)
	}
	if got := scanAll("", NamespaceHeader, "app"); len(got) != 1 || got["user:9"] == nil {
		t.Fatalf("scan in namespace: %v", got)
	}
}

func TestStatsEndpoints(t *testing.T) {
	e, ts := newServer(t)
	e.SetConfig("slowlog-log-slower-than", "0")
	call(t, ts, "POST", "/v1/command", `["PING"]`)

	r := call(t, ts, "GET", "/v1/info", ``)
	srv, _ := r.json["server"].(map[string]any)
	if r.status != 200 || srv["cacheboy_version"] != command.Version {
		t.Fatalf("info: %s", r.body)
	}
	var arr []map[string]any
	r = call(t, ts, "GET", "/v1/slowlog", ``)
	json.Unmarshal([]byte(r.body), &arr)
	if r.status != 200 || len(arr) == 0 || arr[0]["args"] == nil {
		t.Fatalf("slowlog: %s", r.body)
	}
	r = call(t, ts, "GET", "/v1/clients", ``)
	json.Unmarshal([]byte(r.body), &arr)
	if r.status != 200 || len(arr) != 1 || arr[0]["kind"] != "http" {
		t.Fatalf("clients: %s", r.body)
	}
	r = call(t, ts, "GET", "/v1/commandstats", ``)
	if r.status != 200 || !strings.Contains(r.body, `"name":"ping"`) {
		t.Fatalf("commandstats: %s", r.body)
	}
	expect(t, call(t, ts, "GET", "/health", ``), 200, `{"status":"ok"}`)
	r = call(t, ts, "GET", "/metrics", ``)
	for _, want := range []string{"cacheboy_used_memory_bytes", "cacheboy_keys ", "cacheboy_connected_clients", "cacheboy_keyspace_hits_total",
		"cacheboy_keyspace_misses_total", "cacheboy_expired_keys_total", "cacheboy_evicted_keys_total", "cacheboy_blocked_clients", "cacheboy_uptime_seconds", "cacheboy_commands_total"} {
		if !strings.Contains(r.body, want) {
			t.Fatalf("metrics missing %s:\n%s", want, r.body)
		}
	}
}

func TestAuth(t *testing.T) {
	e, ts := newServer(t)
	if err := e.SetConfig("requirepass", "s3cret"); err != nil {
		t.Fatal(err)
	}
	if r := call(t, ts, "POST", "/v1/command", `["PING"]`); r.status != 401 || r.json["error"] == nil {
		t.Fatalf("no auth: %d %s", r.status, r.body)
	}
	if r := call(t, ts, "GET", "/v1/info", ``, "Authorization", "Bearer wrong"); r.status != 401 {
		t.Fatalf("wrong password: %d", r.status)
	}
	expect(t, call(t, ts, "POST", "/v1/command", `["PING"]`, "Authorization", "Bearer s3cret"), 200, `{"result":"PONG"}`)
	expect(t, call(t, ts, "GET", "/health", ``), 200, `{"status":"ok"}`)
	if r := call(t, ts, "GET", "/metrics", ``); r.status != 200 {
		t.Fatalf("metrics should be public: %d", r.status)
	}
	if r := call(t, ts, "GET", "/", ``); r.status != 200 || !strings.Contains(r.body, "<html") {
		t.Fatalf("dashboard should be public: %d", r.status)
	}

	// ACL user: may PING, may not run FLUSHALL or read stats.
	expect(t, call(t, ts, "POST", "/v1/command", `["ACL","SETUSER","bob","on",">pw","~*","+ping","+echo"]`, "Authorization", "Bearer s3cret"), 200, `{"result":"OK"}`)
	req, _ := http.NewRequest("POST", ts.URL+"/v1/command", strings.NewReader(`["PING"]`))
	req.SetBasicAuth("bob", "pw")
	res, err := http.DefaultClient.Do(req)
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("basic auth: %v %v", err, res.StatusCode)
	}
	res.Body.Close()
	if r := call(t, ts, "POST", "/v1/command", `["FLUSHALL"]`, "Authorization", "Bearer bob:pw"); r.status != 403 || !strings.HasPrefix(r.json["error"].(string), "NOPERM") {
		t.Fatalf("NOPERM: %d %s", r.status, r.body)
	}
	if r := call(t, ts, "GET", "/v1/slowlog", ``, "Authorization", "Bearer bob:pw"); r.status != 403 {
		t.Fatalf("slowlog as bob: %d", r.status)
	}
}

func TestCORS(t *testing.T) {
	_, ts := newServer(t)
	req, _ := http.NewRequest("OPTIONS", ts.URL+"/v1/command", nil)
	req.Header.Set("Origin", "http://example.com")
	req.Header.Set("Access-Control-Request-Method", "POST")
	req.Header.Set("Access-Control-Request-Headers", "authorization, content-type")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 204 || res.Header.Get("Access-Control-Allow-Origin") != "*" ||
		!strings.Contains(res.Header.Get("Access-Control-Allow-Headers"), "Authorization") {
		t.Fatalf("preflight: %d %v", res.StatusCode, res.Header)
	}
	r, _ := http.Get(ts.URL + "/health")
	r.Body.Close()
	if r.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatal("missing CORS header on normal response")
	}
}

func TestBlockingCancelled(t *testing.T) {
	e, ts := newServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "POST", ts.URL+"/v1/command", strings.NewReader(`["TEST.BLOCK","q"]`))
	start := time.Now()
	_, err := http.DefaultClient.Do(req)
	if err == nil {
		t.Fatal("expected the request to be cancelled")
	}
	// The server side notices, closes the client and unblocks.
	deadline := time.Now().Add(3 * time.Second)
	for e.Blocking.Blocked() > 0 || e.Metrics.ConnectedClients.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("still blocked=%d clients=%d", e.Blocking.Blocked(), e.Metrics.ConnectedClients.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("too slow")
	}
}

func TestBodyLimit(t *testing.T) {
	_, ts := newServer(t)
	big := `["ECHO","` + strings.Repeat("x", 17<<20) + `"]`
	if r := call(t, ts, "POST", "/v1/command", big); r.status != 413 {
		t.Fatalf("big body: %d", r.status)
	}
}
