// Package httpapi is the REST API: JSON commands, pipelines and
// transactions, a key browser, server stats, Prometheus metrics, the
// WebSocket endpoint (/v1/ws) and the dashboard (/).
//
// Every /v1 request runs as a short-lived client (e.NewClient("http", ...)),
// so ACLs, namespaces, stats and blocking behave as for a TCP connection.
package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"cache-boy/internal/api/apiutil"
	"cache-boy/internal/api/ws"
	"cache-boy/internal/command"
	"cache-boy/internal/protocol/resp"
	"cache-boy/web/dashboard"
)

// NamespaceHeader selects the namespace of a request.
const NamespaceHeader = "X-CacheBoy-Namespace"

type api struct {
	e *command.Engine
}

// New returns the handler serving the whole HTTP interface.
func New(e *command.Engine) http.Handler {
	a := &api{e: e}
	mux := http.NewServeMux()

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, json.RawMessage(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /metrics", a.metrics)

	mux.Handle("/v1/ws", ws.Handler(e))
	mux.HandleFunc("POST /v1/command", a.withClient(a.command))
	mux.HandleFunc("POST /v1/pipeline", a.withClient(a.pipeline))
	mux.HandleFunc("POST /v1/multi", a.withClient(a.multi))
	mux.HandleFunc("GET /v1/keys/{key...}", a.withClient(a.getKey))
	mux.HandleFunc("PUT /v1/keys/{key...}", a.withClient(a.putKey))
	mux.HandleFunc("DELETE /v1/keys/{key...}", a.withClient(a.deleteKey))
	mux.HandleFunc("GET /v1/scan", a.withClient(a.scan))
	mux.HandleFunc("GET /v1/info", a.withClient(a.info))
	mux.HandleFunc("GET /v1/slowlog", a.withClient(a.slowlog))
	mux.HandleFunc("GET /v1/clients", a.withClient(a.clients))
	mux.HandleFunc("GET /v1/commandstats", a.withClient(a.commandStats))
	mux.HandleFunc("/v1/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "ERR unknown endpoint "+r.Method+" "+r.URL.Path)
	})

	mux.Handle("/", dashboard.Handler())
	return cors(mux)
}

// cors allows browser apps on any origin (auth is by header, not cookies).
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", "*")
		if r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != "" {
			h.Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			h.Set("Access-Control-Allow-Headers", "Authorization, Content-Type, "+NamespaceHeader)
			h.Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handlerFunc is an authenticated /v1 handler with its client.
type handlerFunc func(w http.ResponseWriter, r *http.Request, c *command.Client)

// withClient authenticates the request, creates its client, applies the
// namespace header, and closes the client if the request is cancelled (which
// wakes blocking commands such as BLPOP).
func (a *api) withClient(h handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		user, err := apiutil.RequestUser(a.e, r, false)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="cache-boy"`)
			writeError(w, http.StatusUnauthorized, err.Error())
			return
		}
		ns, err := apiutil.Namespace(r.Header.Get(NamespaceHeader))
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, apiutil.MaxBody)

		c := a.e.NewClient("http", r.RemoteAddr)
		defer a.e.RemoveClient(c)
		c.User = user
		c.Namespace = ns
		c.SetProto(3)

		finished := make(chan struct{})
		defer close(finished)
		go func(ctx context.Context) {
			select {
			case <-ctx.Done():
				c.Close()
			case <-finished:
			}
		}(r.Context())

		h(w, r, c)
	}
}

// exec runs one command; replies are never "none" for callers.
func (a *api) exec(c *command.Client, args ...string) resp.Value {
	v := a.e.Execute(c, args)
	if v.IsNone() {
		return resp.Null()
	}
	return v
}

// readBody reads a limited body, writing 400/413 on failure.
func readBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeError(w, http.StatusRequestEntityTooLarge, "ERR request body larger than 16 MB")
		} else {
			writeError(w, http.StatusBadRequest, "ERR reading body: "+err.Error())
		}
		return nil, false
	}
	return b, true
}

// writeResult writes {"result": v} or {"error": ...} with the right status.
func writeResult(w http.ResponseWriter, v resp.Value) {
	if v.IsError() {
		writeError(w, apiutil.ErrorStatus(v.Str), v.Str)
		return
	}
	b := append([]byte(`{"result":`), apiutil.JSON(v)...)
	b = append(b, '}')
	writeJSON(w, http.StatusOK, json.RawMessage(b))
}

func writeError(w http.ResponseWriter, status int, msg string) {
	b := append([]byte(`{"error":`), apiutil.AppendString(nil, msg)...)
	b = append(b, '}')
	writeJSON(w, status, json.RawMessage(b))
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	var b []byte
	if raw, ok := v.(json.RawMessage); ok {
		b = raw
	} else {
		var err error
		if b, err = json.Marshal(v); err != nil {
			status = http.StatusInternalServerError
			b = []byte(`{"error":"ERR encoding response"}`)
		}
	}
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	w.Write(b)
	w.Write([]byte{'\n'})
}

func lower(s string) string { return strings.ToLower(s) }
