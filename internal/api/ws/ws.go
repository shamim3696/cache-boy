// Package ws serves commands over WebSocket (mounted at /v1/ws).
//
// Client → server text messages:
//
//	{"id": <any>, "cmd": ["GET", "a"]}   a command (a bare ["GET","a"] also works)
//	{"id": <any>, "auth": ["user", "pass"]}   or {"auth": "pass"}
//
// Server → client:
//
//	{"id": ..., "result": <json>}  /  {"id": ..., "error": "..."}
//	{"push": <json>}   pub/sub messages, KEYWATCH events, invalidations, MONITOR
//
// Authentication: ?token=<password or user:pass>, the Authorization header
// (Bearer / Basic), or an {"auth": ...} message. The connection speaks RESP3
// semantics, so commands keep working while subscribed.
package ws

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/coder/websocket"

	"github.com/shamim3696/cache-boy/internal/api/apiutil"
	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// NamespaceHeader / ?namespace= select the connection's initial namespace.
const NamespaceHeader = "X-CacheBoy-Namespace"

var (
	pingInterval = 30 * time.Second
	writeTimeout = 10 * time.Second
)

// Handler returns the WebSocket endpoint.
func Handler(e *command.Engine) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serve(e, w, r)
	})
}

type message struct {
	ID   json.RawMessage `json:"id,omitempty"`
	Cmd  json.RawMessage `json:"cmd,omitempty"`
	Auth json.RawMessage `json:"auth,omitempty"`
}

func serve(e *command.Engine, w http.ResponseWriter, r *http.Request) {
	user := ""
	hasCreds := r.Header.Get("Authorization") != "" || r.URL.Query().Get("token") != ""
	if hasCreds || e.Auth.AutoLogin() {
		u, err := apiutil.RequestUser(e, r, true)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write(append(append([]byte(`{"error":`), apiutil.AppendString(nil, err.Error())...), '}'))
			return
		}
		user = u
	}
	nsName := r.Header.Get(NamespaceHeader)
	if nsName == "" {
		nsName = r.URL.Query().Get("namespace")
	}
	ns, err := apiutil.Namespace(nsName)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Same policy as the REST API's CORS "*": auth is by token, not cookies.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(apiutil.MaxBody)

	c := e.NewClient("ws", r.RemoteAddr)
	c.User = user // "" until an auth message when a password is required
	c.Namespace = ns
	c.SetProto(3)
	c.SetCloser(func() { conn.CloseNow() })
	defer e.RemoveClient(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s := &session{e: e, c: c, conn: conn, replies: make(chan []byte, 256)}
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		s.writeLoop(ctx)
	}()

	// The reader keeps reading (and so answers pings and sees close frames)
	// while a command runs, e.g. a long BLPOP. If the peer goes away, the
	// client is closed, which wakes blocked commands.
	inbox := make(chan []byte, 64)
	go func() {
		defer close(inbox)
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				c.Close()
				return
			}
			if typ != websocket.MessageText && typ != websocket.MessageBinary {
				continue
			}
			select {
			case inbox <- data:
			case <-c.Done():
				return
			}
		}
	}()

	s.execLoop(inbox)
	close(s.replies) // the writer flushes what is queued, then stops
	select {
	case <-writerDone:
	case <-time.After(writeTimeout):
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

type session struct {
	e       *command.Engine
	c       *command.Client
	conn    *websocket.Conn
	replies chan []byte
}

// execLoop runs commands one at a time, in order, like a TCP connection.
func (s *session) execLoop(inbox <-chan []byte) {
	for data := range inbox {
		reply := s.handle(data)
		select {
		case s.replies <- reply:
		case <-s.c.Done():
			return
		}
		if s.c.CloseAfterReply || s.c.Closed() {
			return
		}
	}
}

// handle runs one message and returns the encoded reply.
func (s *session) handle(data []byte) []byte {
	data = bytes.TrimSpace(data)
	var m message
	if len(data) > 0 && data[0] == '[' {
		m.Cmd = data
	} else if err := json.Unmarshal(data, &m); err != nil {
		return encodeReply(nil, resp.Err("ERR invalid message: expected {\"id\":..., \"cmd\":[...]}"))
	}
	if len(m.Auth) > 0 {
		return encodeReply(m.ID, s.auth(m.Auth))
	}
	if len(m.Cmd) == 0 {
		return encodeReply(m.ID, resp.Err("ERR message has no \"cmd\""))
	}
	args, err := apiutil.ParseCommand(m.Cmd)
	if err != nil {
		return encodeReply(m.ID, resp.Err("ERR "+err.Error()))
	}
	v := s.e.Execute(s.c, args)
	if v.IsNone() {
		v = resp.Null()
	}
	return encodeReply(m.ID, v)
}

// auth handles {"auth": ["user","pass"]} and {"auth": "pass"}.
func (s *session) auth(raw json.RawMessage) resp.Value {
	var user, pass string
	var pair []string
	if err := json.Unmarshal(raw, &pair); err == nil {
		switch len(pair) {
		case 1:
			pass = pair[0]
		case 2:
			user, pass = pair[0], pair[1]
		default:
			return resp.Err("ERR auth must be [\"user\",\"pass\"] or \"pass\"")
		}
	} else if err := json.Unmarshal(raw, &pass); err != nil {
		return resp.Err("ERR auth must be [\"user\",\"pass\"] or \"pass\"")
	}
	var u string
	var err error
	if user == "" {
		u, err = apiutil.Login(s.e, pass)
	} else {
		u, err = apiutil.LoginUser(s.e, user, pass)
	}
	if err != nil || pass == "" && user == "" {
		if err == nil {
			err = errors.New("WRONGPASS invalid username-password pair or user is disabled.")
		}
		return resp.Err(err.Error())
	}
	s.c.User = u
	return resp.OK
}

func encodeReply(id json.RawMessage, v resp.Value) []byte {
	b := []byte{'{'}
	if len(id) > 0 {
		b = append(b, `"id":`...)
		b = append(b, id...)
		b = append(b, ',')
	}
	if v.IsError() {
		b = append(b, `"error":`...)
		b = apiutil.AppendString(b, v.Str)
	} else {
		b = append(b, `"result":`...)
		b = apiutil.AppendJSON(b, v)
	}
	return append(b, '}')
}

func encodePush(v resp.Value) []byte {
	b := append([]byte(`{"push":`), apiutil.AppendJSON(nil, v)...)
	return append(b, '}')
}

// writeLoop is the only goroutine writing data frames. Pushes queued before
// a reply was produced are written before that reply.
func (s *session) writeLoop(ctx context.Context) {
	out := s.c.Out()
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	write := func(b []byte) bool {
		wctx, cancel := context.WithTimeout(ctx, writeTimeout)
		defer cancel()
		if err := s.conn.Write(wctx, websocket.MessageText, b); err != nil {
			s.c.Close()
			return false
		}
		return true
	}
	drainPushes := func() bool {
		for len(out) > 0 {
			if !write(encodePush(<-out)) {
				return false
			}
		}
		return true
	}
	for {
		select {
		case b, ok := <-s.replies:
			if !drainPushes() {
				return
			}
			if !ok {
				return
			}
			if !write(b) {
				return
			}
		case v := <-out:
			if !write(encodePush(v)) {
				return
			}
		case <-ticker.C:
			go func() {
				pctx, cancel := context.WithTimeout(ctx, writeTimeout)
				defer cancel()
				if err := s.conn.Ping(pctx); err != nil && ctx.Err() == nil {
					s.c.Close()
				}
			}()
		case <-s.c.Done():
			return
		}
	}
}
