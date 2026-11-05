package ws

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"cache-boy/internal/api/apitest"
	"cache-boy/internal/command"
	"cache-boy/internal/config"
)

func init() { apitest.Register() }

func setup(t *testing.T) (*command.Engine, string) {
	t.Helper()
	e := command.New(config.NewStore(config.Default()))
	t.Cleanup(e.Stop)
	ts := httptest.NewServer(Handler(e))
	t.Cleanup(ts.Close)
	return e, "ws" + strings.TrimPrefix(ts.URL, "http")
}

type wsConn struct {
	t *testing.T
	c *websocket.Conn
}

func dial(t *testing.T, url string, hdr http.Header) (*wsConn, *http.Response, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, res, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: hdr})
	if err != nil {
		return nil, res, err
	}
	t.Cleanup(func() { c.CloseNow() })
	return &wsConn{t, c}, res, nil
}

func (w *wsConn) send(msg string) {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.c.Write(ctx, websocket.MessageText, []byte(msg)); err != nil {
		w.t.Fatal(err)
	}
}

func (w *wsConn) recv() string {
	w.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, b, err := w.c.Read(ctx)
	if err != nil {
		w.t.Fatal(err)
	}
	return string(b)
}

func (w *wsConn) expect(want string) {
	w.t.Helper()
	if got := w.recv(); got != want {
		w.t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCommandsAndPushes(t *testing.T) {
	e, url := setup(t)
	w, _, err := dial(t, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.send(`{"id":1,"cmd":["PING"]}`)
	w.expect(`{"id":1,"result":"PONG"}`)
	w.send(`["ECHO","hi"]`)
	w.expect(`{"result":"hi"}`)
	w.send(`{"id":"x","cmd":["NOSUCH"]}`)
	if got := w.recv(); !strings.HasPrefix(got, `{"id":"x","error":"ERR unknown command`) {
		t.Fatal(got)
	}
	w.send(`garbage`)
	if got := w.recv(); !strings.Contains(got, `"error"`) {
		t.Fatal(got)
	}

	w.send(`{"id":2,"cmd":["SUBSCRIBE","news"]}`)
	w.expect(`{"id":2,"result":[["subscribe","news",1]]}`)
	if n := e.PubSub.Publish("news", "hello"); n != 1 {
		t.Fatalf("publish reached %d", n)
	}
	w.expect(`{"push":["message","news","hello"]}`)
	// Commands still work while subscribed.
	w.send(`{"id":3,"cmd":["PING"]}`)
	w.expect(`{"id":3,"result":"PONG"}`)
}

func TestBlockingAndDisconnect(t *testing.T) {
	e, url := setup(t)
	w, _, err := dial(t, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.send(`{"id":1,"cmd":["TEST.BLOCK","k"]}`)
	deadline := time.Now().Add(3 * time.Second)
	for e.Blocking.Blocked() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("never blocked")
		}
		time.Sleep(5 * time.Millisecond)
	}
	w.c.CloseNow()
	for e.Blocking.Blocked() > 0 || e.Metrics.ConnectedClients.Load() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("blocked=%d clients=%d", e.Blocking.Blocked(), e.Metrics.ConnectedClients.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestAuth(t *testing.T) {
	e, url := setup(t)
	e.SetConfig("requirepass", "pw")

	// Wrong token: rejected before upgrading.
	if _, res, err := dial(t, url+"?token=nope", nil); err == nil || res == nil || res.StatusCode != 401 {
		t.Fatalf("bad token: %v", err)
	}
	// Token in the query string.
	w, _, err := dial(t, url+"?token=pw", nil)
	if err != nil {
		t.Fatal(err)
	}
	w.send(`{"id":1,"cmd":["PING"]}`)
	w.expect(`{"id":1,"result":"PONG"}`)

	// Authorization header.
	w, _, err = dial(t, url, http.Header{"Authorization": {"Bearer pw"}})
	if err != nil {
		t.Fatal(err)
	}
	w.send(`["PING"]`)
	w.expect(`{"result":"PONG"}`)

	// No credentials: commands fail until an auth message.
	w, _, err = dial(t, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	w.send(`{"id":1,"cmd":["PING"]}`)
	if got := w.recv(); !strings.Contains(got, "NOAUTH") {
		t.Fatal(got)
	}
	w.send(`{"id":2,"auth":["default","bad"]}`)
	if got := w.recv(); !strings.Contains(got, "WRONGPASS") {
		t.Fatal(got)
	}
	w.send(`{"id":3,"auth":["default","pw"]}`)
	w.expect(`{"id":3,"result":"OK"}`)
	w.send(`{"id":4,"cmd":["PING"]}`)
	w.expect(`{"id":4,"result":"PONG"}`)
}

func TestNamespaceAndQuit(t *testing.T) {
	_, url := setup(t)
	w, _, err := dial(t, url+"?namespace=app", nil)
	if err != nil {
		t.Fatal(err)
	}
	w.send(`{"id":1,"cmd":["NAMESPACE","CURRENT"]}`)
	w.expect(`{"id":1,"result":"app"}`)
	w.send(`{"id":2,"cmd":["QUIT"]}`)
	w.expect(`{"id":2,"result":"OK"}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := w.c.Read(ctx); websocket.CloseStatus(err) != websocket.StatusNormalClosure {
		t.Fatalf("expected normal close, got %v", err)
	}
}
