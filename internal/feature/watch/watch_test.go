package watch

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/feature/namespace"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

type sub struct {
	id   int64
	msgs chan resp.Value
}

func (s *sub) Push(v resp.Value) bool { s.msgs <- v; return true }
func (s *sub) ClientID() int64        { return s.id }

func TestWatchPatternsAndNamespaces(t *testing.T) {
	h := New(nil, nil)
	defer h.Close()
	s := &sub{id: 1, msgs: make(chan resp.Value, 10)}
	h.Watch(s, "", "user:*")
	h.Watch(s, "app", "*")
	h.Notify(store.Event{Key: "user:1", Op: "set"})
	h.Notify(store.Event{Key: "other", Op: "set"})
	h.Notify(store.Event{Key: namespace.Prefix("app") + "k", Op: "del"})
	h.Notify(store.Event{Key: namespace.Prefix("zzz") + "user:1", Op: "set"})
	h.Notify(store.Event{Op: "flushall"})
	if len(s.msgs) != 2 {
		t.Fatalf("%d pushes", len(s.msgs))
	}
	if v := <-s.msgs; v.Elems[1].Str != "user:1" || v.Elems[2].Str != "set" {
		t.Fatalf("push %v", v)
	}
	if v := <-s.msgs; v.Elems[1].Str != "k" || v.Elems[2].Str != "del" {
		t.Fatalf("push %v", v)
	}
	h.RemoveClient(1)
	h.Notify(store.Event{Key: "user:1", Op: "set"})
	if len(s.msgs) != 0 || h.Count(1) != 0 {
		t.Fatal("RemoveClient")
	}
}

func TestWebhookRetriesAndGate(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.ReadAll(r.Body)
		if hits.Add(1) < 3 { // fails twice, then succeeds
			w.WriteHeader(http.StatusBadGateway)
		}
	}))
	defer srv.Close()
	h := New(nil, nil)
	defer h.Close()
	h.Backoff = func(int) time.Duration { return time.Millisecond }
	h.AddWebhook("", "w", "*", srv.URL, "")
	h.Notify(store.Event{Key: "k", Op: "set"})
	deadline := time.Now().Add(3 * time.Second)
	for h.Webhooks("")[0].Delivered() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("not delivered: hits=%d failed=%d", hits.Load(), h.Webhooks("")[0].Failed())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if hits.Load() != 3 {
		t.Fatalf("%d attempts", hits.Load())
	}
	// A closed gate (replica) suppresses deliveries.
	h.SetDeliveryGate(func() bool { return false })
	h.Notify(store.Event{Key: "k", Op: "set"})
	time.Sleep(50 * time.Millisecond)
	if hits.Load() != 3 {
		t.Fatal("delivered through a closed gate")
	}
}
