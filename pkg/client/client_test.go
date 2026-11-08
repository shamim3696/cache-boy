package client

import (
	"context"
	"errors"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/api/apitest"
	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/server"
)

func init() { apitest.Register() }

// startServer runs a real in-process cache-boy server.
func startServer(t *testing.T) (*command.Engine, string) {
	t.Helper()
	e := command.New(config.NewStore(config.Default()))
	srv := server.New(e)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { srv.Close(); e.Stop() })
	return e, srv.Addr().String()
}

func dialT(t *testing.T, addr string, opts ...Option) *Client {
	t.Helper()
	c, err := Dial(addr, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestCoreCommands(t *testing.T) {
	_, addr := startServer(t)
	c := dialT(t, addr)
	ctx := context.Background()

	if v, err := c.Do(ctx, "PING"); err != nil || v.String() != "PONG" {
		t.Fatalf("PING: %v %v", v, err)
	}
	if v, err := c.Do(ctx, "ECHO", 42); err != nil || v.String() != "42" {
		t.Fatalf("ECHO: %v %v", v, err)
	}
	_, err := c.Do(ctx, "NOSUCH")
	var se Error
	if !errors.As(err, &se) || se.Code() != "ERR" {
		t.Fatalf("unknown command: %v", err)
	}

	if ok, err := c.Set(ctx, "k", "v", SetOptions{TTL: 10 * time.Second}); !ok || err != nil {
		t.Fatalf("Set: %v %v", ok, err)
	}
	if v, err := c.Get(ctx, "k"); v != "v" || err != nil {
		t.Fatalf("Get: %q %v", v, err)
	}
	if ttl, err := c.TTL(ctx, "k"); err != nil || ttl <= 9*time.Second || ttl > 10*time.Second {
		t.Fatalf("TTL: %v %v", ttl, err)
	}
	if ok, _ := c.Set(ctx, "k", "other", SetOptions{NX: true}); ok {
		t.Fatal("SET NX on existing key succeeded")
	}
	if _, err := c.Get(ctx, "missing"); err != ErrNil {
		t.Fatalf("Get missing: %v", err)
	}
	if _, err := c.TTL(ctx, "missing"); err != ErrNil {
		t.Fatalf("TTL missing: %v", err)
	}
	if n, err := c.Incr(ctx, "n"); n != 1 || err != nil {
		t.Fatalf("Incr: %v %v", n, err)
	}
	if n, err := c.Del(ctx, "k", "n", "missing"); n != 2 || err != nil {
		t.Fatalf("Del: %v %v", n, err)
	}
	if n, err := c.HSet(ctx, "h", map[string]any{"a": 1, "b": "x"}); n != 2 || err != nil {
		t.Fatalf("HSet: %v %v", n, err)
	}
	if m, err := c.HGetAll(ctx, "h"); err != nil || !reflect.DeepEqual(m, map[string]string{"a": "1", "b": "x"}) {
		t.Fatalf("HGetAll: %v %v", m, err)
	}

	p := c.Pipeline()
	p.Do("SET", "p", 1).Do("INCR", "p").Do("NOSUCH").Do("GET", "p")
	vals, err := p.Exec(ctx)
	if err != nil || len(vals) != 4 || vals[1].Int != 2 || !vals[2].IsError() || vals[3].String() != "2" {
		t.Fatalf("Pipeline: %v %v", vals, err)
	}

	// A transaction must stay on one connection: send it as a pipeline.
	p.Do("MULTI").Do("SET", "m", "1").Do("EXEC")
	if vals, err := p.Exec(ctx); err != nil || len(vals[2].Elems) != 1 {
		t.Fatalf("MULTI in pipeline: %v %v", vals, err)
	}
}

func TestPubSub(t *testing.T) {
	_, addr := startServer(t)
	c := dialT(t, addr)
	ctx := context.Background()
	sub, err := c.Subscribe(ctx, "news")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	psub, err := c.PSubscribe(ctx, "n*")
	if err != nil {
		t.Fatal(err)
	}
	defer psub.Close()
	if n, err := c.Publish(ctx, "news", "hello"); n != 2 || err != nil {
		t.Fatalf("Publish: %v %v", n, err)
	}
	select {
	case m := <-sub.C:
		if m.Kind != "message" || m.Channel != "news" || m.Payload != "hello" {
			t.Fatalf("message: %+v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no message")
	}
	select {
	case m := <-psub.C:
		if m.Kind != "pmessage" || m.Pattern != "n*" || m.Channel != "news" || m.Payload != "hello" {
			t.Fatalf("pmessage: %+v", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no pmessage")
	}
	sub.Close()
	if _, ok := <-sub.C; ok {
		t.Fatal("channel not closed after Close")
	}
}

func TestAuthNamespaceProtocol(t *testing.T) {
	e, addr := startServer(t)
	e.SetConfig("requirepass", "pw")
	if _, err := Dial(addr); err != nil {
		t.Fatalf("dial without password should connect: %v", err)
	}
	c0, _ := Dial(addr)
	if _, err := c0.Do(context.Background(), "PING"); err == nil || !strings.HasPrefix(err.Error(), "NOAUTH") {
		t.Fatalf("expected NOAUTH, got %v", err)
	}
	c0.Close()
	if _, err := Dial(addr, Password("bad")); err == nil {
		t.Fatal("wrong password accepted")
	}
	c := dialT(t, addr, Password("pw"), Namespace("tenant"), Protocol(3))
	ctx := context.Background()
	if v, err := c.Do(ctx, "NAMESPACE", "CURRENT"); err != nil || v.String() != "tenant" {
		t.Fatalf("namespace: %v %v", v, err)
	}
	v, err := c.Do(ctx, "CONFIG", "GET", "maxclients")
	if err != nil || v.Type != resp.TypeMap {
		t.Fatalf("RESP3 map expected: %v %v", v, err)
	}
	cu := dialT(t, addr, Username("default"), Password("pw"))
	if _, err := cu.Do(ctx, "PING"); err != nil {
		t.Fatal(err)
	}
}

func TestContextCancel(t *testing.T) {
	e, addr := startServer(t)
	c := dialT(t, addr, PoolSize(2))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := c.Do(ctx, "TEST.BLOCK", "q"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("cancel too slow")
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel2() }()
	if _, err := c.Do(ctx2, "TEST.BLOCK", "q"); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected canceled, got %v", err)
	}
	// The pool still works and the broken connections were dropped.
	if v, err := c.Do(context.Background(), "PING"); err != nil || v.String() != "PONG" {
		t.Fatal(v, err)
	}
	// Note: the TCP server only notices the closed connections when the
	// blocked command ends, so e.Blocking.Blocked() isn't checked here.
	// Unblock them so the server shuts down quickly.
	for _, cl := range e.Clients() {
		if cl.LastCommand() == "test.block" {
			cl.Close()
		}
	}
}

func TestPoolConcurrency(t *testing.T) {
	e, addr := startServer(t)
	c := dialT(t, addr, PoolSize(4))
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				if _, err := c.Incr(context.Background(), "ctr"); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if v, _ := c.Get(context.Background(), "ctr"); v != "1600" {
		t.Fatalf("ctr = %s", v)
	}
	if n := e.Metrics.ConnectedClients.Load(); n > 4 {
		t.Fatalf("pool opened %d connections", n)
	}
	c.Close()
	if _, err := c.Do(context.Background(), "PING"); err != ErrClosed {
		t.Fatalf("after Close: %v", err)
	}
}

// --- fake server for feature helpers' wire format ---

type fakeServer struct {
	mu      sync.Mutex
	got     [][]string
	handler func(args []string) resp.Value
	addr    string
}

func newFake(t *testing.T, handler func(args []string) resp.Value) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeServer{handler: handler, addr: ln.Addr().String()}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go f.serve(nc)
		}
	}()
	return f
}

func (f *fakeServer) serve(nc net.Conn) {
	defer nc.Close()
	r := resp.NewReader(nc)
	w := resp.NewWriter(nc, 2)
	for {
		args, err := r.ReadCommand()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.got = append(f.got, args)
		f.mu.Unlock()
		v := f.handler(args)
		if v.Type == resp.TypePush { // "streaming": write the elements as separate pushes
			for _, e := range v.Elems {
				w.Write(e)
			}
		} else {
			w.Write(v)
		}
		w.Flush()
	}
}

func (f *fakeServer) last() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.got) == 0 {
		return ""
	}
	return strings.Join(f.got[len(f.got)-1], " ")
}

func (f *fakeServer) all() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, g := range f.got {
		out = append(out, strings.Join(g, " "))
	}
	return out
}

func TestFeatureWireFormat(t *testing.T) {
	cacheStatus := "hit"
	f := newFake(t, func(args []string) resp.Value {
		switch strings.ToUpper(args[0]) {
		case "TAG.ADD", "TAG.INVALIDATE":
			return resp.Int(2)
		case "RATELIMIT":
			return resp.Array(resp.Int(0), resp.Int(0), resp.Int(1500), resp.Int(60000))
		case "LOCK":
			if args[1] == "busy" {
				return resp.Null()
			}
			return resp.Int(7)
		case "UNLOCK", "LOCK.EXTEND", "QACK", "QNACK", "CACHE.RELEASE":
			return resp.Int(1)
		case "QPUSH":
			return resp.Bulk("job-1")
		case "QPOP":
			if args[1] == "empty" {
				return resp.NullArray()
			}
			return resp.Array(resp.Bulk("job-1"), resp.Bulk("payload"), resp.Int(2))
		case "CACHE.GET":
			if cacheStatus == "hit" {
				return resp.Array(resp.Bulk("hit"), resp.Bulk("cached"))
			}
			return resp.Array(resp.Bulk(cacheStatus), resp.Null())
		case "SET":
			return resp.OK
		case "EVAL":
			return resp.Int(5)
		case "ZADD":
			return resp.Int(2)
		}
		return resp.Err("ERR unexpected " + args[0])
	})
	c := dialT(t, f.addr)
	ctx := context.Background()
	check := func(want string) {
		t.Helper()
		if got := f.last(); got != want {
			t.Fatalf("sent %q, want %q", got, want)
		}
	}

	c.TagAdd(ctx, "k", "a", "b")
	check("TAG.ADD k a b")
	c.TagInvalidate(ctx, "a", "b")
	check("TAG.INVALIDATE a b")
	c.Set(ctx, "k", "v", SetOptions{TTL: 1500 * time.Millisecond, NX: true, Tags: []string{"t1", "t2"}})
	check("SET k v PX 1500 NX TAGS t1 t2")
	c.Set(ctx, "k", 3, SetOptions{TTL: time.Minute, XX: true})
	check("SET k 3 EX 60 XX")
	c.ZAdd(ctx, "z", Z{"a", 1.5}, Z{"b", 2})
	check("ZADD z 1.5 a 2 b")

	rl, err := c.RateLimit(ctx, "api:1", 100, time.Minute, 3)
	check("RATELIMIT api:1 100 60000 COST 3")
	if err != nil || rl.Allowed || rl.RetryAfter != 1500*time.Millisecond || rl.ResetAfter != time.Minute {
		t.Fatalf("RateLimit: %+v %v", rl, err)
	}

	l, err := c.Lock(ctx, "res", 10*time.Second, LockOptions{Wait: 2 * time.Second, Owner: "w1"})
	check("LOCK res 10000 WAIT 2000 OWNER w1")
	if err != nil || l.Token != 7 {
		t.Fatalf("Lock: %+v %v", l, err)
	}
	l.Extend(ctx, 5*time.Second)
	check("LOCK.EXTEND res 7 5000")
	if ok, err := l.Unlock(ctx); !ok || err != nil {
		t.Fatal(ok, err)
	}
	check("UNLOCK res 7")
	if _, err := c.Lock(ctx, "busy", time.Second); err != ErrNotAcquired {
		t.Fatalf("busy lock: %v", err)
	}

	id, _ := c.QPush(ctx, "jobs", "payload", QPushOptions{Delay: time.Second, Retries: 5, ID: "x"})
	check("QPUSH jobs payload DELAY 1000 RETRIES 5 ID x")
	if id != "job-1" {
		t.Fatal(id)
	}
	j, err := c.QPop(ctx, "jobs", QPopOptions{Visibility: 30 * time.Second, Block: 2 * time.Second})
	check("QPOP jobs VISIBILITY 30000 BLOCK 2000")
	if err != nil || j.ID != "job-1" || j.Payload != "payload" || j.Attempt != 2 {
		t.Fatalf("QPop: %+v %v", j, err)
	}
	c.QPop(ctx, "jobs", QPopOptions{Forever: true})
	check("QPOP jobs BLOCK 0")
	if _, err := c.QPop(ctx, "empty"); err != ErrNil {
		t.Fatalf("empty QPop: %v", err)
	}
	j.Ack(ctx)
	check("QACK jobs job-1")
	j.Nack(ctx, 500*time.Millisecond)
	check("QNACK jobs job-1 DELAY 500")

	r, _ := c.CacheGet(ctx, "page", CacheGetOptions{Lock: 3 * time.Second, Wait: time.Second})
	check("CACHE.GET page LOCK 3000 WAIT 1000")
	if r.Status != CacheHit || r.Value != "cached" {
		t.Fatalf("CacheGet: %+v", r)
	}

	c.Eval(ctx, "return 5", []string{"k1", "k2"}, "a", 1)
	check("EVAL return 5 2 k1 k2 a 1")
}

func TestWrap(t *testing.T) {
	status := "hit"
	f := newFake(t, func(args []string) resp.Value {
		switch strings.ToUpper(args[0]) {
		case "CACHE.GET":
			if status == "hit" {
				return resp.Array(resp.Bulk("hit"), resp.Bulk("cached"))
			}
			return resp.Array(resp.Bulk(status), resp.Null())
		case "SET":
			return resp.OK
		case "CACHE.RELEASE":
			return resp.Int(1)
		}
		return resp.Err("ERR unexpected")
	})
	c := dialT(t, f.addr)
	ctx := context.Background()
	calls := 0
	fn := func(context.Context) (string, error) { calls++; return "fresh", nil }

	if v, err := c.Wrap(ctx, "page", time.Minute, fn); v != "cached" || err != nil || calls != 0 {
		t.Fatalf("hit: %q %v %d", v, err, calls)
	}

	status = "compute"
	if v, err := c.Wrap(ctx, "page", time.Minute, fn); v != "fresh" || err != nil || calls != 1 {
		t.Fatalf("compute: %q %v %d", v, err, calls)
	}
	if got := f.all(); got[len(got)-2] != "CACHE.GET page" || got[len(got)-1] != "SET page fresh EX 60" {
		t.Fatalf("compute sent %q", got)
	}

	boom := errors.New("db down")
	if _, err := c.Wrap(ctx, "page", time.Minute, func(context.Context) (string, error) { return "", boom }); err != boom {
		t.Fatalf("error: %v", err)
	}
	if f.last() != "CACHE.RELEASE page" {
		t.Fatalf("after error sent %q", f.last())
	}

	status = "timeout"
	n := len(f.all())
	if v, err := c.Wrap(ctx, "page", time.Minute, fn); v != "fresh" || err != nil {
		t.Fatalf("timeout: %q %v", v, err)
	}
	if got := f.all(); len(got) != n+1 { // only CACHE.GET, no SET
		t.Fatalf("timeout sent %q", got[n:])
	}
}

func TestKeyWatchFake(t *testing.T) {
	f := newFake(t, func(args []string) resp.Value {
		if strings.ToUpper(args[0]) != "KEYWATCH" {
			return resp.Err("ERR unexpected")
		}
		return resp.Push(
			resp.Array(resp.Bulk("psubscribe"), resp.Bulk(args[1]), resp.Int(1)),
			resp.Array(resp.Bulk("keyevent"), resp.Bulk("user:1"), resp.Bulk("set")),
			resp.Array(resp.Bulk("pmessage"), resp.Bulk("__key__:*"), resp.Bulk("__key__:user:2"), resp.Bulk("del")),
		)
	})
	c := dialT(t, f.addr)
	w, err := c.KeyWatch(context.Background(), "user:*")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if f.last() != "KEYWATCH user:*" {
		t.Fatal(f.last())
	}
	for _, want := range []Message{{Kind: "keyevent", Channel: "user:1", Payload: "set"}, {Kind: "keyevent", Pattern: "__key__:*", Channel: "user:2", Payload: "del"}} {
		select {
		case m := <-w.C:
			m.Raw = Value{}
			if !reflect.DeepEqual(m, want) {
				t.Fatalf("got %+v, want %+v", m, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("timeout")
		}
	}

	// A server without KEYWATCH returns an error immediately.
	f2 := newFake(t, func(args []string) resp.Value { return resp.Err("ERR unknown command 'KEYWATCH'") })
	c2 := dialT(t, f2.addr)
	if _, err := c2.KeyWatch(context.Background(), "*"); err == nil {
		t.Fatal("expected error")
	}
}

func TestArgs(t *testing.T) {
	got, err := Args("a", []byte("b"), 1, int64(-2), uint(3), 1.5, true, 250*time.Millisecond, []string{"x", "y"})
	want := []string{"a", "b", "1", "-2", "3", "1.5", "1", "250", "x", "y"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("%q %v", got, err)
	}
	if _, err := Args("a", nil); err == nil {
		t.Fatal("nil arg accepted")
	}
}
