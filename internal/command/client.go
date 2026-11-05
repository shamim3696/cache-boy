package command

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/transaction"
)

// outBuffer is how many replies/pushes can be queued for a client before a
// slow subscriber is disconnected (like Redis' client-output-buffer-limit).
const outBuffer = 8192

// Client is the state of one connection (TCP, HTTP request, WebSocket, or
// internal: AOF loading, replication link, scripts).
type Client struct {
	ID      int64
	Addr    string
	Created time.Time
	Kind    string // "tcp", "http", "ws", "internal", "master"

	// Fields below are only touched by the goroutine running this client's
	// commands, except where atomic.
	User      string // authenticated user name; "" = not authenticated
	Namespace string
	Multi     *transaction.Queue
	Watcher   *transaction.Watcher
	IsMaster  bool // replication link from our leader: bypasses READONLY
	Tracking  bool

	name       atomic.Pointer[string]
	proto      atomic.Int32
	lastCmd    atomic.Pointer[string]
	lastActive atomic.Int64

	out       chan resp.Value
	done      chan struct{}
	closeOnce sync.Once
	closeFn   func()

	// Hijack, when set by a handler, is called by the TCP server after the
	// reply is flushed; it then owns the connection (used by replication SYNC).
	Hijack func(conn net.Conn, r *resp.Reader)
	// CloseAfterReply makes the server close the connection after the reply (QUIT).
	CloseAfterReply bool
}

func newClient(id int64, kind, addr string) *Client {
	c := &Client{
		ID:      id,
		Kind:    kind,
		Addr:    addr,
		Created: time.Now(),
		out:     make(chan resp.Value, outBuffer),
		done:    make(chan struct{}),
	}
	c.proto.Store(2)
	empty := ""
	c.name.Store(&empty)
	c.lastCmd.Store(&empty)
	c.lastActive.Store(time.Now().UnixMilli())
	return c
}

func (c *Client) ClientID() int64     { return c.ID }
func (c *Client) Proto() int          { return int(c.proto.Load()) }
func (c *Client) SetProto(p int)      { c.proto.Store(int32(p)) }
func (c *Client) Name() string        { return *c.name.Load() }
func (c *Client) SetName(n string)    { c.name.Store(&n) }
func (c *Client) LastCommand() string { return *c.lastCmd.Load() }
func (c *Client) Idle() time.Duration {
	return time.Since(time.UnixMilli(c.lastActive.Load()))
}

// Authenticated reports whether the client may run normal commands.
func (c *Client) Authenticated() bool { return c.User != "" }

// Out is the stream of replies and pushes to write to the connection.
func (c *Client) Out() <-chan resp.Value { return c.out }

// Done is closed when the client disconnects.
func (c *Client) Done() <-chan struct{} { return c.done }

// SetCloser sets the function that closes the underlying connection.
func (c *Client) SetCloser(fn func()) { c.closeFn = fn }

// Reply queues a command reply; it waits if the buffer is full.
func (c *Client) Reply(v resp.Value) {
	if v.IsNone() {
		return
	}
	select {
	case c.out <- v:
	case <-c.done:
	}
}

// Push queues an out-of-band message (pub/sub, invalidation, monitor). It
// never blocks: a client that can't keep up is disconnected.
func (c *Client) Push(v resp.Value) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.out <- v:
		return true
	default:
		c.Close()
		return false
	}
}

// Close disconnects the client. Safe to call more than once.
func (c *Client) Close() {
	c.closeOnce.Do(func() {
		close(c.done)
		if c.closeFn != nil {
			c.closeFn()
		}
	})
}

func (c *Client) Closed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}
