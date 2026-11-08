// Package client is the Go client for cache-boy (and any RESP server).
//
//	c, err := client.Dial("127.0.0.1:6380", client.Password("secret"))
//	defer c.Close()
//	c.Set(ctx, "greeting", "hello", client.SetOptions{TTL: time.Minute})
//	v, err := c.Get(ctx, "greeting")
//
// A Client is safe for concurrent use; it keeps a pool of connections.
// Do runs any command; typed helpers cover common and cache-boy commands.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Value is a RESP reply. Use v.Type, v.Str, v.Int, v.Float, v.Bool, v.Elems,
// or v.String() for scalars. RESP3 maps keep key, value, key, value... in Elems.
type Value = resp.Value

// ErrNil is returned by typed helpers when the server replies null (missing key...).
var ErrNil = errors.New("cache-boy: nil")

// ErrClosed is returned after Close.
var ErrClosed = errors.New("cache-boy: client closed")

// Error is an error reply from the server, such as "WRONGTYPE ...".
type Error string

func (e Error) Error() string { return string(e) }

// Code returns the error code: "ERR", "WRONGTYPE", "NOPERM"...
func (e Error) Code() string {
	for i := 0; i < len(e); i++ {
		if e[i] == ' ' {
			return string(e[:i])
		}
	}
	return string(e)
}

type options struct {
	username    string
	password    string
	namespace   string
	protocol    int
	dialTimeout time.Duration
	poolSize    int
}

// Option configures Dial.
type Option func(*options)

// Password authenticates as the default user, or as Username if set.
func Password(p string) Option { return func(o *options) { o.password = p } }

// Username sets the ACL user (used with Password).
func Username(u string) Option { return func(o *options) { o.username = u } }

// Namespace isolates every key used by this client (NAMESPACE USE on connect).
func Namespace(ns string) Option { return func(o *options) { o.namespace = ns } }

// Protocol selects RESP2 (default) or RESP3 (maps, doubles, booleans).
func Protocol(v int) Option { return func(o *options) { o.protocol = v } }

// DialTimeout bounds connecting and the handshake (default 5s).
func DialTimeout(d time.Duration) Option { return func(o *options) { o.dialTimeout = d } }

// PoolSize is the maximum number of pooled connections (default 10).
func PoolSize(n int) Option { return func(o *options) { o.poolSize = n } }

// Client is a pool of connections to one server.
type Client struct {
	addr string
	opts options

	sem  chan struct{} // limits open pooled connections
	idle chan *conn

	mu     sync.Mutex
	closed bool
}

// Dial connects to addr ("host:port") and checks the connection (and
// credentials) by opening the first pooled connection.
func Dial(addr string, opts ...Option) (*Client, error) {
	o := options{protocol: 2, dialTimeout: 5 * time.Second, poolSize: 10}
	for _, fn := range opts {
		fn(&o)
	}
	if o.protocol != 2 && o.protocol != 3 {
		return nil, fmt.Errorf("cache-boy: unsupported protocol %d", o.protocol)
	}
	if o.poolSize <= 0 {
		o.poolSize = 1
	}
	c := &Client{
		addr: addr,
		opts: o,
		sem:  make(chan struct{}, o.poolSize),
		idle: make(chan *conn, o.poolSize),
	}
	ctx, cancel := context.WithTimeout(context.Background(), o.dialTimeout)
	defer cancel()
	cn, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	c.put(cn, nil)
	return c, nil
}

// Close closes all idle connections; connections in use close when returned.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	for {
		select {
		case cn := <-c.idle:
			cn.close()
			<-c.sem
		default:
			return nil
		}
	}
}

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// get takes an idle connection or dials a new one when the pool has room.
func (c *Client) get(ctx context.Context) (*conn, error) {
	if c.isClosed() {
		return nil, ErrClosed
	}
	select {
	case cn := <-c.idle:
		return cn, nil
	default:
	}
	select {
	case cn := <-c.idle:
		return cn, nil
	case c.sem <- struct{}{}:
		cn, err := c.dial(ctx)
		if err != nil {
			<-c.sem
			return nil, err
		}
		return cn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// put returns a connection to the pool; it is dropped after a network error.
func (c *Client) put(cn *conn, err error) {
	if err != nil && !isServerError(err) || c.isClosed() || cn.broken {
		cn.close()
		<-c.sem
		return
	}
	select {
	case c.idle <- cn:
	default:
		cn.close()
		<-c.sem
	}
}

func isServerError(err error) bool {
	var e Error
	return errors.As(err, &e) || errors.Is(err, ErrNil)
}

// dial opens and prepares one connection (HELLO/AUTH, NAMESPACE USE).
func (c *Client) dial(ctx context.Context) (*conn, error) {
	d := net.Dialer{Timeout: c.opts.dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		return nil, err
	}
	cn := newConn(nc, c.opts.protocol)
	if err := c.handshake(ctx, cn); err != nil {
		nc.Close()
		return nil, err
	}
	return cn, nil
}

func (c *Client) handshake(ctx context.Context, cn *conn) error {
	o := c.opts
	user := o.username
	if user == "" {
		user = "default"
	}
	var cmds [][]string
	if o.protocol == 3 {
		hello := []string{"HELLO", "3"}
		if o.password != "" {
			hello = append(hello, "AUTH", user, o.password)
		}
		cmds = append(cmds, hello)
	} else if o.password != "" {
		if o.username != "" {
			cmds = append(cmds, []string{"AUTH", o.username, o.password})
		} else {
			cmds = append(cmds, []string{"AUTH", o.password})
		}
	}
	if o.namespace != "" {
		cmds = append(cmds, []string{"NAMESPACE", "USE", o.namespace})
	}
	if len(cmds) == 0 {
		return nil
	}
	replies, err := cn.roundTrip(ctx, cmds)
	if err != nil {
		return err
	}
	for _, v := range replies {
		if v.IsError() {
			return Error(v.Str)
		}
	}
	return nil
}

// Do runs one command and returns its reply. An error reply is returned as
// both the Value and an Error. A null reply is returned as a null Value with
// a nil error (typed helpers turn it into ErrNil).
func (c *Client) Do(ctx context.Context, args ...any) (Value, error) {
	cmd, err := Args(args...)
	if err != nil {
		return Value{}, err
	}
	cn, err := c.get(ctx)
	if err != nil {
		return Value{}, err
	}
	replies, err := cn.roundTrip(ctx, [][]string{cmd})
	c.put(cn, err)
	if err != nil {
		return Value{}, err
	}
	v := replies[0]
	if v.IsError() {
		return v, Error(v.Str)
	}
	return v, nil
}

// Args converts arguments to strings: string, []byte, integers, floats,
// bool (1/0), time.Duration (milliseconds) and fmt.Stringer are supported.
func Args(args ...any) ([]string, error) {
	if len(args) == 0 {
		return nil, errors.New("cache-boy: empty command")
	}
	out := make([]string, 0, len(args))
	for _, a := range args {
		switch v := a.(type) {
		case string:
			out = append(out, v)
		case []byte:
			out = append(out, string(v))
		case []string:
			out = append(out, v...)
		case int:
			out = append(out, strconv.Itoa(v))
		case int64:
			out = append(out, strconv.FormatInt(v, 10))
		case int32:
			out = append(out, strconv.FormatInt(int64(v), 10))
		case uint:
			out = append(out, strconv.FormatUint(uint64(v), 10))
		case uint64:
			out = append(out, strconv.FormatUint(v, 10))
		case uint32:
			out = append(out, strconv.FormatUint(uint64(v), 10))
		case float64:
			out = append(out, resp.FormatFloat(v))
		case float32:
			out = append(out, resp.FormatFloat(float64(v)))
		case bool:
			if v {
				out = append(out, "1")
			} else {
				out = append(out, "0")
			}
		case time.Duration:
			out = append(out, strconv.FormatInt(v.Milliseconds(), 10))
		case fmt.Stringer:
			out = append(out, v.String())
		case nil:
			return nil, errors.New("cache-boy: nil argument")
		default:
			out = append(out, fmt.Sprint(v))
		}
	}
	return out, nil
}

// Pipeline queues commands and sends them in one round-trip.
//
//	p := c.Pipeline()
//	p.Do("SET", "a", 1)
//	p.Do("INCR", "a")
//	replies, err := p.Exec(ctx)
type Pipeline struct {
	c    *Client
	cmds [][]string
	err  error
}

func (c *Client) Pipeline() *Pipeline { return &Pipeline{c: c} }

// Do queues a command.
func (p *Pipeline) Do(args ...any) *Pipeline {
	cmd, err := Args(args...)
	if err != nil && p.err == nil {
		p.err = err
	}
	if err == nil {
		p.cmds = append(p.cmds, cmd)
	}
	return p
}

// Len is the number of queued commands.
func (p *Pipeline) Len() int { return len(p.cmds) }

// Exec sends all queued commands and returns one reply per command, in
// order. Error replies are returned as error Values; err is only set for
// network or argument errors. The pipeline is empty afterwards.
func (p *Pipeline) Exec(ctx context.Context) ([]Value, error) {
	cmds, err := p.cmds, p.err
	p.cmds, p.err = nil, nil
	if err != nil {
		return nil, err
	}
	if len(cmds) == 0 {
		return nil, nil
	}
	cn, err := p.c.get(ctx)
	if err != nil {
		return nil, err
	}
	replies, err := cn.roundTrip(ctx, cmds)
	p.c.put(cn, err)
	return replies, err
}
