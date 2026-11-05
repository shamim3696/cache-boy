package client

import (
	"context"
	"net"
	"time"

	"cache-boy/internal/protocol/resp"
)

var timeZero time.Time

// conn is one connection. It is used by one goroutine at a time.
type conn struct {
	nc     net.Conn
	r      *resp.Reader
	w      *resp.Writer
	broken bool
}

func newConn(nc net.Conn, proto int) *conn {
	return &conn{nc: nc, r: resp.NewReader(nc), w: resp.NewWriter(nc, proto)}
}

func (cn *conn) close() { cn.nc.Close() }

// roundTrip writes commands and reads one reply per command. The context's
// deadline and cancellation apply; on any network error the conn is broken.
func (cn *conn) roundTrip(ctx context.Context, cmds [][]string) ([]resp.Value, error) {
	stop := cn.watch(ctx)
	replies, err := cn.do(cmds)
	if stop() && err != nil {
		err = ctx.Err()
	}
	if err != nil {
		cn.broken = true
	}
	return replies, err
}

func (cn *conn) do(cmds [][]string) ([]resp.Value, error) {
	for _, c := range cmds {
		if err := cn.w.WriteCommand(c...); err != nil {
			return nil, err
		}
	}
	if err := cn.w.Flush(); err != nil {
		return nil, err
	}
	out := make([]resp.Value, len(cmds))
	for i := range out {
		v, err := cn.readReply()
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// readReply skips RESP3 push messages (e.g. invalidations) that arrive
// between replies on a normal connection.
func (cn *conn) readReply() (resp.Value, error) {
	for {
		v, err := cn.r.ReadValue()
		if err != nil {
			return v, err
		}
		if v.Type != resp.TypePush {
			return v, nil
		}
	}
}

// watch applies ctx to the connection. The returned stop function reports
// whether the context ended the operation.
func (cn *conn) watch(ctx context.Context) (stop func() bool) {
	if dl, ok := ctx.Deadline(); ok {
		cn.nc.SetDeadline(dl)
	} else {
		cn.nc.SetDeadline(time.Time{})
	}
	if ctx.Done() == nil {
		return func() bool { return false }
	}
	done := make(chan struct{})
	fired := make(chan bool, 1)
	go func() {
		select {
		case <-ctx.Done():
			cn.nc.SetDeadline(time.Unix(1, 0)) // unblock reads/writes now
			fired <- true
		case <-done:
			fired <- false
		}
	}()
	return func() bool {
		close(done)
		return <-fired
	}
}
