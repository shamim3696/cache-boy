package client

import (
	"context"
	"strings"
	"sync"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Message is one push received by a Subscription.
type Message struct {
	Kind    string // "message", "pmessage", "keyevent", or another push kind
	Pattern string // for pmessage
	Channel string // channel, or the key for keyevent
	Payload string // message, or the operation (set, del, expired...) for keyevent
	Raw     Value
}

// Subscription receives pushes on its own connection. Read C until it is
// closed (after Close, ctx cancellation, or a connection error; see Err).
type Subscription struct {
	C <-chan Message

	cn     *conn
	cancel context.CancelFunc
	mu     sync.Mutex
	err    error
	done   chan struct{}
}

// Subscribe listens to channels on a dedicated connection.
func (c *Client) Subscribe(ctx context.Context, channels ...string) (*Subscription, error) {
	return c.subscribe(ctx, "SUBSCRIBE", channels)
}

// PSubscribe listens to channels matching glob patterns.
func (c *Client) PSubscribe(ctx context.Context, patterns ...string) (*Subscription, error) {
	return c.subscribe(ctx, "PSUBSCRIBE", patterns)
}

// KeyWatch streams changes of keys matching the patterns: each Message has
// Channel = key and Payload = operation ("set", "del", "expired"...).
func (c *Client) KeyWatch(ctx context.Context, patterns ...string) (*Subscription, error) {
	return c.subscribe(ctx, "KEYWATCH", patterns)
}

func (c *Client) subscribe(ctx context.Context, cmd string, names []string) (*Subscription, error) {
	if len(names) == 0 {
		return nil, Error("ERR " + cmd + " needs at least one channel or pattern")
	}
	if c.isClosed() {
		return nil, ErrClosed
	}
	cn, err := c.dial(ctx)
	if err != nil {
		return nil, err
	}
	stop := cn.watch(ctx)
	err = cn.w.WriteCommand(append([]string{cmd}, names...)...)
	if err == nil {
		err = cn.w.Flush()
	}
	// The first reply is either a confirmation or an error (e.g. unknown command).
	var first resp.Value
	if err == nil {
		first, err = cn.r.ReadValue()
	}
	if stop() && err != nil {
		err = ctx.Err()
	}
	if err == nil && first.IsError() {
		err = Error(first.Str)
	}
	if err != nil {
		cn.close()
		return nil, err
	}
	cn.nc.SetDeadline(timeZero)

	ch := make(chan Message, 256)
	sctx, cancel := context.WithCancel(ctx)
	s := &Subscription{C: ch, cn: cn, cancel: cancel, done: make(chan struct{})}
	go func() {
		<-sctx.Done()
		cn.close()
	}()
	go s.loop(sctx, ch, first)
	return s, nil
}

func (s *Subscription) loop(ctx context.Context, ch chan<- Message, first resp.Value) {
	defer close(s.done)
	defer close(ch)
	v := first
	for {
		if m, ok := parseMessage(v); ok {
			select {
			case ch <- m:
			case <-ctx.Done():
				return
			}
		}
		var err error
		v, err = s.cn.r.ReadValue()
		if err != nil {
			if ctx.Err() == nil {
				s.mu.Lock()
				s.err = err
				s.mu.Unlock()
			}
			return
		}
	}
}

// parseMessage turns a push into a Message; confirmations are skipped.
func parseMessage(v resp.Value) (Message, bool) {
	if len(v.Elems) < 2 || (v.Type != resp.TypePush && v.Type != resp.TypeArray) {
		return Message{}, false
	}
	str := func(i int) string {
		if i < len(v.Elems) {
			return v.Elems[i].String()
		}
		return ""
	}
	m := Message{Kind: strings.ToLower(str(0)), Raw: v}
	// Confirmations look like [subscribe, name, count].
	if len(v.Elems) == 3 && v.Elems[2].Type == resp.TypeInteger {
		return Message{}, false
	}
	switch m.Kind {
	case "message":
		m.Channel, m.Payload = str(1), str(2)
	case "pmessage":
		m.Pattern, m.Channel, m.Payload = str(1), str(2), str(3)
		if k, ok := strings.CutPrefix(m.Channel, "__key__:"); ok { // KEYWATCH over pub/sub
			m.Kind, m.Channel = "keyevent", k
		}
	case "keyevent":
		m.Channel, m.Payload = str(1), str(2)
	default:
		m.Channel = str(1)
		m.Payload = str(len(v.Elems) - 1)
	}
	return m, true
}

// Err returns the connection error that ended the subscription, if any.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close ends the subscription and closes its connection.
func (s *Subscription) Close() error {
	s.cancel()
	<-s.done
	return nil
}
