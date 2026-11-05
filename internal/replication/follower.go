package replication

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cache-boy/internal/persistence/snapshot"
	"cache-boy/internal/protocol/resp"
)

// link is the follower's connection to its leader. It survives reconnects:
// one link per ReplicaOf call, closed by the next ReplicaOf.
type link struct {
	addr, host, port string

	state  atomic.Pointer[string] // connect, connecting, sync, connected
	offset atomic.Int64           // applied replication offset
	lastIO atomic.Int64           // unix ms of the last byte from the leader
	syncs  atomic.Int64           // completed full syncs

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once

	connMu sync.Mutex
	conn   net.Conn
}

func (l *link) State() string     { return *l.state.Load() }
func (l *link) setState(s string) { l.state.Store(&s) }

func (l *link) stopped() bool {
	select {
	case <-l.stop:
		return true
	default:
		return false
	}
}

// close stops the link goroutine and waits for it (bounded, so a caller that
// holds locks the link needs can't deadlock forever).
func (l *link) close() {
	l.stopOnce.Do(func() {
		close(l.stop)
		l.connMu.Lock()
		if l.conn != nil {
			l.conn.Close()
		}
		l.connMu.Unlock()
	})
	select {
	case <-l.done:
	case <-time.After(5 * time.Second):
		logf("replication: link to %s did not stop in time", l.addr)
	}
}

// setConn publishes the connection so close can interrupt blocking reads.
// It returns false if the link was stopped meanwhile.
func (l *link) setConn(c net.Conn) bool {
	l.connMu.Lock()
	defer l.connMu.Unlock()
	if l.stopped() {
		return false
	}
	l.conn = c
	return true
}

// ReplicaOf makes this node follow the leader at addr ("host:port"), or
// become a leader when addr is "" (the dataset is kept). Following the
// leader we already follow is a no-op.
func (m *Manager) ReplicaOf(addr string) error {
	m.fmu.Lock()
	defer m.fmu.Unlock()
	select {
	case <-m.stop:
		return errors.New("replication is shut down")
	default:
	}

	if addr == "" {
		m.fenced.Store(false)
		old := m.link.Swap(nil)
		if old == nil {
			return nil
		}
		old.close()
		// Continue the offset sequence where the old leader's stream ended,
		// so offsets stay comparable between nodes after a failover.
		m.mu.Lock()
		m.offset = max(m.offset, old.offset.Load())
		m.mu.Unlock()
		logf("replication: stopped following %s; this node is now a leader", old.addr)
		return nil
	}

	host, port, ok := splitHostPort(addr)
	if !ok {
		return fmt.Errorf("invalid leader address %q", addr)
	}
	addr = net.JoinHostPort(host, port)
	if cur := m.link.Load(); cur != nil && cur.addr == addr {
		return nil
	}
	if old := m.link.Swap(nil); old != nil {
		old.close()
	}
	l := &link{addr: addr, host: host, port: port, stop: make(chan struct{}), done: make(chan struct{})}
	l.setState("connect")
	l.offset.Store(-1)
	m.link.Store(l)
	m.fenced.Store(false)
	logf("replication: following %s", addr)
	go m.follow(l)
	return nil
}

// follow keeps the link up: sync, stream, and on failure reconnect with
// exponential backoff (100ms .. 5s).
func (m *Manager) follow(l *link) {
	defer close(l.done)
	backoff := 100 * time.Millisecond
	for !l.stopped() {
		synced, err := m.syncOnce(l)
		if l.stopped() {
			return
		}
		l.setState("connect")
		if synced {
			backoff = 100 * time.Millisecond
		}
		logf("replication: link to %s: %v; retrying in %v", l.addr, err, backoff)
		select {
		case <-time.After(backoff):
		case <-l.stop:
			return
		}
		backoff = min(backoff*2, 5*time.Second)
	}
}

// deadlineConn extends the read deadline before every read, so a transfer
// that keeps making progress never times out but a silent link does.
type deadlineConn struct {
	net.Conn
	d time.Duration
}

func (c deadlineConn) Read(p []byte) (int, error) {
	c.Conn.SetReadDeadline(time.Now().Add(c.d))
	return c.Conn.Read(p)
}

// syncOnce runs one connection: handshake, full sync, then the command
// stream until an error. synced reports whether the full sync completed.
func (m *Manager) syncOnce(l *link) (synced bool, err error) {
	l.setState("connecting")
	conn, err := net.DialTimeout("tcp", l.addr, 5*time.Second)
	if err != nil {
		return false, err
	}
	defer conn.Close()
	if !l.setConn(conn) {
		return false, errors.New("stopped")
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
		tc.SetKeepAlive(true)
	}
	br := bufio.NewReaderSize(deadlineConn{conn, m.timeout}, 64<<10)

	var wmu sync.Mutex
	send := func(args ...string) error {
		wmu.Lock()
		defer wmu.Unlock()
		conn.SetWriteDeadline(time.Now().Add(m.timeout))
		_, err := conn.Write(resp.AppendCommand(nil, args))
		return err
	}
	readLine := func() (string, error) {
		line, err := br.ReadString('\n')
		if err != nil {
			return "", err
		}
		return strings.TrimRight(line, "\r\n"), nil
	}

	// Handshake.
	if cfg := m.e.Cfg.Get(); cfg.MasterAuth != "" {
		auth := []string{"AUTH", cfg.MasterAuth}
		if cfg.MasterUser != "" {
			auth = []string{"AUTH", cfg.MasterUser, cfg.MasterAuth}
		}
		if err := send(auth...); err != nil {
			return false, err
		}
		line, err := readLine()
		if err != nil {
			return false, err
		}
		if strings.HasPrefix(line, "-") {
			return false, fmt.Errorf("leader refused AUTH: %s", line[1:])
		}
	}
	if err := send("REPLCONF", "listening-port", strconv.Itoa(m.e.Cfg.Get().Port)); err != nil {
		return false, err
	}
	line, err := readLine()
	if err != nil {
		return false, err
	}
	if strings.HasPrefix(line, "-") {
		return false, fmt.Errorf("leader refused REPLCONF: %s", line[1:])
	}
	if err := send("SYNC"); err != nil {
		return false, err
	}
	l.setState("sync")
	if line, err = readLine(); err != nil {
		return false, err
	}
	f := strings.Fields(line)
	if len(f) != 3 || f[0] != "+FULLRESYNC" {
		return false, fmt.Errorf("unexpected SYNC reply %q", line)
	}
	offset, err := strconv.ParseInt(f[2], 10, 64)
	if err != nil {
		return false, fmt.Errorf("bad FULLRESYNC offset %q", f[2])
	}
	if line, err = readLine(); err != nil {
		return false, err
	}
	size, err := strconv.ParseInt(strings.TrimPrefix(line, "$"), 10, 64)
	if !strings.HasPrefix(line, "$") || err != nil || size < 0 {
		return false, fmt.Errorf("bad snapshot header %q", line)
	}

	// Full sync: decode (verifying the checksum) before touching our data.
	lr := &io.LimitedReader{R: br, N: size}
	data, err := snapshot.Decode(lr, int(m.e.DB.CompressThreshold.Load()))
	if err != nil {
		return false, fmt.Errorf("receiving snapshot: %w", err)
	}
	if _, err := io.Copy(io.Discard, lr); err != nil {
		return false, err
	}
	if l.stopped() {
		return false, errors.New("stopped")
	}
	if err := data.Apply(m.e, true); err != nil {
		logf("replication: %v", err)
	}
	m.dropAll() // our own replicas must resync from the new dataset
	l.offset.Store(offset)
	l.lastIO.Store(time.Now().UnixMilli())
	l.syncs.Add(1)
	l.setState("connected")
	logf("replication: full sync from %s done (%d keys, %d bytes, offset %d)", l.addr, len(data.Keys), size, offset)
	data = nil
	if m.e.Persist != nil {
		m.e.Persist.BgRewriteAOF() // the AOF must reflect the new dataset; error = AOF off
	}

	sendAck := func() error { return send("REPLCONF", "ACK", strconv.FormatInt(l.offset.Load(), 10)) }
	if err := sendAck(); err != nil {
		return true, err
	}
	ackDone := make(chan struct{})
	defer close(ackDone)
	go func() {
		t := time.NewTicker(m.ackInterval)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				if sendAck() != nil {
					conn.Close()
					return
				}
			case <-ackDone:
				return
			}
		}
	}()

	// Command stream.
	rr := resp.NewReader(br)
	client := m.e.InternalClient("master", true)
	errorsLogged := 0
	for {
		args, err := rr.ReadCommand()
		if err != nil {
			return true, err
		}
		n := respLen(args)
		l.lastIO.Store(time.Now().UnixMilli())
		switch {
		case strings.EqualFold(args[0], "ping"):
			l.offset.Add(n)
		case strings.EqualFold(args[0], "replconf") && len(args) > 1 && strings.EqualFold(args[1], "getack"):
			l.offset.Add(n)
			if err := sendAck(); err != nil {
				return true, err
			}
		default:
			v := m.e.Execute(client, args)
			l.offset.Add(n)
			if v.IsError() && errorsLogged < 10 {
				errorsLogged++
				logf("replication: command %q from leader failed: %s", args[0], v.Str)
			}
		}
	}
}

// respLen is the size of args encoded as a RESP array of bulk strings
// (what resp.AppendCommand produces), without allocating.
func respLen(args []string) int64 {
	n := int64(1 + digits(len(args)) + 2)
	for _, a := range args {
		n += int64(1+digits(len(a))+2) + int64(len(a)) + 2
	}
	return n
}

func digits(n int) int {
	d := 1
	for n >= 10 {
		n /= 10
		d++
	}
	return d
}
