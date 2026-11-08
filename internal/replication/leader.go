package replication

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/persistence/snapshot"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

var logger = log.Printf

// replica is one connected follower, seen from the leader.
type replica struct {
	id   int64 // client ID
	ip   string
	port int // from REPLCONF listening-port

	ch      chan []byte  // encoded commands waiting to be sent
	pending atomic.Int64 // bytes in ch
	ack     atomic.Int64 // last REPLCONF ACK offset (-1 = none yet)
	ackTime atomic.Int64 // unix ms of the last ACK
	state   atomic.Pointer[string]

	done     chan struct{}
	doneOnce sync.Once
}

func (r *replica) State() string     { return *r.state.Load() }
func (r *replica) setState(s string) { r.state.Store(&s) }
func (r *replica) close()            { r.doneOnce.Do(func() { close(r.done) }) }

// replicaQueue is the channel capacity per replica; Options.MaxReplicaBuffer bounds
// the bytes.
const replicaQueue = 1 << 16

// Propagate implements command.Propagator: every write command is encoded
// once and queued for every replica. It never blocks: a replica whose queue
// is full is dropped (it will reconnect and resync).
func (m *Manager) Propagate(args []string) {
	if m.nreplicas.Load() == 0 && !m.backlogOn.Load() {
		return
	}
	m.feed(resp.AppendCommand(nil, args))
}

// feed appends an encoded command to the replication stream.
func (m *Manager) feed(b []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.replicas) == 0 && m.backlog == nil {
		return
	}
	m.offset += int64(len(b))
	if m.backlog != nil {
		m.backlog.append(b)
	}
	for r := range m.replicas {
		if r.pending.Load()+int64(len(b)) > m.maxReplicaBuffer {
			logger("replication: replica %s:%d can't keep up (%d bytes queued), dropping it", r.ip, r.port, r.pending.Load())
			m.removeLocked(r)
			continue
		}
		select {
		case r.ch <- b:
			r.pending.Add(int64(len(b)))
		default:
			logger("replication: replica %s:%d can't keep up (queue full), dropping it", r.ip, r.port)
			m.removeLocked(r)
		}
	}
}

func (m *Manager) removeLocked(r *replica) {
	if _, ok := m.replicas[r]; ok {
		delete(m.replicas, r)
		m.nreplicas.Add(-1)
	}
	r.close()
}

func (m *Manager) remove(r *replica) {
	m.mu.Lock()
	m.removeLocked(r)
	m.mu.Unlock()
}

// dropAll disconnects every replica (they reconnect and resync). Used when
// this node's dataset is replaced by a full sync from its own leader.
func (m *Manager) dropAll() {
	m.mu.Lock()
	for r := range m.replicas {
		m.removeLocked(r)
	}
	m.mu.Unlock()
}

// sortedReplicas requires mu.
func (m *Manager) sortedReplicas() []*replica {
	out := make([]*replica, 0, len(m.replicas))
	for r := range m.replicas {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// pingLoop sends PING through the stream so followers can tell a quiet
// leader from a dead link.
func (m *Manager) pingLoop() {
	t := time.NewTicker(m.pingInterval)
	defer t.Stop()
	ping := resp.AppendCommand(nil, []string{"PING"})
	for {
		select {
		case <-m.stop:
			return
		case <-m.e.Stopping():
			return
		case <-t.C:
			if m.nreplicas.Load() > 0 {
				m.feed(ping)
			}
		}
	}
}

// Sync handles SYNC/PSYNC. The dispatcher holds every shard lock (the
// command has FlagAllKeys), so encoding the snapshot and registering the
// replica happen atomically with respect to writes: every write is either in
// the snapshot or in the replica's queue, never both, never neither.
func (m *Manager) Sync(c *command.Client, args []string) resp.Value {
	if c.Kind != "tcp" {
		return resp.Err("ERR SYNC needs a TCP connection")
	}
	// PSYNC <replid> <offset>: continue from the backlog if we still have
	// every byte the replica is missing.
	if len(args) == 3 && strings.EqualFold(args[0], "psync") && args[1] == m.e.RunID {
		if from, err := strconv.ParseInt(args[2], 10, 64); err == nil {
			r := m.newReplica(c)
			m.mu.Lock()
			var tail []byte
			ok := m.backlog != nil
			if ok {
				tail, ok = m.backlog.since(from, m.offset)
			}
			if ok {
				m.attachLocked(r, c.ID)
			}
			m.mu.Unlock()
			if ok {
				m.syncPartialOK.Add(1)
				head := append([]byte("+CONTINUE "+m.e.RunID+"\r\n"), tail...)
				c.Hijack = func(conn net.Conn, rd *resp.Reader) {
					m.serveReplica(r, conn, rd, head, fmt.Sprintf("partial resync from offset %d (%d bytes)", from, len(tail)))
				}
				return resp.None()
			}
			m.syncPartialErr.Add(1)
		}
	}

	m.syncFull.Add(1)
	snap, err := snapshot.AppendDB(nil, m.e.DB, m.e.DumpModules)
	if err != nil {
		return resp.Errf("ERR snapshot failed: %v", err)
	}
	r := m.newReplica(c)
	m.mu.Lock()
	if m.backlog == nil {
		m.backlog = newBacklog(m.backlogSize)
		m.backlogOn.Store(true)
	}
	offset := m.offset
	m.attachLocked(r, c.ID)
	m.mu.Unlock()

	head := append([]byte(fmt.Sprintf("+FULLRESYNC %s %d\r\n$%d\r\n", m.e.RunID, offset, len(snap))), snap...)
	c.Hijack = func(conn net.Conn, rd *resp.Reader) {
		m.serveReplica(r, conn, rd, head, fmt.Sprintf("full resync: %d byte snapshot at offset %d", len(snap), offset))
	}
	return resp.None()
}

func (m *Manager) newReplica(c *command.Client) *replica {
	r := &replica{id: c.ID, ch: make(chan []byte, replicaQueue), done: make(chan struct{})}
	r.ack.Store(-1)
	r.setState("wait_bgsave")
	return r
}

// attachLocked starts queueing the stream for r. Requires mu.
func (m *Manager) attachLocked(r *replica, clientID int64) {
	r.port = m.ports[clientID]
	delete(m.ports, clientID)
	m.replicas[r] = struct{}{}
	m.nreplicas.Add(1)
}

// serveReplica owns a hijacked replica connection: it sends head (the
// FULLRESYNC header + snapshot, or CONTINUE + backlog tail), then streams
// queued commands; a second goroutine reads REPLCONF ACKs.
func (m *Manager) serveReplica(r *replica, conn net.Conn, rd *resp.Reader, head []byte, what string) {
	defer conn.Close()
	defer m.remove(r)
	if host, p, err := net.SplitHostPort(conn.RemoteAddr().String()); err == nil {
		m.mu.Lock() // Info and Role read ip/port under mu
		r.ip = host
		if r.port == 0 { // no REPLCONF listening-port: report the source port
			r.port, _ = strconv.Atoi(p)
		}
		m.mu.Unlock()
	}
	logger("replication: replica %s:%d connected, %s", r.ip, r.port, what)

	r.setState("send_bulk")
	w := bufio.NewWriterSize(conn, 64<<10)
	conn.SetWriteDeadline(time.Now().Add(m.timeout))
	w.Write(head)
	if err := w.Flush(); err != nil {
		logger("replication: sending sync data to %s:%d: %v", r.ip, r.port, err)
		return
	}
	head = nil
	r.setState("online")

	go func() { // ACK reader
		defer r.close()
		for {
			conn.SetReadDeadline(time.Now().Add(m.timeout))
			args, err := rd.ReadCommand()
			if err != nil {
				return
			}
			if len(args) >= 3 && strings.EqualFold(args[0], "replconf") && strings.EqualFold(args[1], "ack") {
				if n, err := strconv.ParseInt(args[2], 10, 64); err == nil {
					r.ack.Store(n)
					r.ackTime.Store(time.Now().UnixMilli())
				}
			}
		}
	}()

	write := func(b []byte) bool {
		r.pending.Add(-int64(len(b)))
		_, err := w.Write(b)
		return err == nil
	}
	for {
		select {
		case b := <-r.ch:
			conn.SetWriteDeadline(time.Now().Add(m.timeout))
			ok := write(b)
			for ok && len(r.ch) > 0 {
				ok = write(<-r.ch)
			}
			if !ok || w.Flush() != nil {
				logger("replication: lost replica %s:%d", r.ip, r.port)
				return
			}
		case <-r.done:
			logger("replication: replica %s:%d disconnected", r.ip, r.port)
			return
		case <-m.stop:
			return
		}
	}
}

// ReplConf handles REPLCONF from a replica before SYNC (listening-port,
// capa). ACKs arrive on the hijacked connection and never get here.
func (m *Manager) ReplConf(c *command.Client, args []string) resp.Value {
	for i := 1; i+1 < len(args); i += 2 {
		switch strings.ToLower(args[i]) {
		case "listening-port":
			p, err := strconv.Atoi(args[i+1])
			if err != nil || p < 0 || p > 65535 {
				return resp.Err("ERR invalid listening-port")
			}
			m.mu.Lock()
			m.ports[c.ID] = p
			m.mu.Unlock()
		case "ack", "getack":
			return resp.None() // no reply, as in Redis
		case "capa", "ip-address", "rdb-only", "rdb-filter-only":
		default:
			return resp.Errf("ERR Unrecognized REPLCONF option: %s", args[i])
		}
	}
	return resp.OK
}

// Wait blocks until n replicas acknowledged every write made before the
// call, or timeout passes (0 = no timeout), and returns how many did.
func (m *Manager) Wait(n int, timeout time.Duration) int {
	m.mu.Lock()
	target := m.offset
	m.mu.Unlock()
	count := func() int {
		m.mu.Lock()
		defer m.mu.Unlock()
		k := 0
		for r := range m.replicas {
			if r.ack.Load() >= target {
				k++
			}
		}
		return k
	}
	got := count()
	if got >= n || timeout < 0 {
		return got
	}
	// Ask for an immediate ACK instead of waiting for the next periodic one.
	getack := resp.AppendCommand(nil, []string{"REPLCONF", "GETACK", "*"})
	m.feed(getack)

	var deadline <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		deadline = t.C
	}
	poll := time.NewTicker(2 * time.Millisecond)
	defer poll.Stop()
	resend := time.NewTicker(time.Second)
	defer resend.Stop()
	for {
		select {
		case <-poll.C:
			if got = count(); got >= n {
				return got
			}
		case <-resend.C:
			m.feed(getack)
		case <-deadline:
			return count()
		case <-m.e.Stopping():
			return count()
		}
	}
}
