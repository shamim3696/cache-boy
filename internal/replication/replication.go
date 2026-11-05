// Package replication implements leader → follower replication in the style
// of Redis' SYNC protocol:
//
//  1. The follower connects to the leader, sends "REPLCONF listening-port
//     <port>" (so the leader can report it) and "SYNC".
//  2. The leader runs SYNC with every shard locked (the command has
//     FlagAllKeys), so in one atomic step it serialises a snapshot into
//     memory and starts buffering every write propagated after it. It then
//     hijacks the connection and sends
//     "+FULLRESYNC <runid> <offset>\r\n$<len>\r\n<snapshot bytes>"
//     followed by the write commands as RESP arrays, plus a PING every
//     PingInterval so the follower can detect a dead link.
//  3. The follower flushes its dataset, loads the snapshot, then executes
//     the stream as an internal "master" client (which may write although
//     the node is read-only), and reports its progress with
//     "REPLCONF ACK <offset>" every AckInterval.
//
// Offsets count bytes of the command stream, as in Redis: the leader's
// master_repl_offset grows by the RESP size of every command it sends, and a
// follower's offset is the leader's offset at FULLRESYNC plus the bytes it
// has applied since. WAIT compares acknowledged offsets with the leader's
// offset at the time WAIT was called.
//
// There is no partial resync (PSYNC continuation) and no backlog: a follower
// that disconnects does a full resync when it reconnects, and a follower that
// can't keep up (its output buffer passes MaxReplicaBuffer) is dropped and
// resyncs.
package replication

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/protocol/resp"
)

// Manager implements command.Replicator. It is both sides at once: it can
// serve replicas (leader side) and follow a leader (follower side), which
// also allows chains (A → B → C).
type Manager struct {
	e *command.Engine

	pingInterval     time.Duration
	ackInterval      time.Duration
	timeout          time.Duration
	maxReplicaBuffer int64

	// --- leader side (guarded by mu) ---
	mu        sync.Mutex
	offset    int64 // master_repl_offset
	replicas  map[*replica]struct{}
	nreplicas atomic.Int32
	ports     map[int64]int // client ID -> REPLCONF listening-port

	// --- follower side ---
	fmu       sync.Mutex // serialises ReplicaOf
	link      atomic.Pointer[link]
	fenced    atomic.Bool // refuse writes although not following (raft: no leader known)
	failovers atomic.Int64

	stop     chan struct{}
	stopOnce sync.Once
}

// Options tunes the manager. Zero values get the defaults.
type Options struct {
	PingInterval     time.Duration // leader → replicas keepalive PING (default 1s)
	AckInterval      time.Duration // follower → leader REPLCONF ACK period (default 1s)
	Timeout          time.Duration // a link silent this long is dead (default 60s)
	MaxReplicaBuffer int64         // bytes queued per replica before it is dropped (default 256MB)
}

// Setup creates the manager with default options; see SetupWith.
func Setup(e *command.Engine) *Manager { return SetupWith(e, Options{}) }

// SetupWith creates the manager, sets e.Repl and registers it as a
// propagator. If cfg.ReplicaOf is set ("host port"), it starts following
// that leader.
func SetupWith(e *command.Engine, o Options) *Manager {
	if o.PingInterval <= 0 {
		o.PingInterval = time.Second
	}
	if o.AckInterval <= 0 {
		o.AckInterval = time.Second
	}
	if o.Timeout <= 0 {
		o.Timeout = 60 * time.Second
	}
	if o.MaxReplicaBuffer <= 0 {
		o.MaxReplicaBuffer = 256 << 20
	}
	m := &Manager{
		e:                e,
		pingInterval:     o.PingInterval,
		ackInterval:      o.AckInterval,
		timeout:          o.Timeout,
		maxReplicaBuffer: o.MaxReplicaBuffer,
		replicas:         map[*replica]struct{}{},
		ports:            map[int64]int{},
		stop:             make(chan struct{}),
	}
	e.Repl = m
	e.AddPropagator(m)
	e.OnClientClose(func(c *command.Client) {
		m.mu.Lock()
		delete(m.ports, c.ID)
		m.mu.Unlock()
	})
	go m.pingLoop()
	if addr := e.Cfg.Get().ReplicaOf; addr != "" {
		if host, port, ok := splitHostPort(addr); ok {
			if err := m.ReplicaOf(net.JoinHostPort(host, port)); err != nil {
				logf("replication: replicaof %s: %v", addr, err)
			}
		} else {
			logf("replication: invalid replicaof %q (want \"host port\")", addr)
		}
	}
	return m
}

// Close stops following, disconnects replicas and stops background work.
func (m *Manager) Close() {
	m.stopOnce.Do(func() {
		close(m.stop)
		m.fmu.Lock()
		if l := m.link.Swap(nil); l != nil {
			l.close()
		}
		m.fmu.Unlock()
		m.dropAll()
		m.e.RemovePropagator(m)
	})
}

// splitHostPort accepts "host port" or "host:port".
func splitHostPort(s string) (host, port string, ok bool) {
	if h, p, err := net.SplitHostPort(s); err == nil {
		host, port = h, p
	} else {
		var found bool
		host, port, found = cut(s, ' ')
		if !found {
			return "", "", false
		}
	}
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 || host == "" {
		return "", "", false
	}
	return host, port, true
}

func cut(s string, sep byte) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

// IsReplica reports whether writes from clients must be refused: the node
// follows a leader, or failover fenced it while no leader is known.
func (m *Manager) IsReplica() bool { return m.link.Load() != nil || m.fenced.Load() }

// Offset is this node's replication position: the applied offset when
// following, otherwise master_repl_offset. Raft uses it to prefer the most
// up-to-date candidate.
func (m *Manager) Offset() int64 {
	if l := m.link.Load(); l != nil {
		return l.offset.Load()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.offset
}

// Role returns the ROLE reply.
func (m *Manager) Role() resp.Value {
	if l := m.link.Load(); l != nil {
		port, _ := strconv.Atoi(l.port)
		return resp.Array(resp.Bulk("slave"), resp.Bulk(l.host), resp.Int(int64(port)),
			resp.Bulk(l.State()), resp.Int(l.offset.Load()))
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	var reps []resp.Value
	for _, r := range m.sortedReplicas() {
		reps = append(reps, resp.Array(resp.Bulk(r.ip), resp.Bulk(strconv.Itoa(r.port)),
			resp.Bulk(strconv.FormatInt(max(r.ack.Load(), 0), 10))))
	}
	return resp.Array(resp.Bulk("master"), resp.Int(m.offset), resp.Array(reps...))
}

// Info returns the replication section of INFO (the "role" line is added by
// the INFO command itself).
func (m *Manager) Info() [][2]string {
	i64 := func(n int64) string { return strconv.FormatInt(n, 10) }
	var out [][2]string
	l := m.link.Load()
	if l != nil {
		status := "down"
		if l.State() == "connected" {
			status = "up"
		}
		syncing := "0"
		if l.State() == "sync" {
			syncing = "1"
		}
		lastIO := int64(-1)
		if t := l.lastIO.Load(); t > 0 {
			lastIO = (time.Now().UnixMilli() - t) / 1000
		}
		out = append(out,
			[2]string{"master_host", l.host},
			[2]string{"master_port", l.port},
			[2]string{"master_link_status", status},
			[2]string{"master_last_io_seconds_ago", i64(lastIO)},
			[2]string{"master_sync_in_progress", syncing},
			[2]string{"slave_repl_offset", i64(l.offset.Load())},
			[2]string{"slave_read_only", "1"},
			[2]string{"master_sync_count", i64(l.syncs.Load())},
		)
	}
	m.mu.Lock()
	reps := m.sortedReplicas()
	out = append(out, [2]string{"connected_slaves", strconv.Itoa(len(reps))})
	now := time.Now().UnixMilli()
	for i, r := range reps {
		lag := int64(-1)
		if t := r.ackTime.Load(); t > 0 {
			lag = (now - t) / 1000
		}
		out = append(out, [2]string{"slave" + strconv.Itoa(i),
			"ip=" + r.ip + ",port=" + strconv.Itoa(r.port) + ",state=" + r.State() +
				",offset=" + i64(max(r.ack.Load(), 0)) + ",lag=" + i64(lag)})
	}
	masterOffset := m.offset
	m.mu.Unlock()
	if l != nil {
		masterOffset = l.offset.Load()
	}
	out = append(out,
		[2]string{"master_failover_state", "no-failover"},
		[2]string{"master_replid", m.e.RunID},
		[2]string{"master_repl_offset", i64(masterOffset)},
	)
	if n := m.failovers.Load(); n > 0 {
		out = append(out, [2]string{"raft_failovers", i64(n)})
	}
	return out
}

func logf(format string, args ...any) { logger(format, args...) }
