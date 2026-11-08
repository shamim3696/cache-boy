package command_test

import (
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/replication"
	"github.com/shamim3696/cache-boy/internal/server"
)

var fastRepl = replication.Options{
	PingInterval: 50 * time.Millisecond,
	AckInterval:  100 * time.Millisecond,
	Timeout:      5 * time.Second,
}

// startLeader returns a leader engine served on 127.0.0.1:<random>.
func startLeader(t *testing.T) (*command.Engine, *replication.Manager, string) {
	t.Helper()
	e := infraEngine(t, nil)
	m := replication.SetupWith(e, fastRepl)
	srv := server.New(e)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() {
		m.Close()
		srv.Close()
	})
	return e, m, srv.Addr().String()
}

func newFollower(t *testing.T) (*command.Engine, *replication.Manager) {
	t.Helper()
	e := infraEngine(t, nil)
	m := replication.SetupWith(e, fastRepl)
	t.Cleanup(m.Close)
	return e, m
}

func getVal(e *command.Engine, c *command.Client, key string) string {
	return e.Execute(c, []string{"infratget", key}).Str
}

func TestReplicationEndToEnd(t *testing.T) {
	leader, _, addr := startLeader(t)
	lc := leader.NewClient("tcp", "test")
	mustOK(t, leader, lc, "infratset", "before", "snapshot")

	follower, _ := newFollower(t)
	fc := follower.NewClient("tcp", "test")
	mustOK(t, follower, fc, "infratset", "stale", "local data") // replaced by the full sync

	host, port, _ := net.SplitHostPort(addr)
	mustOK(t, follower, fc, "REPLICAOF", host, port)
	eventually(t, "full sync", func() bool { return getVal(follower, fc, "before") == "snapshot" })
	if getVal(follower, fc, "stale") != "" {
		t.Fatal("follower kept its old data after a full sync")
	}

	// Writes stream to the follower.
	mustOK(t, leader, lc, "infratset", "k", "v1")
	mustOK(t, leader, lc, "infratset", "k", "v2")
	eventually(t, "streamed write", func() bool { return getVal(follower, fc, "k") == "v2" })

	// The follower is read-only for clients.
	if v := exec(follower, fc, "infratset", "x", "y"); !v.IsError() || !strings.HasPrefix(v.Str, "READONLY") {
		t.Fatalf("write on follower: %v", v)
	}

	// WAIT counts the follower once it acknowledged everything.
	mustOK(t, leader, lc, "infratset", "k", "v3")
	if v := mustOK(t, leader, lc, "WAIT", "1", "5000"); v.Int != 1 {
		t.Fatalf("WAIT 1: %v", v)
	}
	if v := mustOK(t, leader, lc, "WAIT", "2", "100"); v.Int != 1 {
		t.Fatalf("WAIT 2 (timeout): %v", v)
	}
	if getVal(follower, fc, "k") != "v3" {
		t.Fatal("WAIT returned before the follower applied the write")
	}

	// ROLE and INFO on both sides.
	role := mustOK(t, leader, lc, "ROLE")
	if role.Elems[0].Str != "master" || len(role.Elems[2].Elems) != 1 {
		t.Fatalf("leader ROLE: %v", role)
	}
	frole := mustOK(t, follower, fc, "ROLE")
	if frole.Elems[0].Str != "slave" || frole.Elems[3].Str != "connected" || frole.Elems[4].Int != role.Elems[1].Int {
		t.Fatalf("follower ROLE: %v (leader %v)", frole, role)
	}
	if infoField(leader, "replication", "connected_slaves") != "1" ||
		!strings.Contains(infoField(leader, "replication", "slave0"), "state=online") ||
		infoField(follower, "replication", "role") != "slave" ||
		infoField(follower, "replication", "master_link_status") != "up" ||
		infoField(follower, "replication", "master_port") != port {
		t.Fatalf("INFO replication: leader %v follower %v", leader.InfoSections(), follower.InfoSections())
	}

	// REPLICAOF NO ONE: keeps the data, accepts writes, stops receiving.
	mustOK(t, follower, fc, "REPLICAOF", "NO", "ONE")
	mustOK(t, follower, fc, "infratset", "own", "write")
	mustOK(t, leader, lc, "infratset", "k", "v4")
	time.Sleep(100 * time.Millisecond)
	if getVal(follower, fc, "k") != "v3" {
		t.Fatal("follower still applies the leader's writes after REPLICAOF NO ONE")
	}
	if v := mustOK(t, follower, fc, "ROLE"); v.Elems[0].Str != "master" {
		t.Fatalf("ROLE after NO ONE: %v", v)
	}
	eventually(t, "leader drops the replica", func() bool {
		return infoField(leader, "replication", "connected_slaves") == "0"
	})

	// Following again resyncs from scratch (SLAVEOF is an alias).
	mustOK(t, follower, fc, "SLAVEOF", host, port)
	eventually(t, "resync", func() bool { return getVal(follower, fc, "k") == "v4" })
	if getVal(follower, fc, "own") != "" {
		t.Fatal("resync kept local writes")
	}
	if v := exec(follower, fc, "REPLICAOF", host, "notaport"); !v.IsError() {
		t.Fatal("bad port accepted")
	}
}

func TestReplicationReconnect(t *testing.T) {
	leader, _, addr := startLeader(t)
	lc := leader.NewClient("tcp", "test")
	follower, frepl := newFollower(t)
	fc := follower.NewClient("tcp", "test")
	if err := frepl.ReplicaOf(addr); err != nil {
		t.Fatal(err)
	}
	mustOK(t, leader, lc, "infratset", "a", "1")
	eventually(t, "first write", func() bool { return getVal(follower, fc, "a") == "1" })

	// Kill the replica link from the leader side: the follower notices,
	// reconnects with backoff and continues from the backlog (PSYNC).
	syncs := infoField(follower, "replication", "master_sync_count")
	for _, cl := range leader.Clients() {
		if cl.LastCommand() == "sync" {
			cl.Close()
		}
	}
	eventually(t, "reconnect", func() bool {
		return infoField(leader, "replication", "sync_partial_ok") == "1" &&
			infoField(follower, "replication", "master_link_status") == "up"
	})
	if infoField(follower, "replication", "master_sync_count") != syncs {
		t.Fatal("reconnect did a full sync instead of a partial one")
	}
	mustOK(t, leader, lc, "infratset", "b", "2")
	eventually(t, "resync after link loss", func() bool { return getVal(follower, fc, "b") == "2" })
	mustOK(t, leader, lc, "infratset", "c", "3")
	eventually(t, "stream after resync", func() bool { return getVal(follower, fc, "c") == "3" })
}

func TestFailoverCallback(t *testing.T) {
	leader, _, addr := startLeader(t)
	lc := leader.NewClient("tcp", "test")
	mustOK(t, leader, lc, "infratset", "x", "1")

	follower, frepl := newFollower(t)
	fc := follower.NewClient("tcp", "test")
	onChange := frepl.OnLeaderChange("f", map[string]string{"l": addr})

	onChange("l") // another node won the election: follow it
	eventually(t, "follow elected leader", func() bool { return getVal(follower, fc, "x") == "1" })
	onChange("") // leader unknown: keep following
	if !frepl.IsReplica() {
		t.Fatal("stopped following while no leader is known")
	}
	onChange("f") // we won: become writable leader
	if frepl.IsReplica() {
		t.Fatal("still a replica after winning the election")
	}
	mustOK(t, follower, fc, "infratset", "y", "2")
	onChange("") // lost quorum as leader: fence writes
	if v := exec(follower, fc, "infratset", "z", "3"); !v.IsError() {
		t.Fatal("fenced node accepted a write")
	}
	if v := exec(follower, fc, "RAFT.STATUS"); !v.IsError() {
		t.Fatal("RAFT.STATUS without raft should fail")
	}
}

func TestReplicationNotConfigured(t *testing.T) {
	e := infraEngine(t, nil)
	c := e.NewClient("tcp", "test")
	if v := exec(e, c, "REPLICAOF", "NO", "ONE"); v.Str != "ERR replication is not configured" {
		t.Fatalf("REPLICAOF: %v", v)
	}
	if v := mustOK(t, e, c, "ROLE"); v.Elems[0].Str != "master" {
		t.Fatalf("ROLE: %v", v)
	}
	if v := mustOK(t, e, c, "WAIT", "1", "10"); v.Int != 0 {
		t.Fatalf("WAIT: %v", v)
	}
}

func TestReplicationMasterAuth(t *testing.T) {
	leader, _, addr := startLeader(t)
	if err := leader.SetConfig("requirepass", "s3cret"); err != nil {
		t.Fatal(err)
	}
	lc := leader.NewClient("tcp", "test")
	mustOK(t, leader, lc, "AUTH", "s3cret")
	mustOK(t, leader, lc, "infratset", "k", "protected")

	follower, _ := newFollower(t)
	fc := follower.NewClient("tcp", "test")
	host, port, _ := net.SplitHostPort(addr)

	// Wrong password: the link never comes up.
	mustOK(t, follower, fc, "CONFIG", "SET", "masterauth", "wrong")
	mustOK(t, follower, fc, "REPLICAOF", host, port)
	time.Sleep(300 * time.Millisecond)
	if getVal(follower, fc, "k") != "" {
		t.Fatal("synced with a wrong masterauth")
	}

	// Right password: full sync works.
	mustOK(t, follower, fc, "REPLICAOF", "NO", "ONE")
	mustOK(t, follower, fc, "CONFIG", "SET", "masterauth", "s3cret")
	mustOK(t, follower, fc, "REPLICAOF", host, port)
	eventually(t, "authenticated sync", func() bool { return getVal(follower, fc, "k") == "protected" })
}

func TestReplicationPartialResync(t *testing.T) {
	leader, _, addr := startLeader(t)
	lc := leader.NewClient("tcp", "test")
	mustOK(t, leader, lc, "infratset", "k", "v0")

	follower, _ := newFollower(t)
	fc := follower.NewClient("tcp", "test")
	host, port, _ := net.SplitHostPort(addr)
	mustOK(t, follower, fc, "REPLICAOF", host, port)
	eventually(t, "full sync", func() bool { return getVal(follower, fc, "k") == "v0" })

	// Cut the replication link from the leader side, then write while the
	// replica is away.
	for _, c := range leader.Clients() {
		if cmd := c.LastCommand(); cmd == "sync" || cmd == "psync" {
			c.Close()
		}
	}
	for i := 1; i <= 50; i++ {
		mustOK(t, leader, lc, "infratset", "k", "v"+strconv.Itoa(i))
	}
	eventually(t, "catch up", func() bool { return getVal(follower, fc, "k") == "v50" })
	if got := infoField(leader, "replication", "sync_partial_ok"); got != "1" {
		t.Fatalf("sync_partial_ok = %s, want 1", got)
	}
	if got := infoField(leader, "replication", "sync_full"); got != "1" {
		t.Fatalf("sync_full = %s, want 1 (reconnect must not resend the snapshot)", got)
	}
	mustOK(t, leader, lc, "infratset", "after", "reconnect")
	eventually(t, "stream after partial resync", func() bool { return getVal(follower, fc, "after") == "reconnect" })
}

func TestReplicationBacklogOverflowFallsBackToFullSync(t *testing.T) {
	e := infraEngine(t, nil)
	m := replication.SetupWith(e, replication.Options{
		PingInterval: 50 * time.Millisecond, AckInterval: 100 * time.Millisecond,
		Timeout: 5 * time.Second, BacklogSize: 256,
	})
	srv := server.New(e)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	t.Cleanup(func() { m.Close(); srv.Close() })
	lc := e.NewClient("tcp", "test")

	follower, _ := newFollower(t)
	fc := follower.NewClient("tcp", "test")
	host, port, _ := net.SplitHostPort(srv.Addr().String())
	mustOK(t, follower, fc, "REPLICAOF", host, port)
	mustOK(t, e, lc, "infratset", "k", "start")
	eventually(t, "sync", func() bool { return getVal(follower, fc, "k") == "start" })

	for _, c := range e.Clients() {
		if cmd := c.LastCommand(); cmd == "sync" || cmd == "psync" {
			c.Close()
		}
	}
	// Far more than the 256-byte backlog while the replica is away.
	for i := 0; i < 100; i++ {
		mustOK(t, e, lc, "infratset", "k", strings.Repeat("x", 50)+strconv.Itoa(i))
	}
	eventually(t, "resync", func() bool { return strings.HasSuffix(getVal(follower, fc, "k"), "x99") })
	if got := infoField(e, "replication", "sync_full"); got != "2" {
		t.Fatalf("sync_full = %s, want 2 (backlog was too small)", got)
	}
}
