package raft

import (
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type cluster struct {
	nodes   []*Node
	mu      sync.Mutex
	changes map[string][]string // node -> leaders seen by OnLeaderChange
}

func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	c := &cluster{changes: map[string][]string{}}
	lns := make([]net.Listener, n)
	addrs := map[string]string{}
	for i := range lns {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		lns[i] = ln
		addrs[fmt.Sprintf("n%d", i)] = ln.Addr().String()
	}
	dir := t.TempDir()
	for i := range n {
		id := fmt.Sprintf("n%d", i)
		peers := map[string]string{}
		for pid, a := range addrs {
			if pid != id {
				peers[pid] = a
			}
		}
		node, err := New(Config{
			ID: id, Peers: peers, Listener: lns[i],
			ElectionTimeoutMin: 150 * time.Millisecond,
			ElectionTimeoutMax: 300 * time.Millisecond,
			HeartbeatInterval:  30 * time.Millisecond,
			RPCTimeout:         50 * time.Millisecond,
			StateFile:          filepath.Join(dir, id+".json"),
			Logf:               t.Logf,
			OnLeaderChange: func(leader string) {
				c.mu.Lock()
				c.changes[id] = append(c.changes[id], leader)
				c.mu.Unlock()
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := node.Start(); err != nil {
			t.Fatal(err)
		}
		c.nodes = append(c.nodes, node)
	}
	t.Cleanup(func() {
		for _, n := range c.nodes {
			n.Stop()
		}
	})
	return c
}

// stableLeader waits until exactly one of the running nodes is leader and
// every running node agrees on it.
func (c *cluster) stableLeader(t *testing.T, running []*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		var leaders []*Node
		for _, n := range running {
			if n.State() == Leader {
				leaders = append(leaders, n)
			}
		}
		if len(leaders) == 1 {
			agree := true
			for _, n := range running {
				if n.Leader() != leaders[0].ID() || n.Term() != leaders[0].Term() {
					agree = false
				}
			}
			if agree {
				return leaders[0]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no stable leader elected")
	return nil
}

func TestElectionAndFailover(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.stableLeader(t, c.nodes)
	term := leader.Term()

	// Stays stable for a few election timeouts (heartbeats work).
	time.Sleep(600 * time.Millisecond)
	if leader.State() != Leader || leader.Term() != term {
		t.Fatalf("leadership not stable: state %v term %d -> %d", leader.State(), term, leader.Term())
	}

	leader.Stop()
	var rest []*Node
	for _, n := range c.nodes {
		if n != leader {
			rest = append(rest, n)
		}
	}
	next := c.stableLeader(t, rest)
	if next == leader || next.Term() <= term {
		t.Fatalf("re-election: leader %s term %d (old %s term %d)", next.ID(), next.Term(), leader.ID(), term)
	}

	// Callbacks reported the new leader on the surviving nodes.
	time.Sleep(50 * time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range rest {
		seen := c.changes[n.ID()]
		if len(seen) == 0 || seen[len(seen)-1] != next.ID() {
			t.Fatalf("%s saw leader changes %v, want last %s", n.ID(), seen, next.ID())
		}
	}
}

func TestUpToDateCandidateWins(t *testing.T) {
	// Node votes are refused to candidates with a lower Position.
	n, err := New(Config{ID: "a", Position: func() int64 { return 100 }})
	if err != nil {
		t.Fatal(err)
	}
	n.resetDeadline()
	if r := n.handle(message{Type: "vote", Term: 1, From: "b", Position: 50}); r.Granted {
		t.Fatal("vote granted to a stale candidate")
	}
	if r := n.handle(message{Type: "vote", Term: 1, From: "c", Position: 100}); !r.Granted {
		t.Fatal("vote refused to an up-to-date candidate")
	}
	if r := n.handle(message{Type: "vote", Term: 1, From: "d", Position: 200}); r.Granted {
		t.Fatal("voted twice in one term")
	}
	if r := n.handle(message{Type: "append", Term: 0, From: "x", LeaderID: "x"}); r.Success || r.Term != 1 {
		t.Fatal("stale heartbeat accepted")
	}
}

func TestStatePersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	n, _ := New(Config{ID: "a", StateFile: path})
	n.mu.Lock()
	n.term, n.votedFor = 7, "b"
	n.saveState()
	n.mu.Unlock()
	m, err := New(Config{ID: "a", StateFile: path})
	if err != nil {
		t.Fatal(err)
	}
	if m.term != 7 || m.votedFor != "b" {
		t.Fatalf("restored term %d vote %q", m.term, m.votedFor)
	}
}
