// Package raft implements Raft leader election (the Raft paper, §5.2) to pick
// which cache-boy node is the replication leader. There is no replicated log:
// data flows through the replication package, and Raft only answers "who is
// the leader now?" so the others can follow it.
//
// Pieces of the protocol that are implemented:
//
//   - Terms. Every message carries the sender's term; seeing a higher term
//     makes a node adopt it and become a follower.
//   - Randomised election timeouts. A follower that hears no heartbeat for
//     a random duration in [ElectionTimeoutMin, ElectionTimeoutMax) becomes
//     a candidate, increments the term, votes for itself and sends
//     RequestVote to every peer. Randomisation makes split votes rare.
//   - One vote per term (votedFor), granted first-come first-served, and
//     only to candidates whose Position is at least ours. This is the
//     "up-to-date log" check of §5.4.1 with the replication offset standing
//     in for the log index, so the most up-to-date replica wins.
//   - A candidate with votes from a majority becomes leader and sends
//     AppendEntries heartbeats (with no entries) every HeartbeatInterval.
//   - Check quorum: a leader that hasn't heard from a majority for an
//     election timeout steps down, so a partitioned old leader stops
//     accepting writes instead of causing a split brain forever.
//
// Term and vote are kept in StateFile (if set) so a restarted node cannot
// vote twice in the same term.
//
// The transport is one JSON object per line over TCP: each RPC dials (or
// reuses) a connection to the peer, writes the request and reads one reply.
package raft

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"time"
)

// State of a node.
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}

// Config configures a node.
type Config struct {
	ID    string            // unique node ID
	Addr  string            // listen address for Raft traffic ("host:port")
	Peers map[string]string // other nodes: ID -> Raft address

	// Listener, if set, is used instead of listening on Addr (tests use
	// it to bind port 0 before the peers' addresses are known).
	Listener net.Listener

	// Timeouts. Zero values get the defaults below. HeartbeatInterval must
	// be well below ElectionTimeoutMin.
	ElectionTimeoutMin time.Duration // default 1s
	ElectionTimeoutMax time.Duration // default 2s
	HeartbeatInterval  time.Duration // default 200ms
	RPCTimeout         time.Duration // default HeartbeatInterval

	// Position, if set, reports how up to date this node's data is (the
	// replication offset). Votes are refused to candidates that are behind.
	Position func() int64

	// OnLeaderChange is called (from a dedicated goroutine, never with
	// internal locks held, in order) whenever the known leader changes.
	// leaderID is "" while no leader is known.
	OnLeaderChange func(leaderID string)

	// StateFile persists the current term and vote ("" = memory only).
	StateFile string

	Logf func(format string, args ...any) // default log.Printf
}

// Node is one Raft participant.
type Node struct {
	cfg Config
	ln  net.Listener

	mu       sync.Mutex
	state    State
	term     uint64
	votedFor string
	leader   string
	deadline time.Time            // election deadline (follower/candidate)
	lastAck  map[string]time.Time // leader: last successful reply per peer
	votes    int

	notify  chan struct{} // leader changed
	stop    chan struct{}
	stopped sync.Once
	wg      sync.WaitGroup

	connsMu sync.Mutex
	conns   map[string]*peerConn
}

type peerConn struct {
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

// message is the wire format for both RPCs and their replies.
type message struct {
	Type     string `json:"type"` // "vote", "append"
	Term     uint64 `json:"term"`
	From     string `json:"from"`
	Position int64  `json:"pos,omitempty"`
	Granted  bool   `json:"granted,omitempty"` // vote reply
	Success  bool   `json:"success,omitempty"` // append reply
	LeaderID string `json:"leader,omitempty"`
}

// New creates a node; call Start to run it.
func New(cfg Config) (*Node, error) {
	if cfg.ID == "" {
		return nil, errors.New("raft: node ID is required")
	}
	if cfg.ElectionTimeoutMin <= 0 {
		cfg.ElectionTimeoutMin = time.Second
	}
	if cfg.ElectionTimeoutMax <= cfg.ElectionTimeoutMin {
		cfg.ElectionTimeoutMax = 2 * cfg.ElectionTimeoutMin
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = cfg.ElectionTimeoutMin / 5
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = cfg.HeartbeatInterval
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	n := &Node{
		cfg:     cfg,
		lastAck: map[string]time.Time{},
		notify:  make(chan struct{}, 1),
		stop:    make(chan struct{}),
		conns:   map[string]*peerConn{},
	}
	if err := n.loadState(); err != nil {
		return nil, err
	}
	return n, nil
}

// Start listens for peers and starts the election timer.
func (n *Node) Start() error {
	ln := n.cfg.Listener
	if ln == nil {
		var err error
		if ln, err = net.Listen("tcp", n.cfg.Addr); err != nil {
			return fmt.Errorf("raft: %w", err)
		}
	}
	n.ln = ln
	n.mu.Lock()
	n.resetDeadline()
	n.mu.Unlock()
	n.wg.Add(3)
	go n.acceptLoop()
	go n.tickLoop()
	go n.notifyLoop()
	return nil
}

// Stop shuts the node down (it stops answering and sending RPCs).
func (n *Node) Stop() {
	n.stopped.Do(func() {
		close(n.stop)
		if n.ln != nil {
			n.ln.Close()
		}
		n.connsMu.Lock()
		for _, pc := range n.conns {
			pc.mu.Lock()
			if pc.conn != nil {
				pc.conn.Close()
			}
			pc.mu.Unlock()
		}
		n.connsMu.Unlock()
		n.wg.Wait()
	})
}

// ID returns this node's ID.
func (n *Node) ID() string { return n.cfg.ID }

// Addr returns the address the node listens on.
func (n *Node) Addr() string {
	if n.ln != nil {
		return n.ln.Addr().String()
	}
	return n.cfg.Addr
}

// State returns the current role.
func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

// Term returns the current term.
func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

// Leader returns the ID of the known leader ("" if none).
func (n *Node) Leader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leader
}

// Status returns id, state, term and leader in one consistent read.
func (n *Node) Status() (id, state string, term uint64, leader string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cfg.ID, n.state.String(), n.term, n.leader
}

func (n *Node) isStopped() bool {
	select {
	case <-n.stop:
		return true
	default:
		return false
	}
}

// --- timers ---

func (n *Node) randomTimeout() time.Duration {
	span := n.cfg.ElectionTimeoutMax - n.cfg.ElectionTimeoutMin
	return n.cfg.ElectionTimeoutMin + time.Duration(rand.Int64N(int64(span)))
}

// resetDeadline requires mu.
func (n *Node) resetDeadline() { n.deadline = time.Now().Add(n.randomTimeout()) }

func (n *Node) tickLoop() {
	defer n.wg.Done()
	tick := time.NewTicker(n.cfg.HeartbeatInterval / 2)
	defer tick.Stop()
	lastBeat := time.Time{}
	for {
		select {
		case <-n.stop:
			return
		case now := <-tick.C:
			n.mu.Lock()
			switch n.state {
			case Leader:
				if n.lostQuorum(now) {
					n.cfg.Logf("raft %s: lost contact with a majority, stepping down (term %d)", n.cfg.ID, n.term)
					n.becomeFollower(n.term, "")
					n.mu.Unlock()
					continue
				}
				due := now.Sub(lastBeat) >= n.cfg.HeartbeatInterval
				term := n.term
				n.mu.Unlock()
				if due {
					lastBeat = now
					n.broadcastHeartbeat(term)
				}
			default:
				if now.After(n.deadline) {
					n.startElectionLocked()
				}
				n.mu.Unlock()
			}
		}
	}
}

// lostQuorum requires mu.
func (n *Node) lostQuorum(now time.Time) bool {
	acks := 1 // ourselves
	for id := range n.cfg.Peers {
		if now.Sub(n.lastAck[id]) < n.cfg.ElectionTimeoutMax {
			acks++
		}
	}
	return acks <= n.clusterSize()/2
}

func (n *Node) clusterSize() int { return len(n.cfg.Peers) + 1 }

// --- state transitions (all require mu) ---

func (n *Node) setLeader(id string) {
	if n.leader != id {
		n.leader = id
		select {
		case n.notify <- struct{}{}:
		default:
		}
	}
}

func (n *Node) becomeFollower(term uint64, leader string) {
	if term > n.term {
		n.term, n.votedFor = term, ""
		n.saveState()
	}
	n.state = Follower
	n.setLeader(leader)
	n.resetDeadline()
}

func (n *Node) startElectionLocked() {
	n.state = Candidate
	n.term++
	n.votedFor = n.cfg.ID
	n.votes = 1
	n.saveState()
	n.setLeader("")
	n.resetDeadline()
	term := n.term
	n.cfg.Logf("raft %s: election timeout, starting election for term %d", n.cfg.ID, term)
	if n.votes > n.clusterSize()/2 { // single-node cluster
		n.becomeLeader()
		return
	}
	req := message{Type: "vote", Term: term, From: n.cfg.ID, Position: n.position()}
	for id, addr := range n.cfg.Peers {
		go n.requestVote(id, addr, req)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	now := time.Now()
	for id := range n.cfg.Peers {
		n.lastAck[id] = now // grace period before the quorum check applies
	}
	n.cfg.Logf("raft %s: became leader for term %d", n.cfg.ID, n.term)
	n.setLeader(n.cfg.ID)
	term := n.term
	go n.broadcastHeartbeat(term)
}

func (n *Node) position() int64 {
	if n.cfg.Position == nil {
		return 0
	}
	return n.cfg.Position()
}

// --- outgoing RPCs ---

func (n *Node) requestVote(id, addr string, req message) {
	reply, err := n.call(id, addr, req)
	if err != nil {
		return
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.isStopped() {
		return
	}
	if reply.Term > n.term {
		n.becomeFollower(reply.Term, "")
		return
	}
	if n.state != Candidate || n.term != req.Term || !reply.Granted {
		return
	}
	n.votes++
	if n.votes > n.clusterSize()/2 {
		n.becomeLeader()
	}
}

func (n *Node) broadcastHeartbeat(term uint64) {
	req := message{Type: "append", Term: term, From: n.cfg.ID, LeaderID: n.cfg.ID}
	for id, addr := range n.cfg.Peers {
		go func() {
			reply, err := n.call(id, addr, req)
			if err != nil {
				return
			}
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.isStopped() {
				return
			}
			if reply.Term > n.term {
				n.cfg.Logf("raft %s: saw higher term %d from %s, stepping down", n.cfg.ID, reply.Term, id)
				n.becomeFollower(reply.Term, "")
				return
			}
			if n.state == Leader && n.term == term {
				n.lastAck[id] = time.Now()
			}
		}()
	}
}

// call sends one RPC to a peer over a cached connection.
func (n *Node) call(id, addr string, req message) (message, error) {
	select {
	case <-n.stop:
		return message{}, errors.New("stopped")
	default:
	}
	n.connsMu.Lock()
	pc := n.conns[id]
	if pc == nil {
		pc = &peerConn{}
		n.conns[id] = pc
	}
	n.connsMu.Unlock()

	pc.mu.Lock()
	defer pc.mu.Unlock()
	if pc.conn == nil {
		c, err := net.DialTimeout("tcp", addr, n.cfg.RPCTimeout)
		if err != nil {
			return message{}, err
		}
		pc.conn, pc.r = c, bufio.NewReader(c)
	}
	pc.conn.SetDeadline(time.Now().Add(n.cfg.RPCTimeout))
	b, _ := json.Marshal(req)
	var reply message
	_, err := pc.conn.Write(append(b, '\n'))
	if err == nil {
		var line []byte
		if line, err = pc.r.ReadBytes('\n'); err == nil {
			err = json.Unmarshal(line, &reply)
		}
	}
	if err != nil {
		pc.conn.Close()
		pc.conn = nil
		return message{}, err
	}
	return reply, nil
}

// --- incoming RPCs ---

func (n *Node) acceptLoop() {
	defer n.wg.Done()
	for {
		c, err := n.ln.Accept()
		if err != nil {
			select {
			case <-n.stop:
				return
			default:
			}
			time.Sleep(10 * time.Millisecond)
			continue
		}
		n.wg.Add(1)
		go n.serveConn(c)
	}
}

func (n *Node) serveConn(c net.Conn) {
	defer n.wg.Done()
	defer c.Close()
	go func() { // unblock the read when stopping
		<-n.stop
		c.Close()
	}()
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadBytes('\n')
		if err != nil {
			return
		}
		var req message
		if json.Unmarshal(line, &req) != nil {
			return
		}
		select {
		case <-n.stop:
			return
		default:
		}
		reply := n.handle(req)
		b, _ := json.Marshal(reply)
		c.SetWriteDeadline(time.Now().Add(n.cfg.RPCTimeout))
		if _, err := c.Write(append(b, '\n')); err != nil {
			return
		}
	}
}

func (n *Node) handle(req message) message {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term > n.term {
		n.becomeFollower(req.Term, "")
	}
	reply := message{Type: req.Type, Term: n.term, From: n.cfg.ID}
	if req.Term < n.term {
		return reply // stale sender: refuse; our term in the reply updates it
	}
	switch req.Type {
	case "vote":
		if (n.votedFor == "" || n.votedFor == req.From) && req.Position >= n.position() {
			n.votedFor = req.From
			n.saveState()
			n.resetDeadline() // granting a vote defers our own candidacy
			reply.Granted = true
		}
	case "append":
		// A valid heartbeat for our term: the sender is the leader.
		if n.state != Follower || n.leader != req.LeaderID {
			n.cfg.Logf("raft %s: following leader %s (term %d)", n.cfg.ID, req.LeaderID, req.Term)
		}
		n.state = Follower
		n.setLeader(req.LeaderID)
		n.resetDeadline()
		reply.Success = true
	}
	return reply
}

// notifyLoop delivers leader changes to OnLeaderChange, in order, outside
// the node's lock. Rapid changes may be coalesced into the latest one.
func (n *Node) notifyLoop() {
	defer n.wg.Done()
	last := ""
	for {
		select {
		case <-n.stop:
			return
		case <-n.notify:
		}
		cur := n.Leader()
		if cur != last {
			last = cur
			if n.cfg.OnLeaderChange != nil {
				n.cfg.OnLeaderChange(cur)
			}
		}
	}
}

// --- persistent state ---

type persisted struct {
	Term     uint64 `json:"term"`
	VotedFor string `json:"voted_for"`
}

func (n *Node) loadState() error {
	if n.cfg.StateFile == "" {
		return nil
	}
	b, err := os.ReadFile(n.cfg.StateFile)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("raft: %w", err)
	}
	var p persisted
	if err := json.Unmarshal(b, &p); err != nil {
		return fmt.Errorf("raft: %s: %w", n.cfg.StateFile, err)
	}
	n.term, n.votedFor = p.Term, p.VotedFor
	return nil
}

// saveState requires mu. It writes a temp file and renames it so a crash
// never leaves a half-written state file.
func (n *Node) saveState() {
	if n.cfg.StateFile == "" {
		return
	}
	b, _ := json.Marshal(persisted{n.term, n.votedFor})
	tmp := n.cfg.StateFile + ".tmp"
	f, err := os.Create(tmp)
	if err == nil {
		_, err = f.Write(b)
		if err == nil {
			err = f.Sync()
		}
		f.Close()
		if err == nil {
			err = os.Rename(tmp, n.cfg.StateFile)
		}
	}
	if err != nil {
		n.cfg.Logf("raft %s: saving state: %v", n.cfg.ID, err)
	}
}
