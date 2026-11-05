package replication

import (
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/consensus/raft"
)

// Automatic failover: Raft elects one leader among the nodes; the elected
// node stops following (ReplicaOf("")), every other node follows the
// elected node's data address. Raft is only used for the election; data is
// replicated by this package.

// Peer is one entry of cfg.RaftPeers: "id raftHost:raftPort dataHost:dataPort".
type Peer struct {
	ID       string
	RaftAddr string
	DataAddr string
}

// ParsePeer parses a raft-peer line.
func ParsePeer(line string) (Peer, error) {
	f := strings.Fields(line)
	if len(f) != 3 {
		return Peer{}, fmt.Errorf("raft-peer %q: want \"id raftHost:raftPort dataHost:dataPort\"", line)
	}
	for _, a := range f[1:] {
		if _, _, err := net.SplitHostPort(a); err != nil {
			return Peer{}, fmt.Errorf("raft-peer %q: %v", line, err)
		}
	}
	return Peer{ID: f[0], RaftAddr: f[1], DataAddr: f[2]}, nil
}

// OnLeaderChange returns the callback that wires Raft leadership into
// replication:
//
//   - this node elected: become a leader (stop following, accept writes);
//   - another node elected: follow its data address;
//   - no leader known: if we are not following anyone (we were the leader,
//     or never followed), fence writes until a leader is elected, so a node
//     cut off from the majority stops accepting writes.
func (m *Manager) OnLeaderChange(selfID string, dataAddrs map[string]string) func(leaderID string) {
	return func(leaderID string) {
		switch {
		case leaderID == selfID:
			m.failovers.Add(1)
			logf("replication: raft elected this node (%s) as leader", selfID)
			if err := m.ReplicaOf(""); err != nil {
				logf("replication: failover: %v", err)
			}
		case leaderID != "":
			addr, ok := dataAddrs[leaderID]
			if !ok {
				logf("replication: raft leader %q has no known data address", leaderID)
				return
			}
			if err := m.ReplicaOf(addr); err != nil {
				logf("replication: following raft leader %s at %s: %v", leaderID, addr, err)
			}
		default:
			if m.link.Load() == nil {
				m.fenced.Store(true)
			}
		}
	}
}

// RaftOptions tunes SetupRaft (zero values = raft package defaults).
type RaftOptions struct {
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
	HeartbeatInterval  time.Duration
}

// SetupRaft starts Raft leader election when cfg.RaftPort is set, and wires
// it into m (see OnLeaderChange). It returns nil, nil when Raft is disabled.
// The node ID is cfg.ClusterNodeID; cfg.RaftPeers lists every node (this
// one may be included; it is skipped). Until a leader is elected this node
// refuses writes. Term and vote are persisted in <dir>/raft-<id>.json.
// It also makes RAFT.STATUS available. Stop the returned node at shutdown.
func SetupRaft(e *command.Engine, m *Manager, o RaftOptions) (*raft.Node, error) {
	cfg := e.Cfg.Get()
	if cfg.RaftPort == 0 {
		return nil, nil
	}
	if cfg.ClusterNodeID == "" {
		return nil, fmt.Errorf("raft needs cluster-node-id to name this node")
	}
	peers := map[string]string{}
	data := map[string]string{}
	for _, line := range cfg.RaftPeers {
		p, err := ParsePeer(line)
		if err != nil {
			return nil, err
		}
		data[p.ID] = p.DataAddr
		if p.ID != cfg.ClusterNodeID {
			peers[p.ID] = p.RaftAddr
		}
	}
	m.fenced.Store(true) // no writes until the cluster has a leader
	node, err := raft.New(raft.Config{
		ID:                 cfg.ClusterNodeID,
		Addr:               net.JoinHostPort(cfg.Bind, strconv.Itoa(cfg.RaftPort)),
		Peers:              peers,
		ElectionTimeoutMin: o.ElectionTimeoutMin,
		ElectionTimeoutMax: o.ElectionTimeoutMax,
		HeartbeatInterval:  o.HeartbeatInterval,
		Position:           m.Offset,
		OnLeaderChange:     m.OnLeaderChange(cfg.ClusterNodeID, data),
		StateFile:          filepath.Join(cfg.Dir, "raft-"+cfg.ClusterNodeID+".json"),
	})
	if err != nil {
		return nil, err
	}
	if err := node.Start(); err != nil {
		return nil, err
	}
	command.SetRaft(e, node)
	return node, nil
}
