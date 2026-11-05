package cluster

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"cache-boy/internal/command"
	"cache-boy/internal/feature/namespace"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

// Node is one cluster member from the configuration.
type Node struct {
	ID     string
	Host   string
	Port   int
	Ranges [][2]int // inclusive slot ranges, sorted
}

// Addr is "host:port".
func (n *Node) Addr() string { return net.JoinHostPort(n.Host, strconv.Itoa(n.Port)) }

// Router implements command.ClusterRouter with a static slot map.
type Router struct {
	enabled bool
	self    *Node
	nodes   []*Node         // sorted by first slot
	owner   [NumSlots]*Node // slot -> node (nil = unassigned)
	db      *store.DB
}

// ParseNode parses "id host:port 0-8191[,10000-10100] [12000]".
// Ranges may be separated by commas or spaces; a single number is one slot.
func ParseNode(line string) (*Node, error) {
	f := strings.Fields(line)
	if len(f) < 2 {
		return nil, fmt.Errorf("cluster-node %q: want \"id host:port slots\"", line)
	}
	host, portStr, err := net.SplitHostPort(f[1])
	if err != nil {
		return nil, fmt.Errorf("cluster-node %q: %v", line, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, fmt.Errorf("cluster-node %q: bad port", line)
	}
	n := &Node{ID: f[0], Host: host, Port: port}
	for _, spec := range f[2:] {
		for _, r := range strings.Split(spec, ",") {
			if r == "" {
				continue
			}
			lo, hi, isRange := strings.Cut(r, "-")
			a, err1 := strconv.Atoi(lo)
			b := a
			var err2 error
			if isRange {
				b, err2 = strconv.Atoi(hi)
			}
			if err1 != nil || err2 != nil || a < 0 || b >= NumSlots || a > b {
				return nil, fmt.Errorf("cluster-node %q: bad slot range %q", line, r)
			}
			n.Ranges = append(n.Ranges, [2]int{a, b})
		}
	}
	sort.Slice(n.Ranges, func(i, j int) bool { return n.Ranges[i][0] < n.Ranges[j][0] })
	return n, nil
}

// New builds a router. With enabled false it only answers CLUSTER KEYSLOT
// style questions; Check never redirects.
func New(enabled bool, selfID string, nodeLines []string, db *store.DB) (*Router, error) {
	r := &Router{enabled: enabled, db: db}
	if !enabled {
		return r, nil
	}
	if selfID == "" {
		return nil, errors.New("cluster-enabled requires cluster-node-id")
	}
	seen := map[string]bool{}
	for _, line := range nodeLines {
		n, err := ParseNode(line)
		if err != nil {
			return nil, err
		}
		if seen[n.ID] {
			return nil, fmt.Errorf("cluster-node id %q listed twice", n.ID)
		}
		seen[n.ID] = true
		for _, rg := range n.Ranges {
			for s := rg[0]; s <= rg[1]; s++ {
				if r.owner[s] != nil {
					return nil, fmt.Errorf("slot %d is assigned to both %s and %s", s, r.owner[s].ID, n.ID)
				}
				r.owner[s] = n
			}
		}
		if n.ID == selfID {
			r.self = n
		}
		r.nodes = append(r.nodes, n)
	}
	if r.self == nil {
		return nil, fmt.Errorf("cluster-node-id %q is not in the cluster-node list", selfID)
	}
	sort.Slice(r.nodes, func(i, j int) bool { return firstSlot(r.nodes[i]) < firstSlot(r.nodes[j]) })
	return r, nil
}

func firstSlot(n *Node) int {
	if len(n.Ranges) == 0 {
		return NumSlots
	}
	return n.Ranges[0][0]
}

// Setup creates the router from the config and sets e.Cluster.
func Setup(e *command.Engine) (*Router, error) {
	cfg := e.Cfg.Get()
	r, err := New(cfg.ClusterEnabled, cfg.ClusterNodeID, cfg.ClusterNodes, e.DB)
	if err != nil {
		return nil, err
	}
	e.Cluster = r
	return r, nil
}

// Enabled reports whether cluster mode is on.
func (r *Router) Enabled() bool { return r.enabled }

// Self returns this node (nil when disabled).
func (r *Router) Self() *Node { return r.self }

// slotOf hashes a stored key. Namespace prefixes are removed first so the
// slot matches what a client computes for the key it sent.
func slotOf(stored string) int {
	if ns := namespace.Of(stored); ns != "" {
		stored = stored[len(namespace.Prefix(ns)):]
	}
	return Slot(stored)
}

// Check returns an error if keys don't all hash to one slot (CROSSSLOT),
// or that slot is served by another node (MOVED) or nobody (CLUSTERDOWN).
func (r *Router) Check(keys []string) error {
	if !r.enabled || len(keys) == 0 {
		return nil
	}
	slot := slotOf(keys[0])
	for _, k := range keys[1:] {
		if slotOf(k) != slot {
			return errors.New("CROSSSLOT Keys in request don't hash to the same slot")
		}
	}
	switch n := r.owner[slot]; n {
	case r.self:
		return nil
	case nil:
		return errors.New("CLUSTERDOWN Hash slot not served")
	default:
		return fmt.Errorf("MOVED %d %s", slot, n.Addr())
	}
}

// Info returns the cluster section of INFO.
func (r *Router) Info() [][2]string {
	return [][2]string{
		{"cluster_state", r.state()},
		{"cluster_slots_assigned", strconv.Itoa(r.assigned())},
		{"cluster_known_nodes", strconv.Itoa(len(r.nodes))},
	}
}

func (r *Router) assigned() int {
	n := 0
	for _, o := range r.owner {
		if o != nil {
			n++
		}
	}
	return n
}

func (r *Router) state() string {
	if r.assigned() == NumSlots {
		return "ok"
	}
	return "fail"
}

// Command runs a CLUSTER subcommand (args excludes "CLUSTER"). The CLUSTER
// command is registered with FlagAllKeys, so the caller holds every shard
// lock (needed by COUNTKEYSINSLOT and GETKEYSINSLOT).
func (r *Router) Command(args []string) resp.Value {
	if len(args) == 0 {
		return resp.Err("ERR wrong number of arguments for 'cluster' command")
	}
	sub := strings.ToLower(args[0])
	if sub == "keyslot" { // pure function: useful even without cluster mode
		if len(args) != 2 {
			return resp.Err("ERR wrong number of arguments for 'cluster|keyslot' command")
		}
		return resp.Int(int64(Slot(args[1])))
	}
	if !r.enabled {
		return resp.Err("ERR This instance has cluster support disabled")
	}
	switch sub {
	case "myid":
		return resp.Bulk(r.self.ID)
	case "info":
		var b strings.Builder
		assigned := r.assigned()
		for _, kv := range [][2]string{
			{"cluster_enabled", "1"},
			{"cluster_state", r.state()},
			{"cluster_slots_assigned", strconv.Itoa(assigned)},
			{"cluster_slots_ok", strconv.Itoa(assigned)},
			{"cluster_slots_pfail", "0"},
			{"cluster_slots_fail", "0"},
			{"cluster_known_nodes", strconv.Itoa(len(r.nodes))},
			{"cluster_size", strconv.Itoa(r.size())},
			{"cluster_current_epoch", strconv.Itoa(len(r.nodes))},
			{"cluster_my_epoch", strconv.Itoa(r.epoch(r.self))},
		} {
			b.WriteString(kv[0] + ":" + kv[1] + "\r\n")
		}
		return resp.Bulk(b.String())
	case "slots":
		var out []resp.Value
		for _, n := range r.nodes {
			for _, rg := range n.Ranges {
				out = append(out, resp.Array(resp.Int(int64(rg[0])), resp.Int(int64(rg[1])),
					resp.Array(resp.Bulk(n.Host), resp.Int(int64(n.Port)), resp.Bulk(n.ID))))
			}
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Elems[0].Int < out[j].Elems[0].Int })
		return resp.Array(out...)
	case "shards":
		var out []resp.Value
		for _, n := range r.nodes {
			var slots []resp.Value
			for _, rg := range n.Ranges {
				slots = append(slots, resp.Int(int64(rg[0])), resp.Int(int64(rg[1])))
			}
			node := resp.Map(
				resp.Bulk("id"), resp.Bulk(n.ID),
				resp.Bulk("port"), resp.Int(int64(n.Port)),
				resp.Bulk("ip"), resp.Bulk(n.Host),
				resp.Bulk("endpoint"), resp.Bulk(n.Host),
				resp.Bulk("role"), resp.Bulk("master"),
				resp.Bulk("replication-offset"), resp.Int(0),
				resp.Bulk("health"), resp.Bulk("online"),
			)
			out = append(out, resp.Map(resp.Bulk("slots"), resp.Array(slots...), resp.Bulk("nodes"), resp.Array(node)))
		}
		return resp.Array(out...)
	case "nodes":
		var b strings.Builder
		for _, n := range r.nodes {
			flags := "master"
			if n == r.self {
				flags = "myself,master"
			}
			fmt.Fprintf(&b, "%s %s@%d %s - 0 0 %d connected", n.ID, n.Addr(), n.Port+10000, flags, r.epoch(n))
			for _, rg := range n.Ranges {
				if rg[0] == rg[1] {
					fmt.Fprintf(&b, " %d", rg[0])
				} else {
					fmt.Fprintf(&b, " %d-%d", rg[0], rg[1])
				}
			}
			b.WriteByte('\n')
		}
		return resp.Bulk(b.String())
	case "countkeysinslot", "getkeysinslot":
		want := 2
		if sub == "getkeysinslot" {
			want = 3
		}
		if len(args) != want {
			return resp.Errf("ERR wrong number of arguments for 'cluster|%s' command", sub)
		}
		slot, err := strconv.Atoi(args[1])
		if err != nil || slot < 0 || slot >= NumSlots {
			return resp.Err("ERR Invalid slot")
		}
		limit := -1
		if sub == "getkeysinslot" {
			if limit, err = strconv.Atoi(args[2]); err != nil || limit < 0 {
				return resp.Err("ERR Invalid number of keys")
			}
		}
		var keys []string
		count := 0
		r.db.ForEach(func(k string, _ *store.Entry) bool {
			if slotOf(k) == slot {
				count++
				if limit >= 0 {
					keys = append(keys, k)
					if len(keys) >= limit {
						return false
					}
				}
			}
			return true
		})
		if sub == "countkeysinslot" {
			return resp.Int(int64(count))
		}
		return resp.Strings(keys)
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try CLUSTER HELP.", args[0])
}

// size is the number of nodes serving at least one slot.
func (r *Router) size() int {
	n := 0
	for _, x := range r.nodes {
		if len(x.Ranges) > 0 {
			n++
		}
	}
	return n
}

// epoch gives each node a stable config epoch (its position in the list).
func (r *Router) epoch(n *Node) int {
	for i, x := range r.nodes {
		if x == n {
			return i + 1
		}
	}
	return 0
}
