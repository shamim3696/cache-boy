package command

import (
	"time"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Interfaces for subsystems that depend on the Engine (they import this
// package), so the Engine only knows them through these interfaces.

// Propagator receives every write command after it runs (AOF, replicas).
type Propagator interface {
	Propagate(args []string)
}

// Dumper is a feature module with state outside the keyspace (queues, tags...).
// Dump emits commands that recreate that state; used by snapshots, AOF
// rewrite and replica full sync.
type Dumper interface {
	Dump(emit func(args []string))
}

// Persister is implemented by the persistence package.
type Persister interface {
	Save() error
	BgSave() error
	BgRewriteAOF() error
	LastSave() time.Time
	Info() [][2]string
}

// Replicator is implemented by the replication package.
type Replicator interface {
	IsReplica() bool
	// ReplicaOf starts following a leader at "host:port"; "" makes this node a leader.
	ReplicaOf(addr string) error
	Role() resp.Value
	// Sync handles SYNC/PSYNC from a replica. It usually hijacks the connection.
	Sync(c *Client, args []string) resp.Value
	ReplConf(c *Client, args []string) resp.Value
	// Wait blocks until n replicas acknowledged all writes so far, or timeout.
	Wait(n int, timeout time.Duration) int
	Info() [][2]string
}

// ClusterRouter is implemented by the cluster package.
type ClusterRouter interface {
	Enabled() bool
	// Check returns a MOVED or CROSSSLOT error if keys are not served here.
	Check(keys []string) error
	// Command runs a CLUSTER subcommand (args excludes "CLUSTER").
	Command(args []string) resp.Value
	Info() [][2]string
}
