package command

import (
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// Cluster commands. Routing (MOVED/CROSSSLOT) happens in the dispatcher via
// e.Cluster.Check; CLUSTER subcommands are answered by e.Cluster.Command.

func init() {
	Register(
		// FlagAllKeys: COUNTKEYSINSLOT/GETKEYSINSLOT scan the keyspace.
		&Spec{Name: "cluster", Arity: -2, Flags: FlagAllKeys, Group: "cluster", Summary: "CLUSTER KEYSLOT key | INFO | SLOTS | SHARDS | NODES | MYID | COUNTKEYSINSLOT slot | GETKEYSINSLOT slot count", Handler: clusterCmd},
		&Spec{Name: "readonly", Arity: 1, Group: "cluster", Summary: "Allows reads from a replica in cluster mode (no-op)", Handler: func(x *Ctx) resp.Value { return resp.OK }},
		&Spec{Name: "readwrite", Arity: 1, Group: "cluster", Summary: "Disables READONLY (no-op)", Handler: func(x *Ctx) resp.Value { return resp.OK }},
	)
}

func clusterCmd(x *Ctx) resp.Value {
	if x.E.Cluster == nil || !x.E.Cluster.Enabled() {
		return resp.Err("ERR This instance has cluster support disabled")
	}
	return x.E.Cluster.Command(x.Args[1:])
}
