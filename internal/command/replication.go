package command

import (
	"strings"
	"sync"
	"time"

	"cache-boy/internal/protocol/resp"
)

// Replication commands. The work is done by e.Repl (package replication).

func init() {
	Register(
		&Spec{Name: "replicaof", Arity: 3, Flags: FlagAdmin | FlagNoScript | FlagNoMulti, Group: "server", Summary: "REPLICAOF host port | NO ONE: follow a leader or become one", Handler: replicaofCmd},
		&Spec{Name: "slaveof", Arity: 3, Flags: FlagAdmin | FlagNoScript | FlagNoMulti, Group: "server", Summary: "Old name of REPLICAOF", Handler: replicaofCmd},
		&Spec{Name: "role", Arity: 1, Group: "server", Summary: "Replication role: master or slave, with offsets", Handler: roleCmd},
		// FlagAllKeys: the snapshot and the start of the replica's command
		// stream must be taken atomically with respect to writes.
		&Spec{Name: "sync", Arity: 1, Flags: FlagAdmin | FlagAllKeys | FlagNoScript | FlagNoMulti, Group: "server", Summary: "Full resynchronisation (used by replicas)", Handler: syncCmd},
		&Spec{Name: "psync", Arity: -3, Flags: FlagAdmin | FlagAllKeys | FlagNoScript | FlagNoMulti, Group: "server", Summary: "Same as SYNC: always a full resynchronisation", Handler: syncCmd},
		&Spec{Name: "replconf", Arity: -1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Replica handshake options (listening-port, capa, ack)", Handler: replconfCmd},
		&Spec{Name: "wait", Arity: 3, Flags: FlagNoScript, Group: "server", Summary: "WAIT numreplicas timeout-ms: waits until replicas acknowledged previous writes", Handler: waitCmd},
		&Spec{Name: "raft.status", Arity: 1, Group: "server", Summary: "Raft election state of this node: id, state, term, leader", Handler: raftStatusCmd},
	)
}

var errNoRepl = resp.Err("ERR replication is not configured")

func replicaofCmd(x *Ctx) resp.Value {
	if x.E.Repl == nil {
		return errNoRepl
	}
	value := x.Arg(1) + " " + x.Arg(2)
	if strings.EqualFold(x.Arg(1), "no") && strings.EqualFold(x.Arg(2), "one") {
		value = "no one"
	} else if p, ok := x.Int(2); !ok || p <= 0 || p > 65535 {
		return resp.Err("ERR Invalid master port")
	}
	if err := x.E.SetConfig("replicaof", value); err != nil {
		return resp.Errf("ERR %v", err)
	}
	return resp.OK
}

func roleCmd(x *Ctx) resp.Value {
	if x.E.Repl == nil {
		return resp.Array(resp.Bulk("master"), resp.Int(0), resp.Array())
	}
	return x.E.Repl.Role()
}

func syncCmd(x *Ctx) resp.Value {
	if x.E.Repl == nil {
		return errNoRepl
	}
	return x.E.Repl.Sync(x.C, x.Args)
}

func replconfCmd(x *Ctx) resp.Value {
	if x.E.Repl == nil {
		return errNoRepl
	}
	return x.E.Repl.ReplConf(x.C, x.Args)
}

func waitCmd(x *Ctx) resp.Value {
	n, ok1 := x.Int(1)
	ms, ok2 := x.Int(2)
	if !ok1 || !ok2 || n < 0 {
		return ErrNotInt
	}
	if ms < 0 {
		return resp.Err("ERR timeout is negative")
	}
	if x.E.Repl == nil {
		return resp.Int(0)
	}
	timeout := time.Duration(ms) * time.Millisecond
	if x.InExec {
		timeout = -1 // inside MULTI/scripts: report the current count, never block
	}
	return resp.Int(int64(x.E.Repl.Wait(int(n), timeout)))
}

// RaftStatus is implemented by *raft.Node.
type RaftStatus interface {
	Status() (id, state string, term uint64, leader string)
}

var raftNodes sync.Map // *Engine -> RaftStatus

// SetRaft makes a Raft node visible to RAFT.STATUS (nil removes it).
func SetRaft(e *Engine, r RaftStatus) {
	if r == nil {
		raftNodes.Delete(e)
		return
	}
	raftNodes.Store(e, r)
}

func raftStatusCmd(x *Ctx) resp.Value {
	v, ok := raftNodes.Load(x.E)
	if !ok {
		return resp.Err("ERR raft is not enabled (set raft-port and raft-peer)")
	}
	id, state, term, leader := v.(RaftStatus).Status()
	return resp.Map(
		resp.Bulk("id"), resp.Bulk(id),
		resp.Bulk("state"), resp.Bulk(state),
		resp.Bulk("term"), resp.Int(int64(term)),
		resp.Bulk("leader"), resp.Bulk(leader),
	)
}
