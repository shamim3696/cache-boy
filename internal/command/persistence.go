package command

import (
	"cache-boy/internal/protocol/resp"
)

// Persistence commands. The work is done by e.Persist (package persistence).

func init() {
	Register(
		&Spec{Name: "save", Arity: 1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Synchronously saves a snapshot to disk", Handler: saveCmd},
		&Spec{Name: "bgsave", Arity: -1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Saves a snapshot to disk in the background", Handler: bgsaveCmd},
		&Spec{Name: "bgrewriteaof", Arity: 1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Compacts the append-only file in the background", Handler: bgrewriteaofCmd},
		&Spec{Name: "lastsave", Arity: 1, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "Unix time of the last successful snapshot", Handler: lastsaveCmd},
	)
}

var errNoPersist = resp.Err("ERR persistence is not configured")

func saveCmd(x *Ctx) resp.Value {
	if x.E.Persist == nil {
		return errNoPersist
	}
	if x.InExec {
		// EXEC already holds every shard lock and Save takes them too.
		return resp.Err("ERR SAVE is not allowed inside MULTI; use BGSAVE")
	}
	if err := x.E.Persist.Save(); err != nil {
		return persistErr(err)
	}
	return resp.OK
}

func bgsaveCmd(x *Ctx) resp.Value {
	if x.E.Persist == nil {
		return errNoPersist
	}
	if x.NArgs() > 2 || (x.NArgs() == 2 && x.ArgLower(1) != "schedule") {
		return ErrSyntax
	}
	if err := x.E.Persist.BgSave(); err != nil {
		return persistErr(err)
	}
	return resp.Simple("Background saving started")
}

func bgrewriteaofCmd(x *Ctx) resp.Value {
	if x.E.Persist == nil {
		return errNoPersist
	}
	if err := x.E.Persist.BgRewriteAOF(); err != nil {
		return persistErr(err)
	}
	return resp.Simple("Background append only file rewriting started")
}

func lastsaveCmd(x *Ctx) resp.Value {
	if x.E.Persist == nil {
		return errNoPersist
	}
	return resp.Int(x.E.Persist.LastSave().Unix())
}

// persistErr turns a Go error into a RESP error, adding "ERR" when the
// message has no error code yet.
func persistErr(err error) resp.Value {
	return resp.Err(err.Error())
}
