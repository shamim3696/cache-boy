package command

import (
	"net/url"

	"cache-boy/internal/feature/watch"
	"cache-boy/internal/protocol/resp"
)

// Key change streams (KEYWATCH) and webhooks (feature/watch).
//
// KEYWATCH patterns are scoped to the client's namespace; pushed keys have
// the namespace removed. Webhooks are scoped to the namespace they were
// added in (the internal option _NS carries it through AOF and snapshots).
// Webhooks are replicated like data, but only the leader delivers them.

func init() {
	Register(
		&Spec{Name: "keywatch", Arity: -2, Flags: FlagPubSub | FlagNoScript, Group: "features",
			Summary: "KEYWATCH pattern [pattern ...] — streams [\"keyevent\", key, op] for matching keys", Handler: keywatchCmd},
		&Spec{Name: "keyunwatch", Arity: -1, Flags: FlagPubSub | FlagNoScript, Group: "features",
			Summary: "KEYUNWATCH [pattern ...] — stops KEYWATCH patterns (all if none given)", Handler: keyunwatchCmd},
		&Spec{Name: "webhook.add", Arity: -4, Flags: FlagWrite | FlagAdmin | FlagNoScript, Group: "features",
			Summary: "WEBHOOK.ADD id pattern url [SECRET s] — POSTs key changes to url", Handler: webhookAddCmd},
		&Spec{Name: "webhook.del", Arity: -2, Flags: FlagWrite | FlagAdmin | FlagNoScript, Group: "features",
			Summary: "WEBHOOK.DEL id — removes a webhook", Handler: webhookDelCmd},
		&Spec{Name: "webhook.list", Arity: 1, Flags: FlagAdmin, Group: "features",
			Summary: "WEBHOOK.LIST — [id, pattern, url, delivered, failed] per webhook", Handler: webhookListCmd},
	)
}

func keywatchCmd(x *Ctx) resp.Value {
	return resp.Multi(x.E.Watch.Watch(x.C, x.C.Namespace, x.Args[1:]...)...)
}

func keyunwatchCmd(x *Ctx) resp.Value {
	return resp.Multi(x.E.Watch.Unwatch(x.C, x.C.Namespace, x.Args[1:]...)...)
}

// webhookGate makes only the leader deliver webhooks.
func webhookGate(e *Engine) {
	e.Watch.SetDeliveryGate(func() bool { return e.Repl == nil || !e.Repl.IsReplica() })
}

func webhookAddCmd(x *Ctx) resp.Value {
	id, pat, target := x.Arg(1), x.Arg(2), x.Arg(3)
	secret, ns := "", x.C.Namespace
	for i := 4; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		switch x.ArgLower(i) {
		case "secret":
			secret = x.Arg(i + 1)
		case "_ns":
			ns = x.Arg(i + 1)
		default:
			return ErrSyntax
		}
	}
	if id == "" {
		return resp.Err("ERR empty webhook id")
	}
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return resp.Err("ERR invalid webhook url (http or https expected)")
	}
	webhookGate(x.E)
	x.E.Watch.AddWebhook(ns, id, pat, target, secret)
	x.Propagate(watch.AddArgs(ns, id, pat, target, secret)...)
	return resp.OK
}

func webhookDelCmd(x *Ctx) resp.Value {
	ns := x.C.Namespace
	switch {
	case x.NArgs() == 2:
	case x.NArgs() == 4 && x.ArgLower(2) == "_ns":
		ns = x.Arg(3)
	default:
		return ErrSyntax
	}
	if !x.E.Watch.DelWebhook(ns, x.Arg(1)) {
		x.NoChange()
		return resp.Int(0)
	}
	if ns != "" {
		x.Propagate("WEBHOOK.DEL", x.Arg(1), "_NS", ns)
	} else {
		x.Propagate("WEBHOOK.DEL", x.Arg(1))
	}
	return resp.Int(1)
}

func webhookListCmd(x *Ctx) resp.Value {
	hooks := x.E.Watch.Webhooks(x.C.Namespace)
	out := make([]resp.Value, len(hooks))
	for i, w := range hooks {
		out[i] = resp.Array(resp.Bulk(w.ID), resp.Bulk(w.Pattern), resp.Bulk(w.URL),
			resp.Int(w.Delivered()), resp.Int(w.Failed()))
	}
	return resp.Array(out...)
}
