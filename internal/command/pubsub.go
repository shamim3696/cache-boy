package command

import (
	"cache-boy/internal/protocol/resp"
)

func init() {
	Register(
		&Spec{Name: "subscribe", Arity: -2, Flags: FlagPubSub | FlagNoScript, Group: "pubsub", Summary: "Listens for messages on channels", Handler: func(x *Ctx) resp.Value {
			return resp.Multi(x.E.PubSub.Subscribe(x.C, x.Args[1:]...)...)
		}},
		&Spec{Name: "unsubscribe", Arity: -1, Flags: FlagPubSub | FlagNoScript, Group: "pubsub", Summary: "Stops listening to channels (all if none given)", Handler: func(x *Ctx) resp.Value {
			return resp.Multi(x.E.PubSub.Unsubscribe(x.C, optionalList(x.Args[1:])...)...)
		}},
		&Spec{Name: "psubscribe", Arity: -2, Flags: FlagPubSub | FlagNoScript, Group: "pubsub", Summary: "Listens for messages on channels matching glob patterns", Handler: func(x *Ctx) resp.Value {
			return resp.Multi(x.E.PubSub.PSubscribe(x.C, x.Args[1:]...)...)
		}},
		&Spec{Name: "punsubscribe", Arity: -1, Flags: FlagPubSub | FlagNoScript, Group: "pubsub", Summary: "Stops listening to patterns (all if none given)", Handler: func(x *Ctx) resp.Value {
			return resp.Multi(x.E.PubSub.PUnsubscribe(x.C, optionalList(x.Args[1:])...)...)
		}},
		&Spec{Name: "publish", Arity: 3, Group: "pubsub", Summary: "Sends a message; returns the number of receivers", Handler: func(x *Ctx) resp.Value {
			return resp.Int(int64(x.E.PubSub.Publish(x.Arg(1), x.Arg(2))))
		}},
		&Spec{Name: "pubsub", Arity: -2, Group: "pubsub", Summary: "PUBSUB CHANNELS [pattern] | NUMSUB [channel...] | NUMPAT", Handler: pubsubCmd},
	)
}

// optionalList returns nil for an empty list ("all") instead of an empty slice.
func optionalList(args []string) []string {
	if len(args) == 0 {
		return nil
	}
	return args
}

func pubsubCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "channels":
		return resp.Strings(x.E.PubSub.Channels(x.Arg(2)))
	case "numsub":
		var kv []resp.Value
		for _, ch := range x.Args[2:] {
			kv = append(kv, resp.Bulk(ch), resp.Int(int64(x.E.PubSub.NumSub(ch))))
		}
		return resp.Array(kv...)
	case "numpat":
		return resp.Int(int64(x.E.PubSub.NumPat()))
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try PUBSUB HELP.", x.Arg(1))
}
