package command

import (
	"sort"
	"strings"

	"github.com/shamim3696/cache-boy/internal/feature/namespace"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

func init() {
	Register(
		&Spec{Name: "namespace", Arity: -2, Flags: FlagAllKeys, Group: "features",
			Summary: "NAMESPACE USE name | CURRENT | LIST | DROP name — isolate keys per app or tenant", Handler: namespaceCmd},
	)
}

func namespaceCmd(x *Ctx) resp.Value {
	switch x.ArgLower(1) {
	case "use":
		if x.NArgs() != 3 {
			return wrongArgs("namespace|use")
		}
		name := x.Arg(2)
		if name == "default" || name == "" {
			x.C.Namespace = ""
			return resp.OK
		}
		if !namespace.Valid(name) {
			return resp.Err("ERR invalid namespace name")
		}
		if x.C.Multi != nil {
			return resp.Err("ERR NAMESPACE USE is not allowed inside MULTI")
		}
		x.C.Namespace = name
		return resp.OK
	case "current":
		if x.C.Namespace == "" {
			return resp.Bulk("default")
		}
		return resp.Bulk(x.C.Namespace)
	case "list":
		counts := map[string]int64{}
		x.DB().ForEach(func(key string, _ *storeEntry) bool {
			ns := namespace.Of(key)
			if ns == "" {
				ns = "default"
			}
			counts[ns]++
			return true
		})
		names := make([]string, 0, len(counts))
		for n := range counts {
			names = append(names, n)
		}
		sort.Strings(names)
		var kv []resp.Value
		for _, n := range names {
			kv = append(kv, resp.Bulk(n), resp.Int(counts[n]))
		}
		return resp.Map(kv...)
	case "drop":
		if x.NArgs() != 3 {
			return wrongArgs("namespace|drop")
		}
		name := x.Arg(2)
		if name == "default" || !namespace.Valid(name) {
			return resp.Err("ERR can only drop a named namespace")
		}
		prefix := namespace.Prefix(name)
		var doomed []string
		x.DB().ForEach(func(key string, _ *storeEntry) bool {
			if strings.HasPrefix(key, prefix) {
				doomed = append(doomed, key)
			}
			return true
		})
		for _, k := range doomed {
			x.DB().Delete(k)
			x.Emit(k)
			x.PropagateNow("DEL", k)
		}
		return resp.Int(int64(len(doomed)))
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try NAMESPACE USE|CURRENT|LIST|DROP.", x.Arg(1))
}
