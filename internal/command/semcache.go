package command

import (
	"strconv"

	"cache-boy/internal/feature/semcache"
	"cache-boy/internal/protocol/resp"
)

// Semantic cache for LLM responses (feature/semcache). Cache names are
// namespaced by hand (they are not keyspace keys). Writes are propagated by
// the manager while its mutex is held, with explicit IDs and absolute expiry.

func init() {
	const w = FlagWrite | FlagNoPropagate
	Register(
		&Spec{Name: "scache.set", Arity: -4, Flags: w | FlagDenyOOM, Group: "features",
			Summary: "SCACHE.SET name vector response [PROMPT text] [EX s|PX ms|PXAT ms] [ID id] — stores a response by prompt embedding", Handler: scacheSetCmd},
		&Spec{Name: "scache.get", Arity: -3, Flags: FlagReadOnly, Group: "features",
			Summary: "SCACHE.GET name vector [PROMPT text] [THRESHOLD t] — [hit|miss, response, score, id]", Handler: scacheGetCmd},
		&Spec{Name: "scache.del", Arity: 3, Flags: FlagWrite, Group: "features",
			Summary: "SCACHE.DEL name id — removes one entry", Handler: scacheDelCmd},
		&Spec{Name: "scache.config", Arity: -2, Flags: w, Group: "features",
			Summary: "SCACHE.CONFIG name [THRESHOLD t] [MAXENTRIES n] — tunes a cache", Handler: scacheConfigCmd},
		&Spec{Name: "scache.info", Arity: 2, Flags: FlagReadOnly, Group: "features",
			Summary: "SCACHE.INFO name — {entries, dim, threshold, maxentries, hits, misses, hit_ratio}", Handler: scacheInfoCmd},
		&Spec{Name: "scache.drop", Arity: 2, Flags: FlagWrite, Group: "features",
			Summary: "SCACHE.DROP name — deletes the whole cache", Handler: scacheDropCmd},
	)
}

func scacheSetCmd(x *Ctx) resp.Value {
	vec, err := semcache.ParseVector(x.Arg(2))
	if err != nil {
		return resp.Err(err.Error())
	}
	now := x.Now()
	var id, prompt string
	var expireAt int64
	for i := 4; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		val := x.Arg(i + 1)
		switch x.ArgLower(i) {
		case "id":
			id = val
		case "prompt":
			prompt = val
		case "px": // <duration>: plain ms or "1h", "30m"...
			d, ok := featureDuration(val)
			if !ok || d <= 0 {
				return resp.Err("ERR invalid expire time in 'scache.set' command")
			}
			expireAt = now + featCeilMs(d)
		case "ex", "pxat":
			n, ok := ParseInt(val)
			if !ok || n <= 0 {
				return resp.Err("ERR invalid expire time in 'scache.set' command")
			}
			if x.ArgLower(i) == "ex" {
				expireAt = now + n*1000
			} else {
				expireAt = n
			}
		default:
			return ErrSyntax
		}
	}
	id, err = x.E.SemCache.Set(x.StoredKey(x.Arg(1)), id, prompt, vec, x.Arg(3), expireAt, now, !x.C.IsMaster, x.PropagateNow)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Bulk(id)
}

func scacheGetCmd(x *Ctx) resp.Value {
	vec, err := semcache.ParseVector(x.Arg(2))
	if err != nil {
		return resp.Err(err.Error())
	}
	var prompt string
	var threshold float32
	for i := 3; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		switch x.ArgLower(i) {
		case "prompt":
			prompt = x.Arg(i + 1)
		case "threshold":
			t, ok := ParseFloat(x.Arg(i + 1))
			if !ok || t <= 0 || t > 1 {
				return resp.Err("ERR THRESHOLD must be in (0, 1]")
			}
			threshold = float32(t)
		default:
			return ErrSyntax
		}
	}
	r, err := x.E.SemCache.Get(x.StoredKey(x.Arg(1)), vec, prompt, threshold, x.Now())
	if err != nil {
		return resp.Err(err.Error())
	}
	if !r.Hit {
		score := resp.Null()
		if r.ID != "" {
			score = resp.Double(float64(r.Score))
		}
		return resp.Array(resp.Bulk("miss"), resp.Null(), score, resp.Null())
	}
	return resp.Array(resp.Bulk("hit"), resp.Bulk(r.Response), resp.Double(float64(r.Score)), resp.Bulk(r.ID))
}

func scacheDelCmd(x *Ctx) resp.Value {
	if !x.E.SemCache.Del(x.StoredKey(x.Arg(1)), x.Arg(2)) {
		x.NoChange()
		return resp.Int(0)
	}
	x.Propagate("SCACHE.DEL", x.StoredKey(x.Arg(1)), x.Arg(2))
	return resp.Int(1)
}

func scacheConfigCmd(x *Ctx) resp.Value {
	var threshold float32
	var maxEntries int
	for i := 2; i < x.NArgs(); i += 2 {
		if i+1 >= x.NArgs() {
			return ErrSyntax
		}
		switch x.ArgLower(i) {
		case "threshold":
			t, ok := ParseFloat(x.Arg(i + 1))
			if !ok || t <= 0 || t > 1 {
				return resp.Err("ERR THRESHOLD must be in (0, 1]")
			}
			threshold = float32(t)
		case "maxentries":
			n, ok := ParseInt(x.Arg(i + 1))
			if !ok || n <= 0 {
				return ErrNotInt
			}
			maxEntries = int(n)
		default:
			return ErrSyntax
		}
	}
	x.E.SemCache.Configure(x.StoredKey(x.Arg(1)), threshold, maxEntries, x.Now(), !x.C.IsMaster, x.PropagateNow)
	return resp.OK
}

func scacheInfoCmd(x *Ctx) resp.Value {
	in, err := x.E.SemCache.Info(x.StoredKey(x.Arg(1)))
	if err != nil {
		return resp.Err(err.Error())
	}
	ratio := 0.0
	if in.Hits+in.Misses > 0 {
		ratio = float64(in.Hits) / float64(in.Hits+in.Misses)
	}
	return resp.Map(
		resp.Bulk("entries"), resp.Int(int64(in.Entries)),
		resp.Bulk("dim"), resp.Int(int64(in.Dim)),
		resp.Bulk("threshold"), resp.Double(float64(in.Threshold)),
		resp.Bulk("maxentries"), resp.Int(int64(in.MaxEntries)),
		resp.Bulk("hits"), resp.Int(in.Hits),
		resp.Bulk("misses"), resp.Int(in.Misses),
		resp.Bulk("hit_ratio"), resp.Bulk(strconv.FormatFloat(ratio, 'f', 4, 64)),
	)
}

func scacheDropCmd(x *Ctx) resp.Value {
	n := x.E.SemCache.Drop(x.StoredKey(x.Arg(1)))
	if n == 0 {
		x.NoChange()
	} else {
		x.Propagate("SCACHE.DROP", x.StoredKey(x.Arg(1)))
	}
	return resp.Int(int64(n))
}
