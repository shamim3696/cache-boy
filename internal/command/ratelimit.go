package command

import (
	"strconv"
	"strings"
	"time"

	"cache-boy/internal/protocol/resp"
)

// GCRA rate limiting (feature/ratelimit). Rate-limit keys are not keyspace
// keys: they are namespaced by hand and are neither persisted nor replicated.

func init() {
	Register(
		&Spec{Name: "ratelimit", Arity: -4, Group: "features",
			Summary: "RATELIMIT key limit period [COST n] — [allowed, remaining, retry_after_ms, reset_after_ms]", Handler: ratelimitCmd},
		&Spec{Name: "ratelimit.peek", Arity: 4, Group: "features",
			Summary: "RATELIMIT.PEEK key limit period — like RATELIMIT without consuming", Handler: ratelimitCmd},
		&Spec{Name: "ratelimit.reset", Arity: 2, Group: "features",
			Summary: "RATELIMIT.RESET key — forgets a rate-limit key", Handler: ratelimitResetCmd},
	)
}

func ratelimitCmd(x *Ctx) resp.Value {
	limit, ok := x.Int(2)
	if !ok || limit <= 0 {
		return resp.Err("ERR limit must be a positive integer")
	}
	period, ok := featureDuration(x.Arg(3))
	if !ok || period <= 0 {
		return resp.Err("ERR invalid period")
	}
	cost := int64(1)
	peek := x.Name == "ratelimit.peek"
	for i := 4; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "cost":
			if i+1 >= x.NArgs() {
				return ErrSyntax
			}
			n, ok := x.Int(i + 1)
			if !ok || n < 0 {
				return resp.Err("ERR COST must be a non-negative integer")
			}
			cost = n
			i++
		default:
			return ErrSyntax
		}
	}
	r := x.E.RateLimit.Take(x.StoredKey(x.Arg(1)), limit, period, cost, peek)
	allowed := int64(0)
	if r.Allowed {
		allowed = 1
	}
	return resp.Array(resp.Int(allowed), resp.Int(r.Remaining), resp.Int(featCeilMs(r.RetryAfter)), resp.Int(featCeilMs(r.ResetAfter)))
}

func ratelimitResetCmd(x *Ctx) resp.Value {
	if x.E.RateLimit.Reset(x.StoredKey(x.Arg(1))) {
		return resp.Int(1)
	}
	return resp.Int(0)
}

// --- helpers shared by the feature commands ---

// featureDuration parses a <duration>: a plain number of milliseconds, or a
// number with a unit (ms, s, m, h), e.g. 500ms, 10s, 1.5m, 1h.
func featureDuration(s string) (time.Duration, bool) {
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n < 0 || n > int64(time.Duration(1<<62)/time.Millisecond) {
			return 0, false
		}
		return time.Duration(n) * time.Millisecond, true
	}
	ls := strings.ToLower(s)
	if !strings.HasSuffix(ls, "ms") && !strings.HasSuffix(ls, "s") && !strings.HasSuffix(ls, "m") && !strings.HasSuffix(ls, "h") {
		return 0, false
	}
	d, err := time.ParseDuration(ls)
	if err != nil || d < 0 {
		return 0, false
	}
	return d, true
}

func featCeilMs(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Millisecond - 1) / time.Millisecond)
}

func featInt01(b bool) resp.Value {
	if b {
		return resp.Int(1)
	}
	return resp.Int(0)
}
