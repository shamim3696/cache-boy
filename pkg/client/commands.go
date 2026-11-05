package client

import (
	"context"
	"strconv"
	"time"

	"cache-boy/internal/protocol/resp"
)

// --- reply conversion helpers ---

func asString(v Value, err error) (string, error) {
	if err != nil {
		return "", err
	}
	if v.IsNull() {
		return "", ErrNil
	}
	return v.String(), nil
}

func asInt(v Value, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	if v.IsNull() {
		return 0, ErrNil
	}
	if v.Type == resp.TypeInteger {
		return v.Int, nil
	}
	if v.Type == resp.TypeBool {
		if v.Bool {
			return 1, nil
		}
		return 0, nil
	}
	return strconv.ParseInt(v.String(), 10, 64)
}

func asBool(v Value, err error) (bool, error) {
	n, err := asInt(v, err)
	return n != 0, err
}

func asFloat(v Value) float64 {
	if v.Type == resp.TypeDouble {
		return v.Float
	}
	if v.Type == resp.TypeInteger {
		return float64(v.Int)
	}
	f, _ := strconv.ParseFloat(v.String(), 64)
	if s := v.String(); s == "inf" || s == "+inf" {
		f = posInf
	} else if s == "-inf" {
		f = -posInf
	}
	return f
}

var posInf = func() float64 { f, _ := strconv.ParseFloat("+Inf", 64); return f }()

func asStrings(v Value, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	if v.IsNull() {
		return nil, ErrNil
	}
	out := make([]string, len(v.Elems))
	for i, e := range v.Elems {
		out[i] = e.String()
	}
	return out, nil
}

// asMap reads a RESP3 map or a RESP2 flat [k, v, k, v...] array.
func asMap(v Value, err error) (map[string]string, error) {
	if err != nil {
		return nil, err
	}
	if v.IsNull() {
		return nil, ErrNil
	}
	m := make(map[string]string, len(v.Elems)/2)
	for i := 0; i+1 < len(v.Elems); i += 2 {
		m[v.Elems[i].String()] = v.Elems[i+1].String()
	}
	return m, nil
}

// ttlArgs converts a TTL to EX/PX arguments (seconds when exact).
func ttlArgs(ttl time.Duration) []any {
	if ttl <= 0 {
		return nil
	}
	if ttl%time.Second == 0 {
		return []any{"EX", int64(ttl / time.Second)}
	}
	return []any{"PX", max(ttl.Milliseconds(), 1)}
}

// ms formats a duration as a whole number of milliseconds (at least 1).
func ms(d time.Duration) string { return strconv.FormatInt(max(d.Milliseconds(), 1), 10) }

// --- strings & keys ---

// Get returns the value of key, or ErrNil.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return asString(c.Do(ctx, "GET", key))
}

// SetOptions are the options of Set.
type SetOptions struct {
	TTL  time.Duration // EX/PX; 0 = no expiry
	NX   bool          // only set if the key doesn't exist
	XX   bool          // only set if the key exists
	Tags []string      // cache-boy tags (see TagInvalidate)
}

// Set stores value at key. It returns false (and no error) when NX/XX
// prevented the write.
func (c *Client) Set(ctx context.Context, key string, value any, opts ...SetOptions) (bool, error) {
	args := []any{"SET", key, value}
	if len(opts) > 0 {
		o := opts[0]
		args = append(args, ttlArgs(o.TTL)...)
		if o.NX {
			args = append(args, "NX")
		}
		if o.XX {
			args = append(args, "XX")
		}
		if len(o.Tags) > 0 {
			args = append(args, "TAGS", o.Tags)
		}
	}
	v, err := c.Do(ctx, args...)
	if err != nil {
		return false, err
	}
	return !v.IsNull(), nil
}

// Del deletes keys and returns how many existed.
func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return asInt(c.Do(ctx, "DEL", keys))
}

// Exists counts how many of keys exist.
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return asInt(c.Do(ctx, "EXISTS", keys))
}

// Incr increments an integer value and returns the new value.
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return asInt(c.Do(ctx, "INCR", key))
}

// Expire sets a TTL (millisecond precision); false if the key doesn't exist.
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	if ttl%time.Second == 0 {
		return asBool(c.Do(ctx, "EXPIRE", key, int64(ttl/time.Second)))
	}
	return asBool(c.Do(ctx, "PEXPIRE", key, max(ttl.Milliseconds(), 1)))
}

// NoExpiry is returned by TTL for a key without a TTL.
const NoExpiry time.Duration = -1

// TTL returns the remaining time to live, NoExpiry, or ErrNil if the key
// doesn't exist.
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	n, err := asInt(c.Do(ctx, "PTTL", key))
	switch {
	case err != nil:
		return 0, err
	case n == -2:
		return 0, ErrNil
	case n == -1:
		return NoExpiry, nil
	}
	return time.Duration(n) * time.Millisecond, nil
}

// --- hashes, lists, sets, sorted sets ---

// HSet sets hash fields and returns how many were new.
func (c *Client) HSet(ctx context.Context, key string, fields map[string]any) (int64, error) {
	args := make([]any, 0, 2+2*len(fields))
	args = append(args, "HSET", key)
	for f, v := range fields {
		args = append(args, f, v)
	}
	return asInt(c.Do(ctx, args...))
}

// HGetAll returns every field of a hash (empty map if missing).
func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return asMap(c.Do(ctx, "HGETALL", key))
}

// LPush prepends values and returns the new length.
func (c *Client) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	return asInt(c.Do(ctx, append([]any{"LPUSH", key}, values...)...))
}

// RPop removes and returns the last element, or ErrNil.
func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	return asString(c.Do(ctx, "RPOP", key))
}

// SAdd adds members and returns how many were new.
func (c *Client) SAdd(ctx context.Context, key string, members ...any) (int64, error) {
	return asInt(c.Do(ctx, append([]any{"SADD", key}, members...)...))
}

// SMembers returns all members of a set.
func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	return asStrings(c.Do(ctx, "SMEMBERS", key))
}

// Z is a sorted-set member with its score.
type Z struct {
	Member string
	Score  float64
}

// ZAdd adds members and returns how many were new.
func (c *Client) ZAdd(ctx context.Context, key string, members ...Z) (int64, error) {
	args := []any{"ZADD", key}
	for _, m := range members {
		args = append(args, m.Score, m.Member)
	}
	return asInt(c.Do(ctx, args...))
}

// ZRange returns members by rank (inclusive, negative = from the end).
func (c *Client) ZRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return asStrings(c.Do(ctx, "ZRANGE", key, start, stop))
}

// ZRangeWithScores returns members and scores by rank.
func (c *Client) ZRangeWithScores(ctx context.Context, key string, start, stop int64) ([]Z, error) {
	v, err := c.Do(ctx, "ZRANGE", key, start, stop, "WITHSCORES")
	if err != nil {
		return nil, err
	}
	var out []Z
	if len(v.Elems) > 0 && len(v.Elems[0].Elems) == 2 { // RESP3: [[member, score], ...]
		for _, p := range v.Elems {
			out = append(out, Z{p.Elems[0].String(), asFloat(p.Elems[1])})
		}
		return out, nil
	}
	for i := 0; i+1 < len(v.Elems); i += 2 {
		out = append(out, Z{v.Elems[i].String(), asFloat(v.Elems[i+1])})
	}
	return out, nil
}

// Publish sends a message and returns how many subscribers received it.
func (c *Client) Publish(ctx context.Context, channel string, message any) (int64, error) {
	return asInt(c.Do(ctx, "PUBLISH", channel, message))
}

// Eval runs a server-side JavaScript script (see docs/features.md).
func (c *Client) Eval(ctx context.Context, script string, keys []string, args ...any) (Value, error) {
	a := []any{"EVAL", script, len(keys), keys}
	return c.Do(ctx, append(a, args...)...)
}
