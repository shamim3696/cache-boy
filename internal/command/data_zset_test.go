package command

import (
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

func TestZAdd(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"ZADD z 1 a 2 b", "2"},
		{"ZADD z 3 a 4 c", "1"},
		{"ZADD z CH 5 a 4 c 6 d", "2"},
		{"ZSCORE z a", "5"},
		{"ZADD z NX 9 a 7 e", "1"},
		{"ZSCORE z a", "5"},
		{"ZADD z XX 9 a 7 f", "0"},
		{"ZSCORE z a", "9"},
		{"ZSCORE z f", "nil"},
		{"ZADD z GT CH 1 a 10 b", "1"},
		{"ZSCORE z a", "9"},
		{"ZSCORE z b", "10"},
		{"ZADD z LT CH 1 a 20 b", "1"},
		{"ZMSCORE z a b nope", "[1 10 nil]"},
		{"ZADD z INCR 2 a", "3"},
		{"ZADD z INCR GT -1 a", "nil"},
		{"ZADD z INCR NX 1 a", "nil"},
		{"ZADD z INCR XX 1 nope", "nil"},
		{"ZADD z XX 1 x", "0"},
		{"ZADD fresh XX 1 x", "0"},
		{"ZADD fresh XX INCR 1 x", "nil"},
		{"EXISTS fresh", "0"},
		{"ZADD z NX XX 1 a", "!ERR XX and NX options at the same time are not compatible"},
		{"ZADD z GT LT 1 a", "!ERR GT, LT, and/or NX"},
		{"ZADD z NX GT 1 a", "!ERR GT, LT, and/or NX"},
		{"ZADD z INCR 1 a 2 b", "!ERR INCR option supports a single increment-element pair"},
		{"ZADD z 1", "!ERR wrong number of arguments"},
		{"ZADD z NX 1", "!ERR syntax error"},
		{"ZADD z 1 a 2", "!ERR syntax error"},
		{"ZADD z x a", "!ERR value is not a valid float"},
		{"ZADD z nan a", "!ERR value is not a valid float"},
		{"ZADD z +inf top -inf bottom", "2"},
		{"ZADD z INCR -inf top", "!ERR resulting score is not a number"},
		{"ZINCRBY z 2 a", "5"},
		{"ZINCRBY z 2 new", "2"},
		{"ZINCRBY z x a", "!ERR value is not a valid float"},
		{"ZINCRBY z -inf top", "!ERR resulting score is not a number"},
		{"ZCARD z", "8"},
		{"ZCARD nope", "0"},
		{"ZREM z a nope", "1"},
		{"ZREM z nope", "0"},
		{"ZRANGE z 0 -1 WITHSCORES", "[bottom -inf new 2 c 4 d 6 e 7 b 10 top inf]"},
		{"SET str v", "OK"},
		{"ZADD str 1 a", "!WRONGTYPE"},
		{"ZSCORE str a", "!WRONGTYPE"},
		{"ZRANGE str 0 1", "!WRONGTYPE"},
		{"ZINCRBY str 1 a", "!WRONGTYPE"},
	})
}

func TestZRange(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"ZADD z 1 a 2 b 3 c 4 d 5 e", "5"},
		{"ZRANGE z 0 -1", "[a b c d e]"},
		{"ZRANGE z 1 2 WITHSCORES", "[b 2 c 3]"},
		{"ZRANGE z -2 -1", "[d e]"},
		{"ZRANGE z 3 1", "[]"},
		{"ZRANGE z 0 -1 REV", "[e d c b a]"},
		{"ZRANGE z 0 1 REV WITHSCORES", "[e 5 d 4]"},
		{"ZREVRANGE z 0 1", "[e d]"},
		{"ZREVRANGE z 0 0 WITHSCORES", "[e 5]"},
		{"ZRANGE nope 0 -1", "[]"},
		{"ZRANGE z a 1", "!ERR value is not an integer"},

		{"ZRANGE z 2 4 BYSCORE", "[b c d]"},
		{"ZRANGE z (2 4 BYSCORE", "[c d]"},
		{"ZRANGE z (2 (4 BYSCORE", "[c]"},
		{"ZRANGE z -inf +inf BYSCORE LIMIT 1 2", "[b c]"},
		{"ZRANGE z -inf +inf BYSCORE LIMIT 1 -1", "[b c d e]"},
		{"ZRANGE z -inf +inf BYSCORE LIMIT -1 2", "[]"},
		{"ZRANGE z 4 2 BYSCORE REV", "[d c b]"},
		{"ZRANGE z +inf -inf BYSCORE REV LIMIT 0 2 WITHSCORES", "[e 5 d 4]"},
		{"ZRANGEBYSCORE z 2 4", "[b c d]"},
		{"ZRANGEBYSCORE z -inf 2 WITHSCORES", "[a 1 b 2]"},
		{"ZRANGEBYSCORE z 0 10 LIMIT 3 10", "[d e]"},
		{"ZREVRANGEBYSCORE z 4 2", "[d c b]"},
		{"ZREVRANGEBYSCORE z (4 2 WITHSCORES LIMIT 0 1", "[c 3]"},
		{"ZRANGEBYSCORE z x 2", "!ERR min or max is not a float"},
		{"ZRANGEBYSCORE z 1 2 REV", "!ERR syntax error"},
		{"ZRANGE z 0 1 LIMIT 0 1", "!ERR syntax error, LIMIT is only supported"},
		{"ZRANGE z 0 1 BYSCORE BYLEX", "!ERR syntax error"},
		{"ZRANGE z - + BYLEX WITHSCORES", "!ERR syntax error, WITHSCORES not supported"},

		{"ZADD lex 0 a 0 b 0 c 0 d 0 e", "5"},
		{"ZRANGE lex [b (d BYLEX", "[b c]"},
		{"ZRANGE lex - + BYLEX LIMIT 1 2", "[b c]"},
		{"ZRANGE lex + - BYLEX REV LIMIT 0 2", "[e d]"},
		{"ZRANGEBYLEX lex - [c", "[a b c]"},
		{"ZRANGEBYLEX lex (a +", "[b c d e]"},
		{"ZREVRANGEBYLEX lex [d (a", "[d c b]"},
		{"ZRANGEBYLEX lex a c", "!ERR min or max not valid string range item"},
		{"ZLEXCOUNT lex [b +", "4"},
		{"ZLEXCOUNT lex - +", "5"},
		{"ZLEXCOUNT nope - +", "0"},
		{"ZLEXCOUNT lex x +", "!ERR min or max not valid string range item"},

		{"ZCOUNT z 2 4", "3"},
		{"ZCOUNT z (2 (4", "1"},
		{"ZCOUNT z -inf +inf", "5"},
		{"ZCOUNT z 10 20", "0"},
		{"ZCOUNT nope 0 1", "0"},
		{"ZCOUNT z x 1", "!ERR min or max is not a float"},

		{"ZRANK z c", "2"},
		{"ZREVRANK z c", "2"},
		{"ZRANK z a", "0"},
		{"ZREVRANK z a", "4"},
		{"ZRANK z c WITHSCORE", "[2 3]"},
		{"ZRANK z nope", "nil"},
		{"ZRANK nope a", "nil"},
		{"ZRANK z nope WITHSCORE", "nil"},
		{"ZRANK z a BOGUS", "!ERR syntax error"},

		{"ZRANGESTORE dst z 1 3", "3"},
		{"ZRANGE dst 0 -1 WITHSCORES", "[b 2 c 3 d 4]"},
		{"ZRANGESTORE dst z (1 3 BYSCORE LIMIT 0 1", "1"},
		{"ZRANGE dst 0 -1", "[b]"},
		{"ZRANGESTORE dst lex [a [b BYLEX REV", "0"},
		{"ZRANGESTORE dst lex [b [a BYLEX REV", "2"},
		{"ZRANGE dst 0 -1", "[a b]"},
		{"ZRANGESTORE dst z 10 20", "0"},
		{"EXISTS dst", "0"},
		{"ZRANGESTORE dst z 0 1 WITHSCORES", "!ERR syntax error"},
	})
}

func TestZRemRangeAndPop(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"ZADD z 1 a 2 b 3 c 4 d 5 e 6 f", "6"},
		{"ZREMRANGEBYRANK z 0 1", "2"},
		{"ZRANGE z 0 -1", "[c d e f]"},
		{"ZREMRANGEBYSCORE z (3 4", "1"},
		{"ZRANGE z 0 -1", "[c e f]"},
		{"ZREMRANGEBYSCORE z 100 200", "0"},
		{"ZREMRANGEBYRANK z -1 -1", "1"},
		{"ZREMRANGEBYRANK nope 0 -1", "0"},
		{"ZREMRANGEBYSCORE z x 1", "!ERR min or max is not a float"},
		{"ZADD lex 0 a 0 b 0 c", "3"},
		{"ZREMRANGEBYLEX lex [a (c", "2"},
		{"ZRANGE lex 0 -1", "[c]"},
		{"ZREMRANGEBYLEX lex - +", "1"},
		{"EXISTS lex", "0"},

		{"ZPOPMIN z", "[c 3]"},
		{"ZADD z 1 a 7 g", "2"},
		{"ZPOPMAX z", "[g 7]"},
		{"ZPOPMIN z 5", "[a 1 e 5]"},
		{"EXISTS z", "0"},
		{"ZPOPMIN z", "[]"},
		{"ZPOPMIN z -1", "!ERR value is out of range"},
		{"SET str v", "OK"},
		{"ZPOPMIN str", "!WRONGTYPE"},
		{"BZPOPMIN str 0", "!WRONGTYPE"},
		{"ZADD q 1 a 2 b", "2"},
		{"BZPOPMIN nope q 1", "[q a 1]"},
		{"BZPOPMAX q 1", "[q b 2]"},
		{"BZPOPMIN q 0.05", "nil"},
	})

	pusher := e.NewClient("tcp", "pusher")
	rec := &recorder{}
	e.AddPropagator(rec)
	done := make(chan resp.Value, 1)
	go func() { done <- do(e, c, "BZPOPMAX", "w1", "w2", "0") }()
	time.Sleep(50 * time.Millisecond)
	do(e, pusher, "ZADD", "w2", "1", "x", "5", "y")
	select {
	case v := <-done:
		if got := render(v); got != "[w2 y 5]" {
			t.Fatalf("BZPOPMAX: %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BZPOPMAX was not woken")
	}
	if got := rec.take(); len(got) != 2 || got[1] != "ZPOPMAX w2" {
		t.Fatalf("BZPOPMAX propagated %q", got)
	}
}

func TestZSetOps(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"ZADD a 1 x 2 y 3 z", "3"},
		{"ZADD b 10 y 20 z 30 w", "3"},
		{"SADD s x w", "2"},
		{"ZUNION 2 a b WITHSCORES", "[x 1 y 12 z 23 w 30]"},
		{"ZUNION 2 a b", "[x y z w]"},
		{"ZUNION 2 a b WEIGHTS 2 1 WITHSCORES", "[x 2 y 14 z 26 w 30]"},
		{"ZUNION 2 a b AGGREGATE MAX WITHSCORES", "[x 1 y 10 z 20 w 30]"},
		{"ZUNION 2 a b AGGREGATE MIN WITHSCORES", "[x 1 y 2 z 3 w 30]"},
		{"ZUNION 2 a s WITHSCORES", "[w 1 x 2 y 2 z 3]"},
		{"ZUNION 2 a nope WITHSCORES", "[x 1 y 2 z 3]"},
		{"ZINTER 2 a b WITHSCORES", "[y 12 z 23]"},
		{"ZINTER 2 a b AGGREGATE MAX WEIGHTS 1 0.5 WITHSCORES", "[y 5 z 10]"},
		{"ZINTER 2 a nope", "[]"},
		{"ZINTER 3 a b s", "[]"},
		{"ZDIFF 2 a b WITHSCORES", "[x 1]"},
		{"ZDIFF 1 a", "[x y z]"},
		{"ZDIFF 2 a s", "[y z]"},
		{"ZUNIONSTORE dst 2 a b", "4"},
		{"ZRANGE dst 0 -1 WITHSCORES", "[x 1 y 12 z 23 w 30]"},
		{"ZINTERSTORE dst 2 a b WEIGHTS 1 2", "2"},
		{"ZRANGE dst 0 -1 WITHSCORES", "[y 22 z 43]"},
		{"ZDIFFSTORE dst 2 a b", "1"},
		{"ZRANGE dst 0 -1", "[x]"},
		{"ZINTERSTORE dst 2 a nope", "0"},
		{"EXISTS dst", "0"},
		{"ZUNIONSTORE a 2 a b", "4"},
		{"ZCARD a", "4"},

		{"ZUNION 0 a", "!ERR numkeys"},
		{"ZUNION 3 a b", "!ERR"},
		{"ZUNION x a", "!ERR value is not an integer"},
		{"ZUNION 2 a b WEIGHTS 1", "!ERR syntax error"},
		{"ZUNION 2 a b WEIGHTS 1 x", "!ERR weight value is not a float"},
		{"ZUNION 2 a b AGGREGATE AVG", "!ERR syntax error"},
		{"ZDIFF 2 a b WEIGHTS 1 1", "!ERR syntax error"},
		{"ZUNIONSTORE dst 2 a b WITHSCORES", "!ERR syntax error"},
		{"SET str v", "OK"},
		{"ZUNION 2 a str", "!WRONGTYPE"},
		{"ZUNIONSTORE dst 1", "!ERR"},

		{"ZADD inf 1 m", "1"},
		{"ZADD inf2 +inf m", "1"},
		{"ZUNION 2 inf2 inf WEIGHTS 0 1 WITHSCORES", "[m 1]"},
	})
}

func TestZRandAndScan(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"ZADD z 1 a 2.5 b 3 c", "3"},
		{"ZSCAN z 0", "[0 [a 1 b 2.5 c 3]]"},
		{"ZSCAN z 0 MATCH b* COUNT 10", "[0 [b 2.5]]"},
		{"ZSCAN nope 0", "[0 []]"},
		{"ZSCAN z 0 NOVALUES", "!ERR syntax error"},
		{"ZRANDMEMBER nope", "nil"},
		{"ZRANDMEMBER nope 2", "[]"},
		{"ZRANDMEMBER z 0", "[]"},
		{"ZRANDMEMBER z 1 BOGUS", "!ERR syntax error"},
	})
	if v := do(e, c, "ZRANDMEMBER", "z"); v.Str != "a" && v.Str != "b" && v.Str != "c" {
		t.Fatalf("ZRANDMEMBER: %s", render(v))
	}
	if v := do(e, c, "ZRANDMEMBER", "z", "5"); len(v.Elems) != 3 {
		t.Fatalf("ZRANDMEMBER 5: %s", render(v))
	}
	if v := do(e, c, "ZRANDMEMBER", "z", "-5", "WITHSCORES"); len(v.Elems) != 10 || v.Elems[1].Type != resp.TypeDouble {
		t.Fatalf("ZRANDMEMBER -5 WITHSCORES: %s", render(v))
	}
}
