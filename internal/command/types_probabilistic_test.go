package command

import (
	"strconv"
	"testing"
)

func TestBloomCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"BF.RESERVE b 0.01 1000", "OK"},
		{"BF.RESERVE b 0.01 1000", "!ERR item exists"},
		{"BF.RESERVE x 1 100", "!ERR (0 < error rate range < 1)"},
		{"BF.RESERVE x abc 100", "!ERR bad error rate"},
		{"BF.RESERVE x 0.01 0", "!ERR (capacity should be larger than 0)"},
		{"BF.RESERVE x 0.01 abc", "!ERR bad capacity"},
		{"BF.RESERVE x 0.01 10 EXPANSION 0", "!ERR expansion should be greater or equal to 1"},
		{"BF.RESERVE x 0.01 10 EXPANSION", "!ERR syntax error"},
		{"BF.RESERVE x 0.01 10 EXPANSION 2 NONSCALING", "!ERR nonscaling filters cannot expand"},
		{"BF.RESERVE x 0.01 10 BOGUS", "!ERR syntax error"},
		{"BF.RESERVE x 0.01 100000000000000", "!ERR filter would be too large"},
		{"EXISTS x", "0"},
		{"BF.ADD b apple", "1"},
		{"BF.ADD b apple", "0"},
		{"BF.MADD b apple pear plum", "[0 1 1]"},
		{"BF.EXISTS b pear", "1"},
		{"BF.EXISTS b grape", "0"},
		{"BF.MEXISTS b apple grape plum", "[1 0 1]"},
		{"BF.CARD b", "3"},
		{"BF.INFO b", "{Capacity:1000 Size:1512 Number of filters:1 Number of items inserted:3 Expansion rate:2}"},
		{"BF.INFO b ITEMS", "[3]"},
		{"BF.INFO b capacity", "[1000]"},
		{"BF.INFO b bogus", "!ERR Invalid information value"},
		{"TYPE b", "MBbloom--"},

		// Missing keys: reads answer 0, BF.ADD creates a default filter.
		{"BF.EXISTS nope a", "0"},
		{"BF.MEXISTS nope a b", "[0 0]"},
		{"BF.CARD nope", "0"},
		{"BF.INFO nope", "!ERR not found"},
		{"BF.ADD d x", "1"},
		{"BF.INFO d", "{Capacity:100 Size:272 Number of filters:1 Number of items inserted:1 Expansion rate:2}"},

		{"BF.RESERVE ns 0.01 2 NONSCALING", "OK"},
		{"BF.MADD ns a b c", "[1 1 !ERR non scaling filter is full]"},
		{"BF.INFO ns EXPANSION", "[nil]"},

		{"SET s v", "OK"},
		{"BF.ADD s a", "!WRONGTYPE"},
		{"BF.EXISTS s a", "!WRONGTYPE"},
		{"BF.CARD s", "!WRONGTYPE"},
		{"BF.RESERVE s 0.01 10", "!ERR item exists"},
	})
}

func TestBloomScales(t *testing.T) {
	e, c := newDataEngine(t)
	mustStr(t, e, c, "OK", "BF.RESERVE", "b", "0.01", "100", "EXPANSION", "4")
	for i := range 1000 {
		do(e, c, "BF.ADD", "b", "item:"+strconv.Itoa(i))
	}
	if got := render(do(e, c, "BF.INFO", "b", "FILTERS")); got != "[3]" { // 100 + 400 + 1600
		t.Fatalf("filters: %s", got)
	}
	for i := range 1000 {
		if v := do(e, c, "BF.EXISTS", "b", "item:"+strconv.Itoa(i)); v.Int != 1 {
			t.Fatalf("false negative for item:%d", i)
		}
	}
}

func TestBloomPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	do(e, c, "BF.ADD", "b", "x")
	do(e, c, "BF.ADD", "b", "x") // no change: not propagated
	do(e, c, "BF.MADD", "b", "x", "y")
	do(e, c, "BF.MADD", "b", "x", "y")
	want := []string{"BF.ADD b x", "BF.MADD b x y"}
	if got := rec.take(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("propagated %q, want %q", got, want)
	}
}

func TestHLLCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"PFADD h a b c d", "1"},
		{"PFADD h a b", "0"},
		{"PFCOUNT h", "4"},
		{"PFADD empty", "1"},
		{"PFADD empty", "0"},
		{"PFCOUNT empty", "0"},
		{"PFCOUNT nope", "0"},
		{"TYPE h", "hyperloglog"},
		{"PFADD h2 c d e f", "1"},
		{"PFCOUNT h h2", "6"},
		{"PFCOUNT h h2 nope", "6"},
		{"PFMERGE m h h2", "OK"},
		{"PFCOUNT m", "6"},
		{"PFADD m g", "1"},
		{"PFMERGE m h", "OK"}, // the destination's own registers are kept
		{"PFCOUNT m", "7"},
		{"PFMERGE fresh", "OK"},
		{"PFCOUNT fresh", "0"},
		{"EXISTS fresh", "1"},
		{"SET s v", "OK"},
		{"PFADD s a", "!WRONGTYPE"},
		{"PFCOUNT h s", "!WRONGTYPE"},
		{"PFMERGE m s", "!WRONGTYPE"},
		{"PFMERGE s h", "!WRONGTYPE"},
		{"GET h", "!WRONGTYPE"},
	})
}

func TestHLLAccuracyThroughCommands(t *testing.T) {
	e, c := newDataEngine(t)
	const n = 100_000
	args := []string{"PFADD", "a"}
	for i := range n {
		args = append(args, "user:"+strconv.Itoa(i))
		if len(args) == 1002 {
			do(e, c, args...)
			args = args[:2]
		}
	}
	got := do(e, c, "PFCOUNT", "a").Int
	if d := float64(got-n) / n; d > 0.02 || d < -0.02 {
		t.Fatalf("PFCOUNT %d for %d distinct elements", got, n)
	}
}

func TestHLLPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	do(e, c, "PFADD", "h", "a")
	do(e, c, "PFADD", "h", "a")
	do(e, c, "PFMERGE", "m", "h")
	want := []string{"PFADD h a", "PFMERGE m h"}
	if got := rec.take(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("propagated %q, want %q", got, want)
	}
}

func TestCopyAndEncodingOfProbabilisticTypes(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	do(e, c, "BF.ADD", "bf", "a")
	do(e, c, "PFADD", "hll", "x", "y")
	mustStr(t, e, c, "1", "COPY", "bf", "bf2")
	mustStr(t, e, c, "1", "COPY", "hll", "hll2")
	mustStr(t, e, c, "1", "BF.EXISTS", "bf2", "a")
	mustStr(t, e, c, "2", "PFCOUNT", "hll2")
	mustStr(t, e, c, "bloom", "OBJECT", "ENCODING", "bf")
	mustStr(t, e, c, "sparse", "OBJECT", "ENCODING", "hll")
}
