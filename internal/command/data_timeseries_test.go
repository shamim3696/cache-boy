package command

import (
	"strconv"
	"testing"
	"time"
)

func TestTimeSeriesCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"TS.CREATE t RETENTION 1000 LABELS sensor 1 room kitchen", "OK"},
		{"TS.CREATE t", "!ERR TSDB: key already exists"},
		{"TS.CREATE t2 LABELS a", "!ERR syntax error"},
		{"TS.CREATE t2 RETENTION -1", "!ERR TSDB: invalid retention"},
		{"TS.CREATE t2 BOGUS", "!ERR syntax error"},
		{"TS.GET t", "[]"},
		{"TS.INFO t", "{totalSamples:0 retentionTime:1000 firstTimestamp:0 lastTimestamp:0 labels:{room:kitchen sensor:1}}"},
		{"TS.ADD t 1000 1.5", "1000"},
		{"TS.ADD t 2000 2.5", "2000"},
		{"TS.ADD t 1500 3", "1500"},
		{"TS.ADD t 1500 4", "1500"},
		{"TS.GET t", "[2000 2.5]"},
		{"TS.RANGE t - +", "[[1000 1.5] [1500 4] [2000 2.5]]"},
		{"TS.RANGE t 1200 +", "[[1500 4] [2000 2.5]]"},
		{"TS.RANGE t - 1500", "[[1000 1.5] [1500 4]]"},
		{"TS.RANGE t 3000 4000", "[]"},
		{"TS.RANGE t - + AGGREGATION avg 1000", "[[1000 2.75] [2000 2.5]]"},
		{"TS.RANGE t - + AGGREGATION SUM 1000", "[[1000 5.5] [2000 2.5]]"},
		{"TS.RANGE t - + AGGREGATION count 10000", "[[0 3]]"},
		{"TS.RANGE t - + AGGREGATION min 10000", "[[0 1.5]]"},
		{"TS.RANGE t - + AGGREGATION max 10000", "[[0 4]]"},
		{"TS.RANGE t - + AGGREGATION range 10000", "[[0 2.5]]"},
		{"TS.RANGE t - + AGGREGATION first 10000", "[[0 1.5]]"},
		{"TS.RANGE t - + AGGREGATION last 10000", "[[0 2.5]]"},
		{"TS.RANGE t - + AGGREGATION bogus 10", "!ERR unknown aggregation"},
		{"TS.RANGE t 5000 6000 AGGREGATION bogus 10", "!ERR unknown aggregation"},
		{"TS.RANGE t - + AGGREGATION avg 0", "!ERR TSDB: bucket size"},
		{"TS.RANGE t - + AGGREGATION avg", "!ERR syntax error"},
		{"TS.RANGE t x +", "!ERR TSDB: invalid timestamp"},
		{"TS.ADD t 3500 1", "3500"},
		{"TS.RANGE t - +", "[[3500 1]]"},
		{"TS.INFO t", "{totalSamples:1 retentionTime:1000 firstTimestamp:3500 lastTimestamp:3500 labels:{room:kitchen sensor:1}}"},

		{"TS.ADD r 1 1 RETENTION 10", "1"},
		{"TS.ADD r 100 2", "100"},
		{"TS.RANGE r - +", "[[100 2]]"},
		{"TS.ADD x abc 1", "!ERR TSDB: invalid timestamp"},
		{"TS.ADD x -5 1", "!ERR TSDB: invalid timestamp"},
		{"TS.ADD x 1 abc", "!ERR TSDB: invalid value"},
		{"TS.ADD x 1 1 RETENTION", "!ERR syntax error"},
		{"EXISTS x", "0"},
		{"TS.GET nope", "!ERR TSDB: the key does not exist"},
		{"TS.RANGE nope - +", "!ERR TSDB: the key does not exist"},
		{"TS.INFO nope", "!ERR TSDB: the key does not exist"},
		{"SET str v", "OK"},
		{"TS.ADD str 1 1", "!WRONGTYPE"},
		{"TS.GET str", "!WRONGTYPE"},
		{"TS.CREATE str", "!ERR TSDB: key already exists"},
		{"TYPE t", "timeseries"},
	})
}

func TestTimeSeriesPropagation(t *testing.T) {
	e, c := newDataEngine(t)
	rec := &recorder{}
	e.AddPropagator(rec)
	before := time.Now().UnixMilli()
	v := do(e, c, "TS.ADD", "t", "*", "1.5", "RETENTION", "60000")
	if v.IsError() || v.Int < before {
		t.Fatalf("TS.ADD *: %s", render(v))
	}
	want := "TS.ADD t " + strconv.FormatInt(v.Int, 10) + " 1.5 RETENTION 60000"
	if got := rec.take(); len(got) != 1 || got[0] != want {
		t.Fatalf("propagated %q, want %q", got, want)
	}
	if got := render(do(e, c, "TS.GET", "t")); got != "["+strconv.FormatInt(v.Int, 10)+" 1.5]" {
		t.Fatalf("TS.GET: %s", got)
	}
}
