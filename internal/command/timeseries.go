package command

import (
	"math"
	"sort"
	"strconv"

	"github.com/shamim3696/cache-boy/internal/datatype/timeseries"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

func init() {
	Register(
		&Spec{Name: "ts.create", Arity: -2, Flags: FlagWrite | FlagDenyOOM, Group: "timeseries", FirstKey: 1, LastKey: 1, Summary: "TS.CREATE key [RETENTION ms] [LABELS k v ...]", Handler: tsCreateCmd},
		&Spec{Name: "ts.add", Arity: -4, Flags: FlagWrite | FlagDenyOOM, Group: "timeseries", FirstKey: 1, LastKey: 1, Summary: "TS.ADD key timestamp|* value [RETENTION ms]", Handler: tsAddCmd},
		&Spec{Name: "ts.get", Arity: 2, Flags: FlagReadOnly, Group: "timeseries", FirstKey: 1, LastKey: 1, Summary: "Returns the newest sample", Handler: tsGetCmd},
		&Spec{Name: "ts.range", Arity: -4, Flags: FlagReadOnly, Group: "timeseries", FirstKey: 1, LastKey: 1, Summary: "TS.RANGE key from|- to|+ [AGGREGATION type bucket_ms]", Handler: tsRangeCmd},
		&Spec{Name: "ts.info", Arity: 2, Flags: FlagReadOnly, Group: "timeseries", FirstKey: 1, LastKey: 1, Summary: "Series metadata", Handler: tsInfoCmd},
	)
}

var (
	errTSNoKey     = resp.Err("ERR TSDB: the key does not exist")
	errTSTimestamp = resp.Err("ERR TSDB: invalid timestamp")
	errTSRetention = resp.Err("ERR TSDB: invalid retention")
)

// getSeries returns key's series, nil if missing, or false on a wrong type.
func getSeries(x *Ctx, key string) (*timeseries.Series, bool) {
	e, err := x.DB().GetKind(key, store.KindTimeSeries)
	if err != nil {
		return nil, false
	}
	if e == nil {
		return nil, true
	}
	return e.Series(), true
}

func parseRetention(x *Ctx, i int) (int64, bool) {
	n, ok := x.Int(i)
	return n, ok && n >= 0
}

func tsCreateCmd(x *Ctx) resp.Value {
	var retention int64
	labels := map[string]string{}
	for i := 2; i < x.NArgs(); i++ {
		switch x.ArgLower(i) {
		case "retention":
			var ok bool
			if retention, ok = parseRetention(x, i+1); !ok {
				return errTSRetention
			}
			i++
		case "labels":
			rest := x.Args[i+1:]
			if len(rest) == 0 || len(rest)%2 != 0 {
				return ErrSyntax
			}
			for j := 0; j < len(rest); j += 2 {
				labels[rest[j]] = rest[j+1]
			}
			i = x.NArgs()
		default:
			return ErrSyntax
		}
	}
	if x.DB().Get(x.Keys[0]) != nil {
		return resp.Err("ERR TSDB: key already exists")
	}
	s := timeseries.New(retention)
	s.Labels = labels
	x.DB().Put(x.Keys[0], store.NewEntry(store.KindTimeSeries, s))
	return resp.OK
}

func tsAddCmd(x *Ctx) resp.Value {
	key := x.Keys[0]
	var ts int64
	if x.Arg(2) == "*" {
		ts = x.Now()
	} else {
		var ok bool
		if ts, ok = x.Int(2); !ok || ts < 0 {
			return errTSTimestamp
		}
	}
	v, ok := x.Float(3)
	if !ok || math.IsInf(v, 0) {
		return resp.Err("ERR TSDB: invalid value")
	}
	retention := int64(-1)
	switch {
	case x.NArgs() == 4:
	case x.NArgs() == 6 && x.ArgLower(4) == "retention":
		if retention, ok = parseRetention(x, 5); !ok {
			return errTSRetention
		}
	default:
		return ErrSyntax
	}
	e := x.DB().Get(key)
	if e == nil {
		// RETENTION only applies when TS.ADD creates the series.
		e = store.NewEntry(store.KindTimeSeries, timeseries.New(max(retention, 0)))
		x.DB().Put(key, e)
	} else if e.Kind != store.KindTimeSeries {
		return ErrWrongTyp
	}
	e.Series().Add(ts, v)
	x.DB().Updated(key)

	tsArg := strconv.FormatInt(ts, 10)
	if retention >= 0 {
		x.Propagate("TS.ADD", key, tsArg, x.Arg(3), "RETENTION", strconv.FormatInt(retention, 10))
	} else {
		x.Propagate("TS.ADD", key, tsArg, x.Arg(3))
	}
	return resp.Int(ts)
}

func tsGetCmd(x *Ctx) resp.Value {
	s, ok := getSeries(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if s == nil {
		return errTSNoKey
	}
	p, ok := s.Latest()
	if !ok {
		return resp.Array()
	}
	return resp.Array(resp.Int(p.TS), resp.Double(p.Value))
}

// parseTSBound reads a range end: "-" and "+" are the oldest and newest
// possible timestamps.
func parseTSBound(s string) (int64, bool) {
	switch s {
	case "-":
		return math.MinInt64, true
	case "+":
		return math.MaxInt64, true
	}
	return ParseInt(s)
}

func tsRangeCmd(x *Ctx) resp.Value {
	from, ok1 := parseTSBound(x.Arg(2))
	to, ok2 := parseTSBound(x.Arg(3))
	if !ok1 || !ok2 {
		return errTSTimestamp
	}
	var agg string
	var bucket int64
	switch {
	case x.NArgs() == 4:
	case x.NArgs() == 7 && x.ArgLower(4) == "aggregation":
		agg = x.ArgLower(5)
		switch agg {
		case "avg", "sum", "min", "max", "count", "first", "last", "range":
		default: // checked here too: the series only notices when a bucket has samples
			return resp.Err(timeseries.ErrUnknownAgg.Error())
		}
		var ok bool
		if bucket, ok = x.Int(6); !ok || bucket <= 0 {
			return resp.Err("ERR TSDB: bucket size must be a positive integer")
		}
	default:
		return ErrSyntax
	}
	s, ok := getSeries(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if s == nil {
		return errTSNoKey
	}
	samples, err := s.Range(from, to, agg, bucket)
	if err != nil {
		return resp.Err(err.Error())
	}
	out := make([]resp.Value, len(samples))
	for i, p := range samples {
		out[i] = resp.Array(resp.Int(p.TS), resp.Double(p.Value))
	}
	return resp.Array(out...)
}

func tsInfoCmd(x *Ctx) resp.Value {
	s, ok := getSeries(x, x.Keys[0])
	if !ok {
		return ErrWrongTyp
	}
	if s == nil {
		return errTSNoKey
	}
	var first, last int64
	if samples := s.Samples(); len(samples) > 0 {
		first, last = samples[0].TS, samples[len(samples)-1].TS
	}
	names := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		names = append(names, k)
	}
	sort.Strings(names)
	labels := make([]resp.Value, 0, len(names)*2)
	for _, k := range names {
		labels = append(labels, resp.Bulk(k), resp.Bulk(s.Labels[k]))
	}
	return resp.Map(
		resp.Bulk("totalSamples"), resp.Int(int64(s.Len())),
		resp.Bulk("retentionTime"), resp.Int(s.Retention),
		resp.Bulk("firstTimestamp"), resp.Int(first),
		resp.Bulk("lastTimestamp"), resp.Int(last),
		resp.Bulk("labels"), resp.Map(labels...),
	)
}
