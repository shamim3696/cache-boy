package snapshot_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/datatype/deque"
	"github.com/shamim3696/cache-boy/internal/datatype/jsondoc"
	"github.com/shamim3696/cache-boy/internal/datatype/skiplist"
	"github.com/shamim3696/cache-boy/internal/datatype/timeseries"
	"github.com/shamim3696/cache-boy/internal/datatype/vector"
	"github.com/shamim3696/cache-boy/internal/persistence/snapshot"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/store"
)

// A module command that records what it was called with.
var (
	modMu   sync.Mutex
	modSeen [][]string
)

func init() {
	command.Register(&command.Spec{Name: "snaptest.mod", Arity: -1, Group: "test", Handler: func(x *command.Ctx) resp.Value {
		modMu.Lock()
		modSeen = append(modSeen, append([]string(nil), x.Args...))
		modMu.Unlock()
		return resp.OK
	}})
}

func newEngine(t *testing.T) *command.Engine {
	t.Helper()
	e := command.New(config.NewStore(config.Default()))
	t.Cleanup(e.Stop)
	return e
}

// fill puts one key of every kind into db (caller holds no lock; we take it).
func fill(t *testing.T, db *store.DB) {
	t.Helper()
	unlock := db.LockAll()
	defer unlock()
	future := time.Now().Add(time.Hour).UnixMilli()

	s := store.NewEntry(store.KindString, store.NewString([]byte("hello\x00world"), 0))
	s.ExpireAt = future
	db.Put("str", s)
	db.Put("bigstr", store.NewEntry(store.KindString, store.NewString(bytes.Repeat([]byte("abc"), 1000), 64)))

	l := deque.New()
	for _, v := range []string{"a", "b", "", "c"} {
		l.PushBack(v)
	}
	db.Put("list", store.NewEntry(store.KindList, l))

	h := store.NewHash()
	h.Set("f1", "v1")
	h.Set("f2", "")
	db.Put("hash", store.NewEntry(store.KindHash, h))

	st := store.NewSet()
	st.Add("x")
	st.Add("y")
	db.Put("set", store.NewEntry(store.KindSet, st))

	z := skiplist.New()
	z.Add("one", 1)
	z.Add("negzero", -0.5)
	z.Add("inf", skiplist.PosInf)
	db.Put("zset", store.NewEntry(store.KindZSet, z))

	doc, err := jsondoc.Parse(`{"a":[1,2,{"b":"c"}],"n":null}`)
	if err != nil {
		t.Fatal(err)
	}
	db.Put("json", store.NewEntry(store.KindJSON, doc))

	ts := timeseries.New(60000)
	ts.Labels["sensor"] = "t1"
	ts.Add(1000, 1.5)
	ts.Add(-5, 2) // negative timestamps survive zigzag encoding
	ts.Add(2000, 3)
	db.Put("ts", store.NewEntry(store.KindTimeSeries, ts))

	vx := vector.New()
	vx.Add("v1", []float32{1, 2, 3}, `{"tag":"a"}`)
	vx.Add("v2", []float32{0.5, -1, 0}, "")
	ve := store.NewEntry(store.KindVector, vx)
	ve.ExpireAt = future
	db.Put("vec", ve)

	expired := store.NewEntry(store.KindString, store.NewString([]byte("gone"), 0))
	expired.ExpireAt = time.Now().Add(-time.Second).UnixMilli()
	db.Put("expired", expired)
}

// dump renders every key as comparable text.
func dump(db *store.DB) map[string]string {
	unlock := db.LockAll()
	defer unlock()
	out := map[string]string{}
	db.ForEach(func(k string, e *store.Entry) bool {
		var parts []string
		switch e.Kind {
		case store.KindString:
			parts = []string{string(e.Str().Bytes())}
		case store.KindList:
			parts = e.List().Slice()
		case store.KindHash:
			for f, v := range e.Hash().Map() {
				parts = append(parts, f+"="+v)
			}
			sort.Strings(parts)
		case store.KindSet:
			parts = e.Set().Members()
			sort.Strings(parts)
		case store.KindZSet:
			e.ZSet().ForEach(func(p skiplist.Pair) bool {
				parts = append(parts, p.Member+"="+resp.FormatFloat(p.Score))
				return true
			})
		case store.KindJSON:
			parts = []string{e.JSON().String()}
		case store.KindTimeSeries:
			s := e.Series()
			parts = append(parts, "ret="+resp.FormatFloat(float64(s.Retention)))
			for k, v := range s.Labels {
				parts = append(parts, k+"="+v)
			}
			for _, smp := range s.Samples() {
				parts = append(parts, resp.FormatFloat(float64(smp.TS))+":"+resp.FormatFloat(smp.Value))
			}
		case store.KindVector:
			ids := e.Vectors().IDs()
			sort.Strings(ids)
			for _, id := range ids {
				v, attr, _ := e.Vectors().Get(id)
				parts = append(parts, id+"="+strings.Trim(strings.Join(strings.Fields(fmtFloats(v)), ","), "[]")+" "+attr)
			}
		}
		out[k] = e.Kind.String() + " ttl=" + boolStr(e.ExpireAt > 0) + " " + strings.Join(parts, "|")
		return true
	})
	return out
}

func fmtFloats(v []float32) string {
	var b strings.Builder
	for _, f := range v {
		b.WriteString(resp.FormatFloat(float64(f)) + " ")
	}
	return b.String()
}

func boolStr(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func encode(t *testing.T, e *command.Engine, modules [][]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	unlock := e.DB.LockAll()
	err := snapshot.EncodeDB(&buf, e.DB, func(emit func([]string)) {
		for _, m := range modules {
			emit(m)
		}
	})
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestRoundTripAllKinds(t *testing.T) {
	src := newEngine(t)
	fill(t, src.DB)
	want := dump(src.DB)
	if len(want) != 9 { // 10 keys minus the expired one
		t.Fatalf("fixture has %d live keys: %v", len(want), want)
	}
	modMu.Lock()
	modSeen = nil
	modMu.Unlock()
	data := encode(t, src, [][]string{{"snaptest.mod", "queue", "x y"}})
	if !bytes.HasPrefix(data, []byte(snapshot.Magic)) {
		t.Fatal("missing magic")
	}

	dst := newEngine(t)
	if err := snapshot.Load(bytes.NewReader(data), dst); err != nil {
		t.Fatal(err)
	}
	got := dump(dst.DB)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip mismatch\n got: %v\nwant: %v", got, want)
	}
	unlock := dst.DB.LockAll()
	if e := dst.DB.Peek("str"); e == nil || e.ExpireAt != src.DB.Peek("str").ExpireAt {
		t.Fatalf("TTL not preserved")
	}
	if e := dst.DB.Peek("expired"); e != nil {
		t.Fatalf("expired key was loaded")
	}
	unlock()
	modMu.Lock()
	defer modMu.Unlock()
	if !reflect.DeepEqual(modSeen, [][]string{{"snaptest.mod", "queue", "x y"}}) {
		t.Fatalf("module commands replayed: %v", modSeen)
	}
}

func TestEncodeWithEngine(t *testing.T) {
	e := newEngine(t)
	fill(t, e.DB)
	var buf bytes.Buffer
	unlock := e.DB.LockAll()
	err := snapshot.Encode(&buf, e)
	unlock()
	if err != nil {
		t.Fatal(err)
	}
	snap, _, err := snapshot.Bytes(e)
	if err != nil {
		t.Fatal(err)
	}
	// Sets and hashes iterate in random order, so compare decoded content.
	d1, err1 := snapshot.Decode(bytes.NewReader(buf.Bytes()), 0)
	d2, err2 := snapshot.Decode(bytes.NewReader(snap), 0)
	if err1 != nil || err2 != nil || len(d1.Keys) != len(d2.Keys) {
		t.Fatalf("decode: %v %v", err1, err2)
	}
}

func TestCorruption(t *testing.T) {
	e := newEngine(t)
	fill(t, e.DB)
	data := encode(t, e, nil)

	// Flip one bit in the middle: either a structural error or a checksum error.
	bad := append([]byte(nil), data...)
	bad[len(bad)/2] ^= 0x10
	if _, err := snapshot.Decode(bytes.NewReader(bad), 0); err == nil {
		t.Fatal("bit flip not detected")
	}

	// Flip a bit inside a string payload: structure stays valid, CRC catches it.
	bad = append([]byte(nil), data...)
	i := bytes.Index(bad, []byte("hello"))
	bad[i] = 'j'
	if _, err := snapshot.Decode(bytes.NewReader(bad), 0); !errors.Is(err, snapshot.ErrChecksum) {
		t.Fatalf("payload corruption: got %v, want checksum error", err)
	}

	// Truncated file.
	var ce *snapshot.CorruptError
	if _, err := snapshot.Decode(bytes.NewReader(data[:len(data)-20]), 0); !errors.As(err, &ce) {
		t.Fatalf("truncation: got %v", err)
	}
	if _, err := snapshot.Decode(bytes.NewReader(data[:len(data)-3]), 0); err == nil {
		t.Fatalf("truncated checksum not detected")
	}

	// Not a snapshot.
	if _, err := snapshot.Decode(strings.NewReader("*1\r\n$4\r\nPING\r\n"), 0); !errors.Is(err, snapshot.ErrBadMagic) {
		t.Fatalf("bad magic: got %v", err)
	}
	// Unknown version.
	bad = append([]byte(nil), data...)
	bad[len(snapshot.Magic)] = 99
	if _, err := snapshot.Decode(bytes.NewReader(bad), 0); err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("bad version: got %v", err)
	}

	// Nothing is applied from a corrupt snapshot.
	dst := newEngine(t)
	bad = append([]byte(nil), data...)
	bad[i] = 'j'
	if err := snapshot.Load(bytes.NewReader(bad), dst); err == nil || dst.DB.Len() != 0 {
		t.Fatalf("corrupt load: err=%v keys=%d", err, dst.DB.Len())
	}
}

func TestFileAtomic(t *testing.T) {
	e := newEngine(t)
	fill(t, e.DB)
	snap, _, err := snapshot.Bytes(e)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "sub", "dump.cbdump")
	if err := snapshot.WriteFileAtomic(path, snap); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.WriteFileAtomic(path, snap); err != nil { // overwrite
		t.Fatal(err)
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("temp files left behind: %v", entries)
	}
	dst := newEngine(t)
	if err := snapshot.LoadFile(path, dst); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(dump(dst.DB), dump(e.DB)) {
		t.Fatal("file round trip mismatch")
	}
	if err := snapshot.LoadFile(path+".missing", dst); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing file: %v", err)
	}
}
