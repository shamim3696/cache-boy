package dbsync

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestTemplate(t *testing.T) {
	row := Row{"id": TextValue("42"), "org": TextValue("acme"), "gone": NullValue(), "big": UnchangedValue(),
		"profile.city": TextValue("Dhaka")}
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"users:list", "users:list", true},
		{"user:{id}", "user:42", true},
		{"{org}:user:{id}:profile", "acme:user:42:profile", true},
		{"{id}", "42", true},
		{"city:{profile.city}", "city:Dhaka", true},
		{"user:{missing}", "", false},
		{"user:{gone}", "", false},
		{"doc:{big}", "", false},
	}
	for _, c := range cases {
		tp, err := parseTemplate(c.in)
		if err != nil {
			t.Fatalf("%q: %v", c.in, err)
		}
		got, ok := tp.render(row)
		if got != c.want || ok != c.ok {
			t.Errorf("%q → (%q, %v), want (%q, %v)", c.in, got, ok, c.want, c.ok)
		}
	}
	for _, bad := range []string{"", "user:{id", "user:id}", "user:{}", "a:{b c}", "}{"} {
		if _, err := parseTemplate(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestParseConfig(t *testing.T) {
	t.Setenv("PG_PASS", "s3cret")
	cfg, err := Parse([]byte(`
auto_invalidate: true
cache:
  addr: cache:6380
sources:
  - type: postgres
    url: postgres://app:${PG_PASS}@db:5432/shop
    rules:
      - table: users
        tags: ["user:{id}", "users:list"]
      - table: billing.orders
        tags: ["user:{user_id}"]
        keys: ["order:{id}"]
        on: [update, DELETE]
  - type: mysql
    url: mysql://u:p@db:3306/shop
    rules: [{table: products, tags: ["product:{id}"]}]
  - type: mongo
    name: docs
    url: mongodb://db:27017/?replicaSet=rs0
    database: cms
    rules: [{collection: posts, tags: ["post:{_id}"]}]
`))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.AutoInvalidate {
		t.Fatal("auto_invalidate not read")
	}
	pg := cfg.Sources[0]
	if pg.URL != "postgres://app:s3cret@db:5432/shop" || pg.Slot != "cache_boy" || pg.Publication != "cache_boy" || pg.Name != "postgres" {
		t.Fatalf("defaults/env not applied: %+v", pg)
	}
	if cfg.Sources[1].Name != "mysql" || cfg.Sources[1].ServerID == 0 {
		t.Fatalf("mysql defaults: %+v", cfg.Sources[1])
	}
	if cfg.Sources[2].Type != TypeMongoDB || cfg.Sources[2].Name != "docs" {
		t.Fatalf("mongo alias/name: %+v", cfg.Sources[2])
	}
	rules, _ := compileRules(&cfg.Sources[0], "public")
	if rules[0].name() != "public.users" || rules[1].name() != "billing.orders" {
		t.Fatalf("table names: %s, %s", rules[0].name(), rules[1].name())
	}
	if rules[1].on != opUpdate|opDelete || rules[0].on != opAll {
		t.Fatalf("on: %b %b", rules[0].on, rules[1].on)
	}
	if got := rules[1].columns(); !reflect.DeepEqual(got, []string{"user_id", "id"}) {
		t.Fatalf("columns: %v", got)
	}
	mr, _ := compileRules(&cfg.Sources[2], "cms")
	if mr[0].name() != "cms.posts" {
		t.Fatalf("mongo rule name %s", mr[0].name())
	}

	src := func(extra string) string {
		return "sources:\n  - type: postgres\n    url: x\n" + extra
	}
	bad := map[string]string{
		"no sources":     "auto_invalidate: true",
		"bad type":       "sources: [{type: oracle, url: x, rules: [{table: t, tags: [a]}]}]",
		"no url":         "sources: [{type: postgres, rules: [{table: t, tags: [a]}]}]",
		"no rules":       src(""),
		"no tags":        src("    rules: [{table: t}]"),
		"bad op":         src("    rules: [{table: t, tags: [a], on: [upsert]}]"),
		"bad tag":        src("    rules: [{table: t, tags: ['a:{id']}]"),
		"typo":           src("    rules: [{table: t, tag: [a]}]"),
		"bad slot":       src("    slot: My-Slot\n    rules: [{table: t, tags: [a]}]"),
		"no table":       src("    rules: [{tags: [a]}]"),
		"table and coll": src("    rules: [{table: t, collection: c, tags: [a]}]"),
		"dup names":      "sources: [{name: a, type: postgres, url: x, rules: [{table: t, tags: [a]}]}, {name: a, type: mysql, url: y, rules: [{table: t, tags: [a]}]}]",
	}
	for name, y := range bad {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestAutoInvalidateIsOffByDefault(t *testing.T) {
	cfg, err := Parse([]byte("sources: [{type: postgres, url: x, rules: [{table: t, tags: [a]}]}]"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AutoInvalidate {
		t.Fatal("auto_invalidate must default to false")
	}
	if _, err := New(cfg, nil, Options{}); !errors.Is(err, ErrDisabled) {
		t.Fatalf("New with auto_invalidate off: %v, want ErrDisabled", err)
	}
	cfg.AutoInvalidate = true
	if _, err := New(cfg, nil, Options{}); err != nil {
		t.Fatalf("New with auto_invalidate on: %v", err)
	}
}

func TestStateDirAndDefaults(t *testing.T) {
	cfg, _ := Parse([]byte(`auto_invalidate: true
sources:
  - {type: mysql, url: "u:p@tcp(h:3306)/db", rules: [{table: t, tags: [a]}]}
  - {type: mysql, url: "u:p@tcp(h:3306)/db", state_file: /abs/x.state, rules: [{table: t, tags: [a]}]}
`))
	dir := t.TempDir()
	if _, err := New(cfg, nil, Options{StateDir: dir}); err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "mysql-1.cbsync-state"); cfg.Sources[0].StateFile != want {
		t.Fatalf("state file %s, want %s", cfg.Sources[0].StateFile, want)
	}
	if cfg.Sources[1].StateFile != "/abs/x.state" && !filepath.IsAbs(cfg.Sources[1].StateFile) {
		t.Fatalf("absolute state file changed: %s", cfg.Sources[1].StateFile)
	}
}

func TestStateFile(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sub", "s.state")
	if b, err := loadState(p); err != nil || b != nil {
		t.Fatalf("missing file: %q %v", b, err)
	}
	if err := saveState(p, []byte("pos-1")); err != nil {
		t.Fatal(err)
	}
	if err := saveState(p, []byte("pos-2")); err != nil {
		t.Fatal(err)
	}
	if b, _ := loadState(p); string(b) != "pos-2" {
		t.Fatalf("got %q", b)
	}
	if err := removeState(p, log.New(&strings.Builder{}, "", 0)); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationURL(t *testing.T) {
	cases := map[string]string{
		"postgres://u:p@h:5432/db":               "postgres://u:p@h:5432/db?replication=database",
		"postgres://u@h/db?sslmode=disable":      "postgres://u@h/db?replication=database&sslmode=disable",
		"postgres://u@h/db?replication=database": "postgres://u@h/db?replication=database",
		"host=h user=u dbname=db":                "host=h user=u dbname=db replication=database",
	}
	for in, want := range cases {
		if got := replicationURL(in); got != want {
			t.Errorf("%s → %s, want %s", in, got, want)
		}
	}
}

// --- sink ---

type fakeInv struct {
	mu    sync.Mutex
	fails int
	calls [][]string
}

func (f *fakeInv) Invalidate(_ context.Context, tags, keys []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fails > 0 {
		f.fails--
		return errors.New("down")
	}
	f.calls = append(f.calls, append(append([]string{}, tags...), keys...))
	return nil
}

func TestSinkRetriesAndChunks(t *testing.T) {
	inv := &fakeInv{fails: 2}
	k := &sink{inv: inv, log: log.New(&strings.Builder{}, "", 0)}
	b := &batch{}
	for i := 0; i < chunkSize+10; i++ {
		b.tags = append(b.tags, "t"+strings.Repeat("x", i%3)+string(rune('a'+i%26)))
	}
	keepalives := 0
	if err := k.flush(context.Background(), "test", b, func() error { keepalives++; return nil }); err != nil {
		t.Fatal(err)
	}
	if keepalives == 0 {
		t.Fatal("keepalive not called while retrying")
	}
	if len(inv.calls) < 1 || k.batches.Load() != 1 {
		t.Fatalf("calls %d batches %d", len(inv.calls), k.batches.Load())
	}
}

// --- PostgreSQL processor ---

func tuple(vals ...any) *pglogrepl.TupleData {
	td := &pglogrepl.TupleData{ColumnNum: uint16(len(vals))}
	for _, v := range vals {
		switch v := v.(type) {
		case nil:
			td.Columns = append(td.Columns, &pglogrepl.TupleDataColumn{DataType: 'n'})
		case byte:
			td.Columns = append(td.Columns, &pglogrepl.TupleDataColumn{DataType: v}) // 'u'
		case string:
			td.Columns = append(td.Columns, &pglogrepl.TupleDataColumn{DataType: 't', Data: []byte(v)})
		}
	}
	return td
}

func relationMsg(id uint32, schema, table string, cols ...string) *pglogrepl.RelationMessage {
	m := &pglogrepl.RelationMessage{RelationID: id, Namespace: schema, RelationName: table, ColumnNum: uint16(len(cols))}
	for _, c := range cols {
		m.Columns = append(m.Columns, &pglogrepl.RelationMessageColumn{Name: c})
	}
	return m
}

func newTestProcessor(t *testing.T, rulesYAML string) (*pgProcessor, *[]string) {
	t.Helper()
	cfg, err := Parse([]byte("sources:\n  - type: postgres\n    url: x\n    rules:\n" + rulesYAML))
	if err != nil {
		t.Fatal(err)
	}
	rules, _ := compileRules(&cfg.Sources[0], "public")
	var warnings []string
	p := newPgProcessor(rules, func(f string, a ...any) { warnings = append(warnings, f) })
	return p, &warnings
}

func run(p *pgProcessor, msgs ...pglogrepl.Message) (*batch, pglogrepl.LSN) {
	p.handle(&pglogrepl.BeginMessage{})
	for _, m := range msgs {
		p.handle(m)
	}
	b, end := p.handle(&pglogrepl.CommitMessage{TransactionEndLSN: 100})
	sort.Strings(b.tags)
	sort.Strings(b.keys)
	return b, end
}

const orderRules = `      - table: orders
        tags: ["user:{user_id}", "order:{id}", "orders:list"]
        keys: ["order:{id}:summary"]
      - table: users
        tags: ["user:{id}"]
        on: [update, delete]
`

func TestProcessorInsertUpdateDelete(t *testing.T) {
	p, _ := newTestProcessor(t, orderRules)
	p.handle(relationMsg(1, "public", "orders", "id", "user_id", "note"))
	p.handle(relationMsg(2, "public", "users", "id", "name"))
	p.handle(relationMsg(3, "public", "audit", "id"))

	b, end := run(p, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("7", "42", "hi")})
	if want := []string{"order:7", "orders:list", "user:42"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("insert tags %v, want %v", b.tags, want)
	}
	if want := []string{"order:7:summary"}; !reflect.DeepEqual(b.keys, want) || end != 100 {
		t.Fatalf("insert keys %v end %v", b.keys, end)
	}

	// Order 7 moves from user 42 to user 43 (REPLICA IDENTITY FULL sends the
	// old row): both users' caches are stale.
	b, _ = run(p, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O',
		OldTuple: tuple("7", "42", "hi"), NewTuple: tuple("7", "43", "hi")})
	if want := []string{"order:7", "orders:list", "user:42", "user:43"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("update tags %v, want %v", b.tags, want)
	}

	// Default replica identity: a delete only carries the key; the user tag
	// can't be computed, the rest still is.
	b, _ = run(p, &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tuple("7", nil, nil)})
	if want := []string{"order:7", "orders:list"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("delete tags %v, want %v", b.tags, want)
	}

	b, _ = run(p, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple("5", "Ada")})
	if len(b.tags) != 0 || b.changes != 0 {
		t.Fatalf("insert into users should be ignored: %+v", b)
	}
	b, _ = run(p, &pglogrepl.InsertMessage{RelationID: 3, Tuple: tuple("1")})
	if len(b.tags)+len(b.keys) != 0 {
		t.Fatalf("audit should be ignored: %+v", b)
	}
}

func TestProcessorDedupAcrossTransaction(t *testing.T) {
	p, _ := newTestProcessor(t, orderRules)
	p.handle(relationMsg(1, "public", "orders", "id", "user_id", "note"))
	var msgs []pglogrepl.Message
	for i := 0; i < 100; i++ {
		msgs = append(msgs, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("1", "42", "x")})
	}
	b, _ := run(p, msgs...)
	if len(b.tags) != 3 || b.changes != 100 {
		t.Fatalf("expected 3 deduplicated tags from 100 changes, got %v (%d)", b.tags, b.changes)
	}
	b, _ = run(p)
	if len(b.tags) != 0 {
		t.Fatalf("state leaked into the next transaction: %v", b.tags)
	}
}

func TestProcessorUnchangedToast(t *testing.T) {
	p, _ := newTestProcessor(t, "      - {table: docs, tags: ['doc:{slug}']}\n")
	p.handle(relationMsg(1, "public", "docs", "id", "slug", "body"))
	b, _ := run(p, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O',
		OldTuple: tuple("1", "intro", "old"), NewTuple: tuple("1", byte('u'), "new")})
	if !reflect.DeepEqual(b.tags, []string{"doc:intro"}) {
		t.Fatalf("tags %v", b.tags)
	}
}

func TestProcessorTruncate(t *testing.T) {
	p, warnings := newTestProcessor(t, orderRules)
	p.handle(relationMsg(1, "public", "orders", "id", "user_id", "note"))
	b, _ := run(p, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{1}})
	if !reflect.DeepEqual(b.tags, []string{"orders:list"}) || len(b.keys) != 0 {
		t.Fatalf("truncate: tags %v keys %v", b.tags, b.keys)
	}
	if len(*warnings) != 1 || !strings.Contains((*warnings)[0], "truncated") {
		t.Fatalf("expected a truncate warning, got %v", *warnings)
	}
}

func TestProcessorRelationChange(t *testing.T) {
	p, _ := newTestProcessor(t, "      - {table: users, tags: ['user:{id}']}\n")
	p.handle(relationMsg(1, "public", "users", "name", "id"))
	b, _ := run(p, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("Ada", "9")})
	if !reflect.DeepEqual(b.tags, []string{"user:9"}) {
		t.Fatalf("tags %v", b.tags)
	}
	p.handle(relationMsg(1, "public", "users", "id", "email", "name"))
	b, _ = run(p, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("10", "a@x", "Bo")})
	if !reflect.DeepEqual(b.tags, []string{"user:10"}) {
		t.Fatalf("tags after ALTER %v", b.tags)
	}
}
