package dbsync

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestTemplate(t *testing.T) {
	row := map[string]value{"id": {text: "42"}, "org": {text: "acme"}, "gone": {null: true}, "big": {unchanged: true}}
	cases := []struct {
		in, want string
		ok       bool
	}{
		{"users:list", "users:list", true},
		{"user:{id}", "user:42", true},
		{"{org}:user:{id}:profile", "acme:user:42:profile", true},
		{"{id}", "42", true},
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
postgres:
  url: postgres://app:${PG_PASS}@db:5432/shop
cache:
  addr: cache:6380
rules:
  - table: users
    tags: ["user:{id}", "users:list"]
  - table: billing.orders
    tags: ["user:{user_id}"]
    keys: ["order:{id}"]
    on: [update, DELETE]
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Postgres.URL != "postgres://app:s3cret@db:5432/shop" || cfg.Postgres.Slot != "cache_boy" || cfg.Postgres.Publication != "cache_boy" {
		t.Fatalf("defaults/env not applied: %+v", cfg.Postgres)
	}
	rules, _ := cfg.compile()
	if rules[0].name() != "public.users" || rules[1].name() != "billing.orders" {
		t.Fatalf("table names: %s, %s", rules[0].name(), rules[1].name())
	}
	if rules[1].on != opUpdate|opDelete || rules[0].on != opAll {
		t.Fatalf("on: %b %b", rules[0].on, rules[1].on)
	}
	if got := rules[1].columns(); !reflect.DeepEqual(got, []string{"user_id", "id"}) {
		t.Fatalf("columns: %v", got)
	}

	bad := map[string]string{
		"no url":   "rules: [{table: t, tags: [a]}]",
		"no rules": "postgres: {url: x}",
		"no tags":  "postgres: {url: x}\nrules: [{table: t}]",
		"bad op":   "postgres: {url: x}\nrules: [{table: t, tags: [a], on: [upsert]}]",
		"bad tag":  "postgres: {url: x}\nrules: [{table: t, tags: ['a:{id']}]",
		"typo":     "postgres: {url: x}\nrules: [{table: t, tag: [a]}]",
		"bad slot": "postgres: {url: x, slot: My-Slot}\nrules: [{table: t, tags: [a]}]",
		"no table": "postgres: {url: x}\nrules: [{tags: [a]}]",
	}
	for name, y := range bad {
		if _, err := Parse([]byte(y)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
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

// --- processor ---

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

func newTestProcessor(t *testing.T, yaml string) (*processor, *[]string) {
	t.Helper()
	cfg, err := Parse([]byte("postgres: {url: x}\n" + yaml))
	if err != nil {
		t.Fatal(err)
	}
	rules, _ := cfg.compile()
	var warnings []string
	p := newProcessor(rules, func(f string, a ...any) { warnings = append(warnings, f) })
	return p, &warnings
}

func run(p *processor, msgs ...pglogrepl.Message) *batch {
	var b *batch
	p.handle(&pglogrepl.BeginMessage{})
	for _, m := range msgs {
		p.handle(m)
	}
	b = p.handle(&pglogrepl.CommitMessage{TransactionEndLSN: 100})
	sort.Strings(b.tags)
	sort.Strings(b.keys)
	return b
}

const orderRules = `rules:
  - table: orders
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

	b := run(p, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("7", "42", "hi")})
	if want := []string{"order:7", "orders:list", "user:42"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("insert tags %v, want %v", b.tags, want)
	}
	if want := []string{"order:7:summary"}; !reflect.DeepEqual(b.keys, want) || b.end != 100 {
		t.Fatalf("insert keys %v end %v", b.keys, b.end)
	}

	// Order 7 moves from user 42 to user 43 (REPLICA IDENTITY FULL sends the
	// old row): both users' caches are stale.
	b = run(p, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O',
		OldTuple: tuple("7", "42", "hi"), NewTuple: tuple("7", "43", "hi")})
	if want := []string{"order:7", "orders:list", "user:42", "user:43"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("update tags %v, want %v", b.tags, want)
	}

	// Default replica identity: a delete only carries the key; the user tag
	// can't be computed, the rest still is.
	b = run(p, &pglogrepl.DeleteMessage{RelationID: 1, OldTupleType: 'K', OldTuple: tuple("7", nil, nil)})
	if want := []string{"order:7", "orders:list"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("delete tags %v, want %v", b.tags, want)
	}

	// users rule ignores inserts.
	b = run(p, &pglogrepl.InsertMessage{RelationID: 2, Tuple: tuple("5", "Ada")})
	if len(b.tags) != 0 || b.changes != 0 {
		t.Fatalf("insert into users should be ignored: %+v", b)
	}
	// Tables without rules produce nothing.
	b = run(p, &pglogrepl.InsertMessage{RelationID: 3, Tuple: tuple("1")})
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
	b := run(p, msgs...)
	if len(b.tags) != 3 || b.changes != 100 {
		t.Fatalf("expected 3 deduplicated tags from 100 changes, got %v (%d)", b.tags, b.changes)
	}
	// The next transaction starts clean.
	b = run(p)
	if len(b.tags) != 0 {
		t.Fatalf("state leaked into the next transaction: %v", b.tags)
	}
}

func TestProcessorUnchangedToast(t *testing.T) {
	p, _ := newTestProcessor(t, "rules: [{table: docs, tags: ['doc:{slug}']}]")
	p.handle(relationMsg(1, "public", "docs", "id", "slug", "body"))
	// slug unchanged and not resent, but the old row (FULL) has it.
	b := run(p, &pglogrepl.UpdateMessage{RelationID: 1, OldTupleType: 'O',
		OldTuple: tuple("1", "intro", "old"), NewTuple: tuple("1", byte('u'), "new")})
	if !reflect.DeepEqual(b.tags, []string{"doc:intro"}) {
		t.Fatalf("tags %v", b.tags)
	}
}

func TestProcessorTruncate(t *testing.T) {
	p, warnings := newTestProcessor(t, orderRules)
	p.handle(relationMsg(1, "public", "orders", "id", "user_id", "note"))
	b := run(p, &pglogrepl.TruncateMessage{RelationNum: 1, RelationIDs: []uint32{1}})
	if !reflect.DeepEqual(b.tags, []string{"orders:list"}) || len(b.keys) != 0 {
		t.Fatalf("truncate: tags %v keys %v", b.tags, b.keys)
	}
	if len(*warnings) != 1 || !strings.Contains((*warnings)[0], "TRUNCATE") {
		t.Fatalf("expected a TRUNCATE warning, got %v", *warnings)
	}
}

func TestProcessorRelationChange(t *testing.T) {
	// ALTER TABLE resends the relation with new columns; rows must map by the
	// latest definition.
	p, _ := newTestProcessor(t, "rules: [{table: users, tags: ['user:{id}']}]")
	p.handle(relationMsg(1, "public", "users", "name", "id"))
	b := run(p, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("Ada", "9")})
	if !reflect.DeepEqual(b.tags, []string{"user:9"}) {
		t.Fatalf("tags %v", b.tags)
	}
	p.handle(relationMsg(1, "public", "users", "id", "email", "name"))
	b = run(p, &pglogrepl.InsertMessage{RelationID: 1, Tuple: tuple("10", "a@x", "Bo")})
	if !reflect.DeepEqual(b.tags, []string{"user:10"}) {
		t.Fatalf("tags after ALTER %v", b.tags)
	}
}
