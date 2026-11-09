package dbsync

import (
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestParseMySQLURL(t *testing.T) {
	cases := []struct {
		in                     string
		user, pass, host, db   string
		port                   uint16
		tls, insecure, wantErr bool
	}{
		{in: "mysql://app:s%40cret@db.local:3307/shop?tls=true", user: "app", pass: "s@cret", host: "db.local", port: 3307, db: "shop", tls: true},
		{in: "mysql://root@localhost/shop", user: "root", host: "localhost", port: 3306, db: "shop"},
		{in: "mysql://u:p@[::1]:3306/", user: "u", pass: "p", host: "::1", port: 3306},
		{in: "app:p@ss@tcp(10.0.0.5:3306)/shop?parseTime=true&tls=skip-verify", user: "app", pass: "p@ss", host: "10.0.0.5", port: 3306, db: "shop", tls: true, insecure: true},
		{in: "app:pw@tcp(db)/shop", user: "app", pass: "pw", host: "db", port: 3306, db: "shop"},
		{in: "app@tcp([::1]:13306)/", user: "app", host: "::1", port: 13306},
		{in: "root:pw@/shop", user: "root", pass: "pw", host: "127.0.0.1", port: 3306, db: "shop"},
		{in: "/shop", host: "127.0.0.1", port: 3306, db: "shop"},
		{in: "root:pw@unix(/tmp/mysql.sock)/shop", wantErr: true},
		{in: "root:pw@db:3306/shop", wantErr: true},
		{in: "localhost", wantErr: true},
		{in: "mysql://u@h:99999/x", wantErr: true},
		{in: "mysql://u@h/x?tls=maybe", wantErr: true},
	}
	for _, c := range cases {
		d, err := parseMySQLURL(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("%s: expected an error, got %+v", c.in, d)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if d.user != c.user || d.pass != c.pass || d.host != c.host || d.port != c.port || d.db != c.db || (d.tls != nil) != c.tls {
			t.Errorf("%s: got %+v", c.in, d)
		}
		if d.tls != nil && d.tls.InsecureSkipVerify != c.insecure {
			t.Errorf("%s: InsecureSkipVerify = %v", c.in, d.tls.InsecureSkipVerify)
		}
	}
}

func mysqlTestConfig(t *testing.T, url, rulesYAML string) *SourceConfig {
	t.Helper()
	cfg, err := Parse([]byte("sources:\n  - type: mysql\n    url: " + url + "\n    rules:\n" + rulesYAML))
	if err != nil {
		t.Fatal(err)
	}
	return &cfg.Sources[0]
}

func TestNewMySQLDefaultDatabase(t *testing.T) {
	sc := mysqlTestConfig(t, "mysql://u:p@h/shop", "      - table: orders\n        tags: [\"o:{id}\"]\n      - table: crm.people\n        tags: [\"p:{id}\"]\n")
	s, err := newMySQL(sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.rules[0].name() != "shop.orders" || s.rules[1].name() != "crm.people" {
		t.Fatalf("rules %s, %s", s.rules[0].name(), s.rules[1].name())
	}
	if !s.watched["shop.orders"] || !s.watched["crm.people"] {
		t.Fatalf("watched %v", s.watched)
	}

	sc = mysqlTestConfig(t, "mysql://u:p@h:3306", "      - table: orders\n        tags: [\"o:{id}\"]\n")
	if _, err := newMySQL(sc, nil); err == nil || !strings.Contains(err.Error(), "no database") {
		t.Fatalf("want a no-database error, got %v", err)
	}
	sc = mysqlTestConfig(t, "u:p@tcp(h)/", "      - table: shop.orders\n        tags: [\"o:{id}\"]\n")
	if _, err := newMySQL(sc, nil); err != nil {
		t.Fatalf("qualified table without url database: %v", err)
	}
}

func TestMyText(t *testing.T) {
	plain := myColumn{name: "c"}
	cases := []struct {
		v    any
		c    myColumn
		want string
	}{
		{"abc", plain, "abc"},
		{[]byte("bin\x00"), plain, "bin\x00"},
		{int8(-5), plain, "-5"},
		{int16(300), plain, "300"},
		{int32(-70000), plain, "-70000"},
		{int64(9007199254740993), plain, "9007199254740993"},
		{uint8(255), plain, "255"},
		{uint32(4294967295), plain, "4294967295"},
		{uint64(18446744073709551615), plain, "18446744073709551615"},
		{1999, plain, "1999"}, // YEAR
		{float32(1.5), plain, "1.5"},
		{float32(0.1), plain, "0.1"},
		{float64(0.1), plain, "0.1"},
		{float64(100), plain, "100"},
		{float64(-2.25), plain, "-2.25"},
		{float64(1e21), plain, "1e+21"},
		{float64(1e-7), plain, "1e-07"},
		{float64(0), plain, "0"},
		{"12.50", plain, "12.50"}, // DECIMAL arrives as text
		{"2024-02-29 13:04:05.120", plain, "2024-02-29 13:04:05.120"},
		{`{"a": 1}`, plain, `{"a": 1}`},
		{int64(2), myColumn{enum: []string{"new", "paid"}}, "paid"},
		{int64(0), myColumn{enum: []string{"new", "paid"}}, ""},
		{int64(5), myColumn{set: []string{"a", "b", "c"}}, "a,c"},
		{int64(0), myColumn{set: []string{"a", "b", "c"}}, ""},
	}
	for _, c := range cases {
		got, ok := myText(c.v, c.c)
		if !ok || got != c.want {
			t.Errorf("myText(%#v) = %q, %v; want %q", c.v, got, ok, c.want)
		}
	}
	if _, ok := myText(&replication.JsonDiff{}, plain); ok {
		t.Error("a partial JSON update should not be renderable")
	}
}

func TestColumnFromType(t *testing.T) {
	c := columnFromType("st", "enum('new','it''s','a,b','back\\\\slash')")
	if want := []string{"new", "it's", "a,b", `back\slash`}; !reflect.DeepEqual(c.enum, want) {
		t.Fatalf("enum %q, want %q", c.enum, want)
	}
	c = columnFromType("f", "set('x','y')")
	if want := []string{"x", "y"}; !reflect.DeepEqual(c.set, want) {
		t.Fatalf("set %q", c.set)
	}
	if c := columnFromType("n", "int unsigned"); c.enum != nil || c.set != nil {
		t.Fatalf("int: %+v", c)
	}
}

func TestTruncateTarget(t *testing.T) {
	cases := []struct{ q, schema, table string }{
		{"TRUNCATE TABLE orders", "shop", "orders"},
		{"truncate orders", "shop", "orders"},
		{"TRUNCATE TABLE `crm`.`people`", "crm", "people"},
		{"TRUNCATE TABLE crm.people", "crm", "people"},
		{"TRUNCATE `we``ird`", "shop", "we`ird"},
	}
	for _, c := range cases {
		s, tb, ok := truncateTarget(trimSQL(c.q), "shop")
		if !ok || s != c.schema || tb != c.table {
			t.Errorf("%s: got %q %q %v", c.q, s, tb, ok)
		}
	}
	if _, _, ok := truncateTarget("TRUNCATE TABLE orders", ""); ok {
		t.Error("an unqualified table without a default database can't be resolved")
	}
	if got := trimSQL(" /* app */ /*!40000 x */ BEGIN; "); got != "BEGIN" {
		t.Errorf("trimSQL = %q", got)
	}
}

// --- processor ---

const myRules = `      - table: orders
        tags: ["order:{id}", "user:{user_id}", "orders:list"]
        keys: ["order:{id}:summary"]
      - table: users
        tags: ["user:{id}"]
        on: [update, delete]
      - table: tickets
        tags: ["tickets:{status}"]
`

type myTest struct {
	p       *myProcessor
	lookups []string
	schema  map[string][]myColumn
	warns   []string
}

func newMyTest(t *testing.T) *myTest {
	t.Helper()
	sc := mysqlTestConfig(t, "mysql://u:p@h/shop", myRules)
	s, err := newMySQL(sc, nil)
	if err != nil {
		t.Fatal(err)
	}
	mt := &myTest{schema: map[string][]myColumn{
		"shop.orders":  cols("id", "user_id", "note"),
		"shop.users":   cols("id", "name"),
		"shop.tickets": {{name: "id"}, {name: "status", enum: []string{"open", "closed"}}},
	}}
	lookup := func(schema, table string) ([]myColumn, error) {
		mt.lookups = append(mt.lookups, schema+"."+table)
		if schema == "down" {
			return nil, errors.New("connection lost")
		}
		return mt.schema[schema+"."+table], nil
	}
	mt.p = newMyProcessor(s.rules, lookup, func(f string, a ...any) { mt.warns = append(mt.warns, f) })
	mt.p.file = "binlog.000001"
	return mt
}

func cols(names ...string) []myColumn {
	var out []myColumn
	for _, n := range names {
		out = append(out, myColumn{name: n})
	}
	return out
}

func myEv(et replication.EventType, logPos uint32, e replication.Event) *replication.BinlogEvent {
	return &replication.BinlogEvent{Header: &replication.EventHeader{EventType: et, LogPos: logPos}, Event: e}
}

func tableMap(id uint64, schema, table string, n int, names ...string) *replication.BinlogEvent {
	tm := &replication.TableMapEvent{TableID: id, Schema: []byte(schema), Table: []byte(table), ColumnCount: uint64(n)}
	for _, name := range names {
		tm.ColumnName = append(tm.ColumnName, []byte(name))
	}
	return myEv(replication.TABLE_MAP_EVENT, 0, tm)
}

func rowsEv(et replication.EventType, id uint64, rows ...[]any) *replication.BinlogEvent {
	return myEv(et, 0, &replication.RowsEvent{TableID: id, Rows: rows})
}

func query(q string, logPos uint32) *replication.BinlogEvent {
	return myEv(replication.QUERY_EVENT, logPos, &replication.QueryEvent{Schema: []byte("shop"), Query: []byte(q)})
}

func xid(logPos uint32) *replication.BinlogEvent {
	return myEv(replication.XID_EVENT, logPos, &replication.XIDEvent{})
}

// tx runs BEGIN, the events and COMMIT (XID) and returns the batch.
func (mt *myTest) tx(t *testing.T, events ...*replication.BinlogEvent) (*batch, *myPos) {
	t.Helper()
	all := append([]*replication.BinlogEvent{query("BEGIN", 0)}, events...)
	all = append(all, xid(500))
	return mt.run(t, all...)
}

func (mt *myTest) run(t *testing.T, events ...*replication.BinlogEvent) (b *batch, pos *myPos) {
	t.Helper()
	for i, e := range events {
		eb, epos, err := mt.p.handle(e)
		if err != nil {
			t.Fatal(err)
		}
		if epos != nil && i != len(events)-1 {
			if _, ok := e.Event.(*replication.RotateEvent); !ok {
				t.Fatalf("event %d (%T) ended the transaction early", i, e.Event)
			}
		}
		b, pos = eb, epos
	}
	if b == nil {
		b = &batch{}
	}
	sort.Strings(b.tags)
	sort.Strings(b.keys)
	return b, pos
}

func TestMyProcessorInsertUpdateDelete(t *testing.T) {
	mt := newMyTest(t)
	b, pos := mt.tx(t, tableMap(10, "shop", "orders", 3), rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(7), int32(42), "hi"}))
	if want := []string{"order:7", "orders:list", "user:42"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("insert tags %v, want %v", b.tags, want)
	}
	if want := []string{"order:7:summary"}; !reflect.DeepEqual(b.keys, want) {
		t.Fatalf("insert keys %v", b.keys)
	}
	if pos == nil || *pos != (myPos{File: "binlog.000001", Pos: 500}) {
		t.Fatalf("position %+v", pos)
	}

	// Order 7 moves from user 42 to user 43: both users' caches are stale.
	b, _ = mt.tx(t, tableMap(10, "shop", "orders", 3),
		rowsEv(replication.UPDATE_ROWS_EVENTv2, 10, []any{int32(7), int32(42), "hi"}, []any{int32(7), int32(43), "hi"}))
	if want := []string{"order:7", "orders:list", "user:42", "user:43"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("update tags %v, want %v", b.tags, want)
	}

	b, _ = mt.tx(t, tableMap(10, "shop", "orders", 3), rowsEv(replication.DELETE_ROWS_EVENTv2, 10, []any{int32(7), int32(43), nil}))
	if want := []string{"order:7", "orders:list", "user:43"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("delete tags %v, want %v", b.tags, want)
	}

	// users rules are update/delete only; inserts are ignored. Table names
	// match case-insensitively.
	b, _ = mt.tx(t, tableMap(11, "SHOP", "Users", 2), rowsEv(replication.WRITE_ROWS_EVENTv2, 11, []any{int32(5), "Ada"}))
	if len(b.tags) != 0 || b.changes != 0 {
		t.Fatalf("insert into users should be ignored: %+v", b)
	}
	// Tables without rules are ignored without a lookup.
	b, _ = mt.tx(t, tableMap(12, "shop", "audit", 1), rowsEv(replication.WRITE_ROWS_EVENTv2, 12, []any{int32(1)}))
	if len(b.tags)+len(b.keys) != 0 {
		t.Fatalf("audit should be ignored: %+v", b)
	}
	if want := []string{"shop.orders", "SHOP.Users"}; !reflect.DeepEqual(mt.lookups, want) {
		t.Fatalf("lookups %v, want %v (columns are cached per table)", mt.lookups, want)
	}
}

func TestMyProcessorManyRowsOneBatch(t *testing.T) {
	mt := newMyTest(t)
	events := []*replication.BinlogEvent{tableMap(10, "shop", "orders", 3)}
	var rows [][]any
	for i := range 50 {
		rows = append(rows, []any{int32(i), int32(1), "x"})
	}
	events = append(events, rowsEv(replication.WRITE_ROWS_EVENTv2, 10, rows...))
	// Several statements in one transaction.
	events = append(events, tableMap(11, "shop", "users", 2),
		rowsEv(replication.UPDATE_ROWS_EVENTv2, 11, []any{int32(1), "a"}, []any{int32(1), "b"}, []any{int32(2), "c"}, []any{int32(2), "d"}))
	b, _ := mt.tx(t, events...)
	// order:0..49, orders:list, user:1 (shared), user:2
	if len(b.tags) != 50+1+1+1 || b.changes != 52 {
		t.Fatalf("got %d tags, %d changes", len(b.tags), b.changes)
	}
}

func TestMyProcessorColumnNamesFromBinlog(t *testing.T) {
	mt := newMyTest(t)
	// binlog_row_metadata = FULL: names come with the event, no lookup.
	// Columns were reordered since the cached information_schema read.
	b, _ := mt.tx(t, tableMap(10, "shop", "orders", 3, "user_id", "id", "note"),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(42), int32(7), "hi"}))
	if want := []string{"order:7", "orders:list", "user:42"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("tags %v, want %v", b.tags, want)
	}
	if len(mt.lookups) != 0 {
		t.Fatalf("unexpected lookups %v", mt.lookups)
	}
	// ENUM members from the event metadata.
	tm := tableMap(20, "shop", "tickets", 2, "id", "status")
	tm.Event.(*replication.TableMapEvent).ColumnType = []byte{mysql.MYSQL_TYPE_LONG, mysql.MYSQL_TYPE_STRING}
	tm.Event.(*replication.TableMapEvent).ColumnMeta = []uint16{0, uint16(mysql.MYSQL_TYPE_ENUM)<<8 | 1}
	tm.Event.(*replication.TableMapEvent).EnumStrValue = [][][]byte{{[]byte("open"), []byte("closed")}}
	b, _ = mt.tx(t, tm, rowsEv(replication.UPDATE_ROWS_EVENTv2, 20, []any{int32(1), int64(1)}, []any{int32(1), int64(2)}))
	if want := []string{"tickets:closed", "tickets:open"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("enum tags %v, want %v", b.tags, want)
	}
}

func TestMyProcessorEnumFromInformationSchema(t *testing.T) {
	mt := newMyTest(t)
	b, _ := mt.tx(t, tableMap(20, "shop", "tickets", 2), rowsEv(replication.DELETE_ROWS_EVENTv2, 20, []any{int32(1), int64(2)}))
	if want := []string{"tickets:closed"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("tags %v, want %v", b.tags, want)
	}
}

func TestMyProcessorMinimalRowImage(t *testing.T) {
	mt := newMyTest(t)
	// binlog_row_image = MINIMAL: the before image has the key, the after
	// image only the changed column. Missing columns are "not sent", not NULL.
	e := rowsEv(replication.UPDATE_ROWS_EVENTv2, 10, []any{int32(7), nil, nil}, []any{nil, nil, "new note"})
	e.Event.(*replication.RowsEvent).SkippedColumns = [][]int{{1, 2}, {0, 1}}
	b, _ := mt.tx(t, tableMap(10, "shop", "orders", 3), e)
	// order:7 from the old key (filled into the new row too); user_id is
	// unknown, so no user tag (and no "user:" with an empty id).
	if want := []string{"order:7", "orders:list"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("tags %v, want %v", b.tags, want)
	}
}

func TestMyProcessorRollbackAndSavepoints(t *testing.T) {
	mt := newMyTest(t)
	// A rolled-back transaction that touched a non-transactional table is
	// logged with ROLLBACK: nothing it did counts.
	b, pos := mt.run(t, query("BEGIN", 0), tableMap(10, "shop", "orders", 3),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(1), int32(2), "x"}), query("ROLLBACK", 300))
	if len(b.tags)+len(b.keys) != 0 || pos == nil || pos.Pos != 300 {
		t.Fatalf("rollback: %+v at %+v", b, pos)
	}
	// SAVEPOINT / ROLLBACK TO inside a transaction don't end it.
	b, _ = mt.tx(t, tableMap(10, "shop", "orders", 3), query("SAVEPOINT `a`", 0),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(1), int32(2), "x"}), query("ROLLBACK TO `a`", 0),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(3), int32(2), "x"}))
	if want := []string{"order:1", "order:3", "orders:list", "user:2"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("savepoint tags %v, want %v", b.tags, want)
	}
	// A non-transactional table commits with a COMMIT query.
	b, pos = mt.run(t, query("BEGIN", 0), tableMap(10, "shop", "orders", 3),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(9), int32(2), "x"}), query("COMMIT", 700))
	if want := []string{"order:9", "orders:list", "user:2"}; !reflect.DeepEqual(b.tags, want) || pos.Pos != 700 {
		t.Fatalf("COMMIT query: %v at %+v", b.tags, pos)
	}
}

func TestMyProcessorTruncateAndDDL(t *testing.T) {
	mt := newMyTest(t)
	b, pos := mt.run(t, query("TRUNCATE TABLE `orders`", 900))
	if want := []string{"orders:list"}; !reflect.DeepEqual(b.tags, want) || pos.Pos != 900 {
		t.Fatalf("truncate: %v at %+v", b.tags, pos)
	}
	if len(mt.warns) != 1 {
		t.Fatalf("want a warning about placeholder tags, got %v", mt.warns)
	}
	b, _ = mt.run(t, query("truncate table other.orders", 950))
	if len(b.tags) != 0 {
		t.Fatalf("other.orders isn't watched: %v", b.tags)
	}

	// Columns are cached until DDL.
	mt.tx(t, tableMap(10, "shop", "orders", 3), rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(1), int32(2), "x"}))
	mt.schema["shop.orders"] = cols("id", "note", "user_id", "extra")
	if _, pos := mt.run(t, query("/* app */ ALTER TABLE orders ADD extra int", 1000)); pos == nil {
		t.Fatal("DDL should end its transaction")
	}
	b, _ = mt.tx(t, tableMap(15, "shop", "orders", 4), rowsEv(replication.WRITE_ROWS_EVENTv2, 15, []any{int32(1), "x", int32(5), nil}))
	if want := []string{"order:1", "orders:list", "user:5"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("after ALTER: %v, want %v", b.tags, want)
	}
	if n := len(mt.lookups); n != 2 {
		t.Fatalf("lookups %v", mt.lookups)
	}
}

func TestMyProcessorColumnMismatch(t *testing.T) {
	mt := newMyTest(t)
	// An old event (2 columns) replayed after the table gained a column:
	// names can't be trusted, so only static tags are invalidated.
	b, _ := mt.tx(t, tableMap(10, "shop", "orders", 2), rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(1), int32(2)}))
	if want := []string{"orders:list"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("tags %v, want %v", b.tags, want)
	}
	if len(mt.warns) != 1 || len(mt.lookups) != 1 {
		t.Fatalf("warns %v lookups %v", mt.warns, mt.lookups)
	}
}

func TestMyProcessorLookupError(t *testing.T) {
	mt := newMyTest(t)
	mt.p.rules[myKey("down", "t")] = mt.p.rules[myKey("shop", "orders")]
	if _, _, err := mt.p.handle(tableMap(10, "down", "t", 3)); err == nil {
		t.Fatal("a failed column lookup must end the session")
	}
}

func TestMyProcessorPositions(t *testing.T) {
	mt := newMyTest(t)
	// The fake rotate at stream start and a real one between transactions
	// both move the position.
	_, pos := mt.run(t, myEv(replication.ROTATE_EVENT, 0, &replication.RotateEvent{Position: 4, NextLogName: []byte("binlog.000002")}))
	if pos == nil || *pos != (myPos{File: "binlog.000002", Pos: 4}) {
		t.Fatalf("rotate: %+v", pos)
	}
	gset, err := mysql.ParseMysqlGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-23")
	if err != nil {
		t.Fatal(err)
	}
	x := xid(1234)
	x.Event.(*replication.XIDEvent).GSet = gset
	_, pos = mt.run(t, query("BEGIN", 0), tableMap(10, "shop", "orders", 3),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(1), int32(2), "x"}), x)
	if want := (myPos{File: "binlog.000002", Pos: 1234, GTID: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-23"}); *pos != want {
		t.Fatalf("position %+v, want %+v", *pos, want)
	}
	// A transaction on an unwatched table still advances the position, with
	// nothing to invalidate.
	b, pos := mt.tx(t, tableMap(12, "shop", "audit", 1), rowsEv(replication.WRITE_ROWS_EVENTv2, 12, []any{int32(1)}))
	if pos == nil || pos.Pos != 500 || pos.GTID == "" || len(b.tags) != 0 {
		t.Fatalf("unwatched: %+v %+v", b, pos)
	}
}

func TestMyProcessorCompressedTransaction(t *testing.T) {
	mt := newMyTest(t)
	payload := &replication.TransactionPayloadEvent{Events: []*replication.BinlogEvent{
		query("BEGIN", 0),
		tableMap(10, "shop", "orders", 3),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(8), int32(3), "x"}),
		xid(0),
	}}
	b, pos := mt.run(t, myEv(replication.TRANSACTION_PAYLOAD_EVENT, 2000, payload))
	if want := []string{"order:8", "orders:list", "user:3"}; !reflect.DeepEqual(b.tags, want) || pos == nil || pos.Pos != 2000 {
		t.Fatalf("payload: %v at %+v", b.tags, pos)
	}
}

func TestMyProcessorXA(t *testing.T) {
	mt := newMyTest(t)
	b, pos := mt.run(t, query("XA START X'01',X'',1", 0), tableMap(10, "shop", "orders", 3),
		rowsEv(replication.WRITE_ROWS_EVENTv2, 10, []any{int32(4), int32(2), "x"}), query("XA END X'01',X'',1", 0),
		myEv(replication.XA_PREPARE_LOG_EVENT, 1500, &replication.GenericEvent{}))
	if want := []string{"order:4", "orders:list", "user:2"}; !reflect.DeepEqual(b.tags, want) || pos.Pos != 1500 {
		t.Fatalf("XA: %v at %+v", b.tags, pos)
	}
	b, pos = mt.run(t, query("XA COMMIT X'01',X'',1", 1600))
	if len(b.tags) != 0 || pos.Pos != 1600 {
		t.Fatalf("XA COMMIT: %v at %+v", b.tags, pos)
	}
}
