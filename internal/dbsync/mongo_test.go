package dbsync

import (
	"log"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func mustRaw(t *testing.T, v any) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestMongoFlatten(t *testing.T) {
	oid, _ := bson.ObjectIDFromHex("65f0c0ffee00000000000001")
	dec, _ := bson.ParseDecimal128("19.90")
	when := time.Date(2024, 5, 1, 10, 0, 0, 250e6, time.FixedZone("x", 3600))
	uuid := bson.Binary{Subtype: 4, Data: []byte{0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0}}
	doc := mustRaw(t, bson.D{
		{Key: "_id", Value: oid},
		{Key: "name", Value: "Ada"},
		{Key: "i32", Value: int32(-7)},
		{Key: "i64", Value: int64(9007199254740993)},
		{Key: "whole", Value: 42.0},
		{Key: "frac", Value: 2.5},
		{Key: "dec", Value: dec},
		{Key: "ok", Value: true},
		{Key: "at", Value: bson.NewDateTimeFromTime(when)},
		{Key: "gone", Value: nil},
		{Key: "uuid", Value: uuid},
		{Key: "bin", Value: bson.Binary{Subtype: 0, Data: []byte("x")}},
		{Key: "profile", Value: bson.D{{Key: "city", Value: "Dhaka"}, {Key: "geo", Value: bson.D{{Key: "zip", Value: int32(1207)}}}}},
		{Key: "tags", Value: bson.A{"a", bson.D{{Key: "k", Value: "v"}}}},
	})
	row := flatten(doc)
	want := map[string]string{
		"_id":             "65f0c0ffee00000000000001",
		"name":            "Ada",
		"i32":             "-7",
		"i64":             "9007199254740993",
		"whole":           "42",
		"frac":            "2.5",
		"dec":             "19.90",
		"ok":              "true",
		"at":              "2024-05-01T09:00:00.25Z",
		"uuid":            "12345678-9abc-def0-1234-56789abcdef0",
		"profile.city":    "Dhaka",
		"profile.geo.zip": "1207",
		"tags.0":          "a",
		"tags.1.k":        "v",
	}
	for k, w := range want {
		if v, ok := row[k]; !ok || v.null || v.text != w {
			t.Errorf("%s = %+v, want %q", k, v, w)
		}
	}
	if !row["gone"].null {
		t.Errorf("null not NULL: %+v", row["gone"])
	}
	if _, ok := row["bin"]; ok {
		t.Error("generic binary should be left out")
	}
	if len(row) != len(want)+1 {
		t.Errorf("unexpected fields: %v", row)
	}
}

func TestFormatDouble(t *testing.T) {
	cases := map[float64]string{
		42: "42", -3: "-3", 2.5: "2.5", 0.1: "0.1", 1e20: "100000000000000000000", 1e21: "1e+21",
		1e-7: "1e-07", 0.000001: "0.000001", math.Copysign(0, -1): "0", math.Inf(1): "Infinity", math.NaN(): "NaN",
	}
	for in, want := range cases {
		if got := formatDouble(in); got != want {
			t.Errorf("%v → %q, want %q", in, got, want)
		}
	}
}

func TestMongoURLDatabase(t *testing.T) {
	cases := map[string]string{
		"mongodb://h:27017/shop":                        "shop",
		"mongodb://u:p@h1:1,h2:2/shop?replicaSet=rs0":   "shop",
		"mongodb+srv://u:p%40x@cluster.example.net/app": "app",
		"mongodb://h:27017/?replicaSet=rs0":             "",
		"mongodb://h:27017":                             "",
		"mongodb://h/my%20db":                           "my db",
	}
	for in, want := range cases {
		if got := mongoURLDatabase(in); got != want {
			t.Errorf("%s → %q, want %q", in, got, want)
		}
	}
}

func TestNewMongoDefaultDatabase(t *testing.T) {
	logger := log.New(&strings.Builder{}, "", 0)
	sc := func(url, db string, colls ...string) *SourceConfig {
		s := &SourceConfig{Name: "m", Type: TypeMongoDB, URL: url, Database: db}
		for _, c := range colls {
			s.Rules = append(s.Rules, RuleConfig{Collection: c, Tags: []string{"t:{_id}"}})
		}
		return s
	}
	s, err := newMongo(sc("mongodb://h/fromurl", "", "posts"), logger)
	if err != nil || s.rules[0].name() != "fromurl.posts" {
		t.Fatalf("url database: %v", err)
	}
	s, err = newMongo(sc("mongodb://h/fromurl", "cfg", "posts"), logger)
	if err != nil || s.rules[0].name() != "cfg.posts" {
		t.Fatalf("database setting should win: %v", err)
	}
	if _, err := newMongo(sc("mongodb://h/?replicaSet=rs0", "", "posts"), logger); err == nil || !strings.Contains(err.Error(), "database") {
		t.Fatalf("expected a missing-database error, got %v", err)
	}
	s, err = newMongo(sc("mongodb://h/", "", "a.posts", "b.users"), logger)
	if err != nil {
		t.Fatalf("qualified collections need no default database: %v", err)
	}
	if got := s.databases(); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("databases %v", got)
	}
}

func TestMongoFieldsBeyondID(t *testing.T) {
	s, err := newMongo(&SourceConfig{Name: "m", URL: "mongodb://h/shop", Rules: []RuleConfig{
		{Collection: "orders", Tags: []string{"user:{user_id}", "order:{_id}", "orders:list"}},
		{Collection: "users", Tags: []string{"user:{_id}", "org:{_id.org}"}},
		{Collection: "events", Tags: []string{"day:{day}"}, On: []string{"insert"}},
	}}, log.New(&strings.Builder{}, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	got := s.fieldsBeyondID()
	if want := map[string][]string{"shop.orders": {"user_id"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("fields %v, want %v", got, want)
	}
}

// --- events ---

func newTestMongoProcessor(t *testing.T, rulesYAML string) (*mongoProcessor, *[]string) {
	t.Helper()
	cfg, err := Parse([]byte("sources:\n  - type: mongodb\n    url: mongodb://h/shop\n    rules:\n" + rulesYAML))
	if err != nil {
		t.Fatal(err)
	}
	src, err := newMongo(&cfg.Sources[0], log.New(&strings.Builder{}, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	var warnings []string
	return newMongoProcessor(src.rules, func(f string, a ...any) { warnings = append(warnings, f) }), &warnings
}

type ev = bson.D

func event(optype, coll string, fields ...bson.E) bson.D {
	e := ev{
		{Key: "_id", Value: bson.D{{Key: "_data", Value: "82AB"}}},
		{Key: "operationType", Value: optype},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: coll}}},
	}
	return append(e, fields...)
}

func key(id any) bson.E { return bson.E{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}} }
func full(d bson.D) bson.E {
	return bson.E{Key: "fullDocument", Value: d}
}
func before(d bson.D) bson.E { return bson.E{Key: "fullDocumentBeforeChange", Value: d} }

func mrun(t *testing.T, p *mongoProcessor, events ...bson.D) *batch {
	t.Helper()
	for _, e := range events {
		if p.handle(mustRaw(t, e)) {
			t.Fatalf("unexpected end of stream on %v", e)
		}
	}
	b := p.c.take()
	sort.Strings(b.tags)
	sort.Strings(b.keys)
	return b
}

const mongoOrderRules = `      - collection: orders
        tags: ["user:{user_id}", "order:{_id}", "orders:list", "city:{ship.city}"]
        keys: ["order:{_id}:summary"]
      - collection: users
        tags: ["user:{_id}"]
        on: [update, delete]
`

func order(id, user int32, city string) bson.D {
	return bson.D{{Key: "_id", Value: id}, {Key: "user_id", Value: user}, {Key: "ship", Value: bson.D{{Key: "city", Value: city}}}}
}

func TestMongoInsertUpdateDelete(t *testing.T) {
	p, _ := newTestMongoProcessor(t, mongoOrderRules)

	b := mrun(t, p, event("insert", "orders", key(int32(7)), full(order(7, 42, "Dhaka"))))
	if want := []string{"city:Dhaka", "order:7", "orders:list", "user:42"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("insert tags %v, want %v", b.tags, want)
	}
	if want := []string{"order:7:summary"}; !reflect.DeepEqual(b.keys, want) {
		t.Fatalf("insert keys %v", b.keys)
	}

	// With pre-images, an order moving from user 42 to 43 invalidates both.
	b = mrun(t, p, event("update", "orders", key(int32(7)), full(order(7, 43, "Dhaka")), before(order(7, 42, "Dhaka")),
		bson.E{Key: "updateDescription", Value: bson.D{{Key: "updatedFields", Value: bson.D{{Key: "user_id", Value: int32(43)}}}}}))
	if want := []string{"city:Dhaka", "order:7", "orders:list", "user:42", "user:43"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("update tags %v, want %v", b.tags, want)
	}

	// No pre-image, and the document was deleted before the lookup: the key
	// and the changed fields (dotted, from updateDescription) still count.
	b = mrun(t, p, event("update", "orders", key(int32(7)), bson.E{Key: "fullDocument", Value: nil},
		bson.E{Key: "updateDescription", Value: bson.D{{Key: "updatedFields", Value: bson.D{{Key: "ship.city", Value: "Paris"}}}}}))
	if want := []string{"city:Paris", "order:7", "orders:list"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("update without document: tags %v, want %v", b.tags, want)
	}

	// The looked-up document may be newer than the change; the change's own
	// values win.
	b = mrun(t, p, event("update", "orders", key(int32(7)), full(order(7, 44, "Rome")),
		bson.E{Key: "updateDescription", Value: bson.D{{Key: "updatedFields", Value: bson.D{{Key: "user_id", Value: int32(43)}}}}}))
	if want := []string{"city:Rome", "order:7", "orders:list", "user:43"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("update with newer lookup: tags %v, want %v", b.tags, want)
	}

	// Replace is an update.
	b = mrun(t, p, event("replace", "orders", key(int32(7)), full(order(7, 50, "Oslo")), before(order(7, 43, "Rome"))))
	if want := []string{"city:Oslo", "city:Rome", "order:7", "orders:list", "user:43", "user:50"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("replace tags %v, want %v", b.tags, want)
	}

	// Delete without pre-image: only _id-based and static tags.
	b = mrun(t, p, event("delete", "orders", key(int32(7))))
	if want := []string{"order:7", "orders:list"}; !reflect.DeepEqual(b.tags, want) || len(b.keys) != 1 {
		t.Fatalf("delete tags %v keys %v, want %v", b.tags, b.keys, want)
	}
	// With pre-image: everything.
	b = mrun(t, p, event("delete", "orders", key(int32(7)), before(order(7, 50, "Oslo"))))
	if want := []string{"city:Oslo", "order:7", "orders:list", "user:50"}; !reflect.DeepEqual(b.tags, want) {
		t.Fatalf("delete with pre-image tags %v, want %v", b.tags, want)
	}

	// users rule ignores inserts; other collections are ignored.
	b = mrun(t, p, event("insert", "users", key("u1"), full(bson.D{{Key: "_id", Value: "u1"}})))
	if len(b.tags) != 0 || b.changes != 0 {
		t.Fatalf("insert into users should be ignored: %+v", b)
	}
	b = mrun(t, p, event("delete", "users", key("u1")))
	if !reflect.DeepEqual(b.tags, []string{"user:u1"}) {
		t.Fatalf("users delete tags %v", b.tags)
	}
	b = mrun(t, p, event("insert", "audit", key(int32(1)), full(order(1, 1, "x"))))
	if len(b.tags)+len(b.keys) != 0 {
		t.Fatalf("audit should be ignored: %+v", b)
	}
}

func TestMongoBatchDedup(t *testing.T) {
	p, _ := newTestMongoProcessor(t, mongoOrderRules)
	var events []bson.D
	for i := 0; i < 50; i++ {
		events = append(events, event("insert", "orders", key(int32(1)), full(order(1, 42, "Dhaka"))))
	}
	b := mrun(t, p, events...)
	if len(b.tags) != 4 || b.changes != 50 {
		t.Fatalf("expected 4 deduplicated tags from 50 changes, got %v (%d)", b.tags, b.changes)
	}
	if !p.c.empty() {
		t.Fatal("collector not reset by take")
	}
}

func TestMongoDropAndInvalidate(t *testing.T) {
	p, warnings := newTestMongoProcessor(t, mongoOrderRules)
	b := mrun(t, p, event("drop", "orders"))
	if !reflect.DeepEqual(b.tags, []string{"orders:list"}) || len(b.keys) != 0 {
		t.Fatalf("drop: tags %v keys %v", b.tags, b.keys)
	}
	if len(*warnings) != 1 || !strings.Contains((*warnings)[0], "dropped") {
		t.Fatalf("expected a drop warning, got %v", *warnings)
	}
	b = mrun(t, p, ev{{Key: "operationType", Value: "dropDatabase"}, {Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}}}})
	if !reflect.DeepEqual(b.tags, []string{"orders:list"}) {
		t.Fatalf("dropDatabase: tags %v", b.tags)
	}
	if !p.handle(mustRaw(t, ev{{Key: "operationType", Value: "invalidate"}})) {
		t.Fatal("invalidate should end the stream")
	}
}

func TestMongoMatchStage(t *testing.T) {
	s, err := newMongo(&SourceConfig{Name: "m", URL: "mongodb://h/shop", Rules: []RuleConfig{
		{Collection: "orders", Tags: []string{"a"}}, {Collection: "users", Tags: []string{"b"}},
		{Collection: "orders", Keys: []string{"c"}}, {Collection: "cms.posts", Tags: []string{"d"}},
	}}, log.New(&strings.Builder{}, "", 0))
	if err != nil {
		t.Fatal(err)
	}
	js, err := bson.MarshalExtJSON(s.matchStage(), false, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`{"ns.db":"cms","ns.coll":{"$in":["posts"]}`, `{"ns.db":"shop","ns.coll":{"$in":["orders","users"]}`, `"dropDatabase"`, `"invalidate"`} {
		if !strings.Contains(string(js), want) {
			t.Errorf("match stage %s lacks %s", js, want)
		}
	}
}
