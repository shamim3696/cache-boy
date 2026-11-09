package dbsync

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoSource streams MongoDB change events. The stream's resume token is
// saved to the state file after each batch is invalidated, so a restart
// resumes where it stopped (as long as the oplog still has that position).
type mongoSource struct {
	cfg   *SourceConfig
	rules []*rule
	log   *log.Logger
}

func newMongo(sc *SourceConfig, logger *log.Logger) (*mongoSource, error) {
	db := sc.Database
	if db == "" {
		db = mongoURLDatabase(sc.URL)
	}
	rules, err := compileRules(sc, db)
	if err != nil {
		return nil, err
	}
	if db == "" {
		for _, r := range rules {
			if r.schema == "" {
				return nil, fmt.Errorf("config: source %q: no database for collection %q: set database: in the source or put it in the url (mongodb://host:27017/<database>)", sc.Name, r.table)
			}
		}
	}
	return &mongoSource{cfg: sc, rules: rules, log: logger}, nil
}

// mongoURLDatabase returns the database in a mongodb:// URL's path ("" if
// none). It doesn't use the driver's parser, which resolves mongodb+srv
// hosts over DNS.
func mongoURLDatabase(raw string) string {
	_, rest, ok := strings.Cut(raw, "://")
	if !ok {
		return ""
	}
	rest, _, _ = strings.Cut(rest, "?")
	if i := strings.LastIndexByte(rest, '@'); i >= 0 {
		rest = rest[i+1:]
	}
	_, db, ok := strings.Cut(rest, "/")
	if !ok {
		return ""
	}
	if s, err := url.PathUnescape(db); err == nil {
		db = s
	}
	return db
}

func (s *mongoSource) name() string { return s.cfg.Name }

const (
	mongoMaxBatch     = 500              // events invalidated together at most
	mongoIdleSave     = 30 * time.Second // how often an idle stream's position is saved
	mongoSetupTimeout = 30 * time.Second
	mongoWire60       = 17 // maxWireVersion of MongoDB 6.0: pre-images
)

func (s *mongoSource) session(ctx context.Context, k *sink) error {
	client, err := mongo.Connect(options.Client().ApplyURI(s.cfg.URL).SetAppName("cache-boy-sync"))
	if err != nil {
		return err
	}
	defer client.Disconnect(context.Background())

	sctx, cancel := context.WithTimeout(ctx, mongoSetupTimeout)
	wire, err := s.checkServer(sctx, client)
	if err == nil && wire >= mongoWire60 {
		s.checkPreImages(sctx, client)
	} else if err == nil && s.needsPreImages() {
		s.log.Printf("%s: warning: MongoDB before 6.0 has no pre-images: deletes (and updates that move a document) can only invalidate tags built from _id", s.name())
	}
	cancel()
	if err != nil {
		return err
	}

	opts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if wire >= mongoWire60 {
		opts.SetFullDocumentBeforeChange(options.WhenAvailable)
	}
	state, err := loadState(s.cfg.StateFile)
	if err != nil {
		return fmt.Errorf("read state file: %w", err)
	}
	var saved bson.Raw
	if state != nil {
		if err := bson.UnmarshalExtJSON(bytes.TrimSpace(state), false, &saved); err != nil || saved.Validate() != nil {
			return fmt.Errorf("state file %s doesn't hold a MongoDB resume token (%v); delete it to start from the current position", s.cfg.StateFile, err)
		}
		// startAfter (not resumeAfter) also accepts the token of an
		// invalidate event, e.g. after the database was dropped.
		opts.SetStartAfter(saved)
	}

	dbs := s.databases()
	pipeline := mongo.Pipeline{{{Key: "$match", Value: s.matchStage()}}}
	var cs *mongo.ChangeStream
	if len(dbs) == 1 {
		cs, err = client.Database(dbs[0]).Watch(ctx, pipeline, opts)
	} else {
		cs, err = client.Watch(ctx, pipeline, opts)
	}
	if err != nil {
		if saved != nil && mongoHistoryLost(err) {
			return s.historyLost(err)
		}
		return fmt.Errorf("open change stream: %w", err)
	}
	defer cs.Close(context.Background())

	var colls []string
	for _, r := range distinctTables(s.rules) {
		colls = append(colls, r.name())
	}
	if saved != nil {
		s.log.Printf("%s: streaming changes to %s (%d rules), resuming from %s", s.name(), strings.Join(colls, ", "), len(s.rules), s.cfg.StateFile)
	} else {
		s.log.Printf("%s: streaming changes to %s (%d rules); no saved position, so changes from now on are tracked", s.name(), strings.Join(colls, ", "), len(s.rules))
	}
	return s.stream(ctx, cs, k, saved)
}

func (s *mongoSource) stream(ctx context.Context, cs *mongo.ChangeStream, k *sink, saved bson.Raw) error {
	p := newMongoProcessor(s.rules, func(f string, a ...any) { s.log.Printf(s.name()+": "+f, a...) })
	lastSave := time.Now()
	save := func() error {
		tok := cs.ResumeToken()
		if tok == nil || bytes.Equal(tok, saved) {
			return nil
		}
		data, err := bson.MarshalExtJSON(tok, false, false)
		if err != nil {
			return err
		}
		if err := saveState(s.cfg.StateFile, append(data, '\n')); err != nil {
			return fmt.Errorf("save state file: %w", err)
		}
		saved, lastSave = append(bson.Raw(nil), tok...), time.Now()
		return nil
	}
	for {
		// TryNext waits up to the server's maxAwaitTime (1 s) for an event,
		// so an idle stream still comes back here to save its position.
		if !cs.TryNext(ctx) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if err := cs.Err(); err != nil {
				if mongoHistoryLost(err) {
					return s.historyLost(err)
				}
				return err
			}
			if cs.ID() == 0 {
				if err := save(); err != nil {
					return err
				}
				return errors.New("change stream closed by the server (the database was dropped or renamed?)")
			}
			// Nothing new: the token still advances past unrelated writes.
			// Saving it keeps the saved position inside the oplog window.
			if time.Since(lastSave) >= mongoIdleSave {
				if err := save(); err != nil {
					return err
				}
			}
			continue
		}
		ended := p.handle(cs.Current)
		// Take what the server already sent, without waiting for more.
		for n := 1; !ended && n < mongoMaxBatch && cs.RemainingBatchLength() > 0 && cs.TryNext(ctx); n++ {
			ended = p.handle(cs.Current)
		}
		if err := k.flush(ctx, s.name(), p.c.take(), nil); err != nil {
			return err
		}
		// Only now that the batch is invalidated may its position be saved:
		// a crash before this line replays the batch (at least once).
		if err := save(); err != nil {
			return err
		}
	}
}

func (s *mongoSource) historyLost(err error) error {
	return fmt.Errorf("the position saved in %s is no longer in the MongoDB oplog (%v). Changes made while cache-boy-sync wasn't reading can't be replayed: delete %s to start from now, and invalidate cache entries changed in the gap yourself (or let them expire)",
		s.cfg.StateFile, err, s.cfg.StateFile)
}

// mongoHistoryLost reports whether a change stream can't resume because its
// token is gone from the oplog (ChangeStreamHistoryLost, or the older
// ChangeStreamFatalError).
func mongoHistoryLost(err error) bool {
	var se mongo.ServerError
	return errors.As(err, &se) && (se.HasErrorCode(286) || se.HasErrorCode(280))
}

// checkServer pings the server and makes sure it supports change streams,
// which need a replica set or a sharded cluster. It returns the server's
// maxWireVersion.
func (s *mongoSource) checkServer(ctx context.Context, client *mongo.Client) (int, error) {
	var hello struct {
		SetName        string `bson:"setName"`
		Msg            string `bson:"msg"`
		MaxWireVersion int    `bson:"maxWireVersion"`
	}
	admin := client.Database("admin")
	err := admin.RunCommand(ctx, bson.D{{Key: "hello", Value: 1}}).Decode(&hello)
	if err != nil {
		var se mongo.ServerError
		if !errors.As(err, &se) {
			return 0, err
		}
		// Servers before 4.4.2 only know isMaster.
		if err := admin.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&hello); err != nil {
			return 0, err
		}
	}
	if hello.SetName == "" && hello.Msg != "isdbgrid" {
		return 0, errors.New("MongoDB is running as a standalone server, but change streams need a replica set or a sharded cluster. A single server can run as a one-member replica set: start mongod with --replSet rs0, then run rs.initiate() once in mongosh")
	}
	return hello.MaxWireVersion, nil
}

// fieldsBeyondID returns, per collection, the placeholder fields that rules
// for updates or deletes use besides the document key (_id).
func (s *mongoSource) fieldsBeyondID() map[string][]string {
	out := map[string][]string{}
	for _, r := range s.rules {
		if r.on&(opUpdate|opDelete) == 0 {
			continue
		}
		for _, c := range r.columns() {
			if c != "_id" && !strings.HasPrefix(c, "_id.") && !contains(out[r.name()], c) {
				out[r.name()] = append(out[r.name()], c)
			}
		}
	}
	return out
}

func (s *mongoSource) needsPreImages() bool { return len(s.fieldsBeyondID()) > 0 }

// checkPreImages warns when updates or deletes can't produce every tag.
// Without pre-images a change event only has the document's _id (and, for
// updates, the document as it is now), so a tag built from another field
// (an order's user_id, say) can't be computed for the document as it was.
func (s *mongoSource) checkPreImages(ctx context.Context, client *mongo.Client) {
	need := s.fieldsBeyondID()
	for _, r := range distinctTables(s.rules) {
		fields := need[r.name()]
		if len(fields) == 0 {
			continue
		}
		cur, err := client.Database(r.schema).ListCollections(ctx, bson.D{{Key: "name", Value: r.table}})
		if err != nil {
			continue
		}
		var colls []struct {
			Options struct {
				PrePost struct {
					Enabled bool `bson:"enabled"`
				} `bson:"changeStreamPreAndPostImages"`
			} `bson:"options"`
		}
		if err := cur.All(ctx, &colls); err != nil {
			continue
		}
		list := strings.Join(fields, ", ")
		switch {
		case len(colls) == 0:
			s.log.Printf("%s: warning: collection %s doesn't exist yet. Create it with pre-images so deletes (and updates that change %s) can invalidate tags built from %s: use %s; db.createCollection(%q, {changeStreamPreAndPostImages: {enabled: true}})",
				s.name(), r.name(), list, list, r.schema, r.table)
		case !colls[0].Options.PrePost.Enabled:
			s.log.Printf("%s: warning: %s: deletes (and updates that change %s) can't invalidate tags built from %s, because without pre-images MongoDB only sends the old document's _id. Fix (MongoDB 6.0+): use %s; db.runCommand({collMod: %q, changeStreamPreAndPostImages: {enabled: true}})",
				s.name(), r.name(), list, list, r.schema, r.table)
		}
	}
}

// databases returns the databases the rules watch, sorted.
func (s *mongoSource) databases() []string {
	var dbs []string
	for _, r := range s.rules {
		if !contains(dbs, r.schema) {
			dbs = append(dbs, r.schema)
		}
	}
	sort.Strings(dbs)
	return dbs
}

var mongoCollOps = bson.A{"insert", "update", "replace", "delete", "drop", "rename"}

// matchStage limits the change stream to the watched collections and the
// operations that can invalidate something, so the server doesn't send the
// rest.
func (s *mongoSource) matchStage() bson.D {
	byDB := map[string]bson.A{}
	for _, r := range distinctTables(s.rules) {
		byDB[r.schema] = append(byDB[r.schema], r.table)
	}
	var or bson.A
	dbs := bson.A{}
	for _, db := range s.databases() {
		or = append(or, bson.D{
			{Key: "ns.db", Value: db},
			{Key: "ns.coll", Value: bson.D{{Key: "$in", Value: byDB[db]}}},
			{Key: "operationType", Value: bson.D{{Key: "$in", Value: mongoCollOps}}},
		})
		dbs = append(dbs, db)
	}
	or = append(or,
		bson.D{{Key: "operationType", Value: "dropDatabase"}, {Key: "ns.db", Value: bson.D{{Key: "$in", Value: dbs}}}},
		bson.D{{Key: "operationType", Value: "invalidate"}},
	)
	return bson.D{{Key: "$or", Value: or}}
}

// --- events ---

// mongoProcessor turns change events into batches. It has no I/O, so it can
// be tested with hand-made events.
type mongoProcessor struct {
	rules map[string][]*rule // "db.collection" → rules
	byDB  map[string][]*rule
	c     *collector
}

func newMongoProcessor(rules []*rule, warn func(string, ...any)) *mongoProcessor {
	p := &mongoProcessor{rules: map[string][]*rule{}, byDB: map[string][]*rule{}, c: newCollector(warn)}
	for _, r := range rules {
		p.rules[r.name()] = append(p.rules[r.name()], r)
		p.byDB[r.schema] = append(p.byDB[r.schema], r)
	}
	return p
}

// handle adds one change event to the collector. It reports whether the
// event ends the stream (invalidate).
//
//   - insert: the new document.
//   - update, replace: the document after the change (fullDocument, looked
//     up by the server; for updates the changed fields are laid over it, so
//     their values are this change's even if the document changed again
//     since) and the document before it (pre-image), when available.
//   - delete: the pre-image when available, otherwise only the document key.
//   - drop, rename, dropDatabase: like TRUNCATE, only static tags and keys.
//
// The document key (_id, plus the shard key) is always filled in, so tags
// built from _id work without pre-images.
func (p *mongoProcessor) handle(ev bson.Raw) (ended bool) {
	optype, _ := ev.Lookup("operationType").StringValueOK()
	db, _ := ev.Lookup("ns", "db").StringValueOK()
	coll, _ := ev.Lookup("ns", "coll").StringValueOK()
	name := db + "." + coll
	rules := p.rules[name]
	key := docRow(ev.Lookup("documentKey"))
	before := docRow(ev.Lookup("fullDocumentBeforeChange"))
	after := docRow(ev.Lookup("fullDocument"))
	switch optype {
	case "insert":
		p.c.row(rules, opInsert, withKey(after, key), nil)
	case "update", "replace":
		if after == nil {
			after = Row{} // deleted since: only the key and changed fields
		}
		if upd, ok := ev.Lookup("updateDescription", "updatedFields").DocumentOK(); ok {
			flattenInto(after, "", upd)
		}
		var old Row
		if before != nil {
			old = withKey(before, key)
		}
		p.c.row(rules, opUpdate, withKey(after, key), old)
	case "delete":
		if before == nil {
			before = Row{}
		}
		p.c.row(rules, opDelete, nil, withKey(before, key))
	case "drop", "rename":
		p.c.truncate(rules, name)
	case "dropDatabase":
		p.c.truncate(p.byDB[db], db)
	case "invalidate":
		return true
	}
	return false
}

// withKey fills the document key's fields into row where missing.
func withKey(row, key Row) Row {
	if row == nil {
		row = Row{}
	}
	for k, v := range key {
		if _, ok := row[k]; !ok {
			row[k] = v
		}
	}
	return row
}

// docRow flattens an embedded document value; nil when it isn't one (absent
// or null, as fullDocument is when no document was found).
func docRow(v bson.RawValue) Row {
	doc, ok := v.DocumentOK()
	if !ok {
		return nil
	}
	return flatten(doc)
}

// flatten turns a document into a Row. Nested documents become dotted
// paths ({"profile": {"city": "Dhaka"}} → "profile.city"), and so do array
// elements, by index ({"tags": ["a", "b"]} → "tags.0", "tags.1"), the same
// paths MongoDB uses in queries and in update events' changed fields.
//
// Values are rendered as an application would print them:
//
//   - string: as is; ObjectId: 24 hex digits; UUID (binary subtype 4): the
//     usual 8-4-4-4-12 form
//   - int32, int64: decimal; decimal128: its string form
//   - double: the shortest form that reads back as the same number, without
//     an exponent for ordinary magnitudes and without ".0" for whole numbers:
//     42.0 → "42", 2.5 → "2.5", 1e21 → "1e+21". Many drivers and tools
//     store whole numbers as doubles, and a tag such as user:{user_id} should
//     then read user:42, the way the application formats the number.
//   - bool: true / false; date: RFC 3339 in UTC with milliseconds when not
//     zero ("2024-05-01T10:00:00Z", "2024-05-01T10:00:00.25Z")
//   - null, undefined: NULL (a tag using it isn't invalidated)
//
// Other types (binary data, regular expressions, timestamps, ...) are left
// out, so tags using them can't be computed.
func flatten(doc bson.Raw) Row {
	row := Row{}
	flattenInto(row, "", doc)
	return row
}

func flattenInto(row Row, prefix string, doc bson.Raw) {
	elems, err := doc.Elements()
	if err != nil {
		return
	}
	for _, e := range elems {
		path := prefix + e.Key()
		v := e.Value()
		switch v.Type {
		case bson.TypeEmbeddedDocument:
			flattenInto(row, path+".", v.Document())
		case bson.TypeArray:
			flattenInto(row, path+".", bson.Raw(v.Array()))
		default:
			if val, ok := mongoValue(v); ok {
				row[path] = val
			}
		}
	}
}

func mongoValue(v bson.RawValue) (value, bool) {
	switch v.Type {
	case bson.TypeString:
		return TextValue(v.StringValue()), true
	case bson.TypeSymbol:
		return TextValue(v.Symbol()), true
	case bson.TypeObjectID:
		return TextValue(v.ObjectID().Hex()), true
	case bson.TypeInt32:
		return TextValue(strconv.FormatInt(int64(v.Int32()), 10)), true
	case bson.TypeInt64:
		return TextValue(strconv.FormatInt(v.Int64(), 10)), true
	case bson.TypeDouble:
		return TextValue(formatDouble(v.Double())), true
	case bson.TypeDecimal128:
		return TextValue(v.Decimal128().String()), true
	case bson.TypeBoolean:
		return TextValue(strconv.FormatBool(v.Boolean())), true
	case bson.TypeDateTime:
		return TextValue(time.UnixMilli(v.DateTime()).UTC().Format(time.RFC3339Nano)), true
	case bson.TypeNull, bson.TypeUndefined:
		return NullValue(), true
	case bson.TypeBinary:
		if sub, data := v.Binary(); sub == 4 && len(data) == 16 {
			h := hex.EncodeToString(data)
			return TextValue(h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]), true
		}
	}
	return value{}, false
}

// formatDouble renders a double like JavaScript's String(n): 42 not 42.0,
// 0.1 not 1e-01, and exponents only for very large or small numbers.
func formatDouble(f float64) string {
	switch {
	case math.IsNaN(f):
		return "NaN"
	case math.IsInf(f, 1):
		return "Infinity"
	case math.IsInf(f, -1):
		return "-Infinity"
	case f == 0:
		return "0" // also -0
	}
	if a := math.Abs(f); a >= 1e-6 && a < 1e21 {
		return strconv.FormatFloat(f, 'f', -1, 64)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}
