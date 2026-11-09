package dbsync

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// mySQLSource streams MySQL row changes from the binlog, like a replica.
//
// Progress is a binlog position (file + offset, plus the executed GTID set
// when gtid_mode is ON) kept in the state file. It is saved only after the
// changes before it were invalidated, so a restart replays from there and
// nothing is skipped (at-least-once). MySQL keeps binlogs for
// binlog_expire_logs_seconds (30 days by default), so a sync that is down
// longer than that can't catch up.
type mySQLSource struct {
	cfg     *SourceConfig
	dsn     *myDSN
	rules   []*rule
	watched map[string]bool // lowercase "db.table" of every rule
	log     *log.Logger
	noted   bool // the binlog_row_metadata note was logged
}

func newMySQL(sc *SourceConfig, logger *log.Logger) (*mySQLSource, error) {
	dsn, err := parseMySQLURL(sc.URL)
	if err != nil {
		return nil, fmt.Errorf("config: source %q: %w", sc.Name, err)
	}
	rules, err := compileRules(sc, dsn.db)
	if err != nil {
		return nil, err
	}
	watched := map[string]bool{}
	for _, r := range rules {
		if r.schema == "" {
			return nil, fmt.Errorf("config: source %q: rule for %s: no database: write the table as db.table or put the database in the url (mysql://user:pass@host:3306/db)", sc.Name, r.table)
		}
		watched[myKey(r.schema, r.table)] = true
	}
	return &mySQLSource{cfg: sc, dsn: dsn, rules: rules, watched: watched, log: logger}, nil
}

func (s *mySQLSource) name() string { return s.cfg.Name }

// myKey is how tables are matched: MySQL table names are case-insensitive
// on Windows and macOS, and rules are written by hand, so case is ignored.
func myKey(schema, table string) string { return strings.ToLower(schema + "." + table) }

// --- connection URL ---

// myDSN is a parsed connection URL.
type myDSN struct {
	user, pass string
	host       string
	port       uint16
	db         string
	tls        *tls.Config
}

func (d *myDSN) addr() string { return net.JoinHostPort(d.host, strconv.Itoa(int(d.port))) }

// parseMySQLURL accepts mysql://user:pass@host:3306/db?tls=true and the Go
// driver form user:pass@tcp(host:3306)/db?tls=true. The only parameter used
// is tls (true, skip-verify, false); others, such as the driver's
// parseTime, are ignored so a DSN can be copied from an application.
func parseMySQLURL(raw string) (*myDSN, error) {
	d := &myDSN{host: "127.0.0.1", port: 3306}
	var query string
	var hostport string
	if strings.HasPrefix(raw, "mysql://") {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("bad mysql url: %w", err)
		}
		if u.User != nil {
			d.user = u.User.Username()
			d.pass, _ = u.User.Password()
		}
		hostport = u.Host
		d.db = strings.TrimPrefix(u.Path, "/")
		query = u.RawQuery
	} else {
		slash := strings.LastIndex(raw, "/")
		if slash < 0 {
			return nil, fmt.Errorf("bad mysql url %q: expected mysql://user:pass@host:3306/db or user:pass@tcp(host:3306)/db", raw)
		}
		head, tail := raw[:slash], raw[slash+1:]
		db, q, _ := strings.Cut(tail, "?")
		var err error
		if d.db, err = url.PathUnescape(db); err != nil {
			return nil, fmt.Errorf("bad mysql url: database: %w", err)
		}
		query = q
		netAddr := head
		if at := strings.LastIndex(head, "@"); at >= 0 {
			d.user, d.pass, _ = strings.Cut(head[:at], ":")
			netAddr = head[at+1:]
		}
		switch {
		case netAddr == "" || netAddr == "tcp":
		case strings.HasPrefix(netAddr, "tcp(") && strings.HasSuffix(netAddr, ")"):
			hostport = netAddr[4 : len(netAddr)-1]
		case strings.HasPrefix(netAddr, "unix("):
			return nil, errors.New("bad mysql url: unix sockets aren't supported; connect over tcp(host:port)")
		default:
			return nil, fmt.Errorf("bad mysql url: expected tcp(host:port) after @, got %q", netAddr)
		}
	}
	if hostport != "" {
		host, port, err := net.SplitHostPort(hostport)
		if err != nil { // no port
			host, port = strings.Trim(hostport, "[]"), ""
		}
		if host != "" {
			d.host = host
		}
		if port != "" {
			p, err := strconv.ParseUint(port, 10, 16)
			if err != nil || p == 0 {
				return nil, fmt.Errorf("bad mysql url: port %q", port)
			}
			d.port = uint16(p)
		}
	}
	params, err := url.ParseQuery(query)
	if err != nil {
		return nil, fmt.Errorf("bad mysql url: parameters: %w", err)
	}
	switch v := strings.ToLower(params.Get("tls")); v {
	case "", "false":
	case "true":
		d.tls = &tls.Config{ServerName: d.host}
	case "skip-verify":
		d.tls = &tls.Config{InsecureSkipVerify: true}
	default:
		return nil, fmt.Errorf("bad mysql url: tls=%s: use true, skip-verify or false", v)
	}
	return d, nil
}

// --- session ---

// myPos is the saved stream position: everything before it is invalidated.
type myPos struct {
	File string `json:"file"`
	Pos  uint32 `json:"pos"`
	GTID string `json:"gtid,omitempty"` // executed GTID set, when gtid_mode is ON
}

func (p myPos) String() string {
	s := fmt.Sprintf("%s:%d", p.File, p.Pos)
	if p.GTID != "" {
		s += " (GTID " + p.GTID + ")"
	}
	return s
}

// myServer is what the startup checks found.
type myServer struct {
	flavor   string // mysql or mariadb
	version  string
	gtid     bool
	rowImage string
	metadata string // binlog_row_metadata
}

func (s *mySQLSource) connect(ctx context.Context) (*client.Conn, error) {
	var opts []client.Option
	if s.dsn.tls != nil {
		cfg := s.dsn.tls
		opts = append(opts, func(c *client.Conn) error { c.SetTLSConfig(cfg); return nil })
	}
	conn, err := client.ConnectWithContext(ctx, s.dsn.addr(), s.dsn.user, s.dsn.pass, "", 10*time.Second, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect to %s: %w", s.dsn.addr(), err)
	}
	return conn, nil
}

func (s *mySQLSource) session(ctx context.Context, k *sink) error {
	conn, err := s.connect(ctx)
	if err != nil {
		return err
	}
	defer func() { conn.Close() }()

	srv, err := s.checkServer(conn)
	if err != nil {
		return err
	}
	s.checkTables(conn, srv)

	start, err := s.startPosition(conn, srv)
	if err != nil {
		return err
	}

	syncer := replication.NewBinlogSyncer(s.syncerConfig(srv))
	defer syncer.Close()
	var streamer *replication.BinlogStreamer
	if srv.gtid && start.GTID != "" {
		gset, err := mysql.ParseMysqlGTIDSet(start.GTID)
		if err != nil {
			return fmt.Errorf("state file %s: bad GTID set: %w", s.cfg.StateFile, err)
		}
		streamer, err = syncer.StartSyncGTID(gset)
	} else {
		streamer, err = syncer.StartSync(mysql.Position{Name: start.File, Pos: start.Pos})
	}
	if err != nil {
		return s.explain(err, start)
	}
	s.log.Printf("%s: streaming binlog from %s (%d rules)", s.name(), start, len(s.rules))

	lookup := func(schema, table string) ([]myColumn, error) {
		cols, err := tableColumns(conn, schema, table)
		if err != nil { // the query connection may have timed out while idle
			conn.Close()
			if conn, err = s.connect(ctx); err != nil {
				return nil, err
			}
			cols, err = tableColumns(conn, schema, table)
		}
		return cols, err
	}
	p := newMyProcessor(s.rules, lookup, func(f string, a ...any) { s.log.Printf(s.name()+": "+f, a...) })
	p.file, p.pos = start.File, start
	return s.stream(ctx, streamer, p, k, start)
}

func (s *mySQLSource) syncerConfig(srv *myServer) replication.BinlogSyncerConfig {
	watched := s.watched
	return replication.BinlogSyncerConfig{
		ServerID:  s.cfg.ServerID,
		Flavor:    srv.flavor,
		Host:      s.dsn.host,
		Port:      s.dsn.port,
		User:      s.dsn.user,
		Password:  s.dsn.pass,
		TLSConfig: s.dsn.tls,
		// TIMESTAMP values are rendered in UTC so tags don't depend on the
		// machine's time zone.
		TimestampStringLocation: time.UTC,
		RenderJSONAsMySQLText:   true,
		HeartbeatPeriod:         15 * time.Second,
		ReadTimeout:             time.Minute,
		// Reconnecting is the Syncer's job: it resumes from the saved
		// position, not from what was read but maybe not invalidated.
		DisableRetrySync: true,
		FillZeroLogPos:   srv.flavor == mysql.MariaDBFlavor,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		// Rows of tables without rules are not decoded at all.
		RowsEventDecodeFunc: func(e *replication.RowsEvent, data []byte) error {
			pos, err := e.DecodeHeader(data)
			if err != nil {
				return err
			}
			if !watched[myKey(string(e.Table.Schema), string(e.Table.Table))] {
				return nil
			}
			return e.DecodeData(pos, data)
		},
	}
}

const mySaveInterval = time.Second

func (s *mySQLSource) stream(ctx context.Context, streamer *replication.BinlogStreamer, p *myProcessor, k *sink, start myPos) error {
	done, saved := start, start // done: everything before it is invalidated
	var lastSave time.Time
	save := func() error {
		if done == saved {
			return nil
		}
		data, _ := json.Marshal(done)
		if err := saveState(s.cfg.StateFile, data); err != nil {
			return fmt.Errorf("save state: %w", err)
		}
		saved, lastSave = done, time.Now()
		return nil
	}
	// Positions are saved at most once a second (replaying a second of
	// changes after a crash only invalidates again), and on the way out.
	defer save()
	for {
		gctx, cancel := context.WithTimeout(ctx, mySaveInterval)
		ev, err := streamer.GetEvent(gctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if errors.Is(err, context.DeadlineExceeded) {
				if err := save(); err != nil {
					return err
				}
				continue
			}
			return s.explain(err, saved)
		}
		b, pos, err := p.handle(ev)
		if err != nil {
			return err
		}
		if pos == nil {
			continue
		}
		if b != nil {
			if err := k.flush(ctx, s.name(), b, nil); err != nil {
				return err
			}
		}
		done = *pos
		if time.Since(lastSave) >= mySaveInterval {
			if err := save(); err != nil {
				return err
			}
		}
	}
}

// explain adds what to do to common replication errors.
func (s *mySQLSource) explain(err error, pos myPos) error {
	var me *mysql.MyError
	code := uint16(0)
	if errors.As(err, &me) {
		code = me.Code
	}
	msg := err.Error()
	switch {
	case code == mysql.ER_MASTER_FATAL_ERROR_READING_BINLOG || strings.Contains(msg, "1236"):
		return fmt.Errorf("%w (binlog position %s is no longer on the server, probably purged after binlog_expire_logs_seconds; changes since then were missed: clear the cache, then delete %s or run cache-boy-sync -uninstall to start from the current position)", err, pos, s.cfg.StateFile)
	case code == mysql.ER_SPECIFIC_ACCESS_DENIED_ERROR || code == mysql.ER_ACCESS_DENIED_ERROR || strings.Contains(msg, "REPLICATION"):
		return fmt.Errorf("%w (the user needs GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* and SELECT on the watched tables)", err)
	}
	return err
}

// checkServer verifies the binlog is on and in row format.
func (s *mySQLSource) checkServer(conn *client.Conn) (*myServer, error) {
	res, err := conn.Execute(`SHOW GLOBAL VARIABLES WHERE Variable_name IN
  ('version', 'log_bin', 'binlog_format', 'binlog_row_image', 'gtid_mode', 'binlog_row_metadata')`)
	if err != nil {
		return nil, fmt.Errorf("read server variables: %w", err)
	}
	vars := map[string]string{}
	for i := range res.RowNumber() {
		n, _ := res.GetString(i, 0)
		v, _ := res.GetString(i, 1)
		vars[strings.ToLower(n)] = strings.ToUpper(v)
	}
	srv := &myServer{flavor: mysql.MySQLFlavor, version: vars["version"], rowImage: vars["binlog_row_image"], metadata: vars["binlog_row_metadata"]}
	if strings.Contains(srv.version, "MARIADB") {
		srv.flavor = mysql.MariaDBFlavor // its GTIDs differ: file + position is used
	} else {
		srv.gtid = vars["gtid_mode"] == "ON"
	}
	if lb := vars["log_bin"]; lb != "ON" && lb != "1" {
		return nil, errors.New("binary logging is off: start MySQL with log_bin (on by default since MySQL 8.0)")
	}
	if f := vars["binlog_format"]; f != "" && f != "ROW" {
		return nil, fmt.Errorf("binlog_format is %s; set it to ROW (SET PERSIST binlog_format = 'ROW', or binlog_format=ROW in my.cnf) so row changes are logged", f)
	}
	return srv, nil
}

// checkTables warns about rules that can't work fully: a missing table, or
// tags built from columns that binlog_row_image doesn't log.
func (s *mySQLSource) checkTables(conn *client.Conn, srv *myServer) {
	if srv.metadata != "FULL" && !s.noted {
		s.noted = true // once, not on every reconnect
		s.log.Printf("%s: note: column names come from information_schema; with binlog_row_metadata = FULL they come with each change instead, which stays right when replaying changes made before an ALTER TABLE", s.name())
	}
	for _, r := range distinctTables(s.rules) {
		cols, err := tableColumns(conn, r.schema, r.table)
		if err != nil {
			continue
		}
		if len(cols) == 0 {
			s.log.Printf("%s: warning: table %s doesn't exist (or the user can't see it); its rules apply once it does", s.name(), r.name())
			continue
		}
		if srv.rowImage == "FULL" || srv.rowImage == "" {
			continue
		}
		info := map[string]myColumn{}
		for _, c := range cols {
			info[strings.ToLower(c.name)] = c
		}
		var missing []string
		for _, rr := range s.rules {
			if !strings.EqualFold(rr.name(), r.name()) || rr.on&(opUpdate|opDelete) == 0 {
				continue
			}
			for _, c := range rr.columns() {
				ci, ok := info[strings.ToLower(c)]
				lost := !ok || !ci.key
				if srv.rowImage == "NOBLOB" {
					lost = ok && ci.blob
				}
				if lost && !contains(missing, c) {
					missing = append(missing, c)
				}
			}
		}
		if len(missing) > 0 {
			s.log.Printf("%s: warning: %s: updates and deletes can't always invalidate tags built from %s, because binlog_row_image is %s and MySQL doesn't log those columns of the old row. Fix: SET PERSIST binlog_row_image = 'FULL';",
				s.name(), r.name(), strings.Join(missing, ", "), srv.rowImage)
		}
	}
}

// startPosition returns the saved position, or the server's current one
// (saved right away) when there is no state file yet.
func (s *mySQLSource) startPosition(conn *client.Conn, srv *myServer) (myPos, error) {
	data, err := loadState(s.cfg.StateFile)
	if err != nil {
		return myPos{}, fmt.Errorf("read state: %w", err)
	}
	if data != nil {
		var p myPos
		if err := json.Unmarshal(data, &p); err != nil || p.File == "" {
			return myPos{}, fmt.Errorf("state file %s is damaged; delete it to start from the current position", s.cfg.StateFile)
		}
		return p, nil
	}
	res, err := conn.Execute("SHOW BINARY LOG STATUS") // MySQL 8.2+
	var me *mysql.MyError
	if errors.As(err, &me) && me.Code == mysql.ER_PARSE_ERROR {
		res, err = conn.Execute("SHOW MASTER STATUS") // older MySQL, MariaDB
	}
	if err != nil {
		return myPos{}, fmt.Errorf("read binlog position: %w (the user needs REPLICATION CLIENT)", err)
	}
	if res.RowNumber() == 0 {
		return myPos{}, errors.New("the server reports no binlog position: is log_bin on, and does the user have REPLICATION CLIENT?")
	}
	var p myPos
	p.File, _ = res.GetStringByName(0, "File")
	pos, _ := res.GetUintByName(0, "Position")
	p.Pos = uint32(pos)
	if srv.gtid {
		g, _ := res.GetStringByName(0, "Executed_Gtid_Set")
		p.GTID = strings.ReplaceAll(strings.ReplaceAll(g, "\n", ""), " ", "")
	}
	b, _ := json.Marshal(p)
	if err := saveState(s.cfg.StateFile, b); err != nil {
		return myPos{}, fmt.Errorf("save state: %w", err)
	}
	s.log.Printf("%s: no state file yet: starting at the current binlog position %s; changes from now on are tracked (position kept in %s)", s.name(), p, s.cfg.StateFile)
	return p, nil
}

// tableColumns reads a table's columns in binlog order.
func tableColumns(conn *client.Conn, schema, table string) ([]myColumn, error) {
	res, err := conn.Execute(`SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_KEY, DATA_TYPE FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`, schema, table)
	if err != nil {
		return nil, fmt.Errorf("read columns of %s.%s: %w", schema, table, err)
	}
	cols := make([]myColumn, res.RowNumber())
	for i := range cols {
		name, _ := res.GetString(i, 0)
		ctype, _ := res.GetString(i, 1)
		key, _ := res.GetString(i, 2)
		dtype, _ := res.GetString(i, 3)
		cols[i] = columnFromType(name, ctype)
		cols[i].key = key == "PRI"
		dtype = strings.ToLower(dtype)
		cols[i].blob = strings.HasSuffix(dtype, "blob") || strings.HasSuffix(dtype, "text") || dtype == "json"
	}
	return cols, nil
}

// columnFromType builds a column from its information_schema COLUMN_TYPE,
// e.g. "int unsigned" or "enum('new','paid')".
func columnFromType(name, ctype string) myColumn {
	c := myColumn{name: name}
	lt := strings.ToLower(ctype)
	switch {
	case strings.HasPrefix(lt, "enum("):
		c.enum = parseEnumValues(ctype[5:])
	case strings.HasPrefix(lt, "set("):
		c.set = parseEnumValues(ctype[4:])
	}
	return c
}

// parseEnumValues parses "'a','b”c')" (the part after "enum(").
func parseEnumValues(s string) []string {
	var out []string
	for {
		s = strings.TrimLeft(s, ", ")
		if !strings.HasPrefix(s, "'") {
			return out
		}
		var b strings.Builder
		i := 1
		for i < len(s) {
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					b.WriteByte('\'')
					i += 2
					continue
				}
				break
			}
			if s[i] == '\\' && i+1 < len(s) {
				i++
			}
			b.WriteByte(s[i])
			i++
		}
		out = append(out, b.String())
		if i >= len(s) {
			return out
		}
		s = s[i+1:]
	}
}

// --- events → batches ---

// myColumn is one column of a watched table.
type myColumn struct {
	name      string
	enum, set []string // member names, to turn ENUM indexes and SET bits into text
	key, blob bool     // from information_schema, for the startup check
}

// myTable is a watched table as described by a TABLE_MAP event.
type myTable struct {
	key, name string
	rules     []*rule
	cols      []myColumn // nil when the columns couldn't be matched
}

// myProcessor turns binlog events into one batch per transaction. It does no
// I/O except through lookup, so it can be tested with hand-made events.
type myProcessor struct {
	rules  map[string][]*rule    // myKey → rules
	tables map[uint64]*myTable   // table id → watched table
	cache  map[string][]myColumn // myKey → columns from lookup
	lookup func(schema, table string) ([]myColumn, error)
	warned map[string]bool
	warn   func(string, ...any)
	c      *collector
	inTx   bool
	file   string // current binlog file
	pos    myPos  // position after the last complete transaction
}

func newMyProcessor(rules []*rule, lookup func(schema, table string) ([]myColumn, error), warn func(string, ...any)) *myProcessor {
	p := &myProcessor{rules: map[string][]*rule{}, lookup: lookup, warn: warn, warned: map[string]bool{}, c: newCollector(warn)}
	p.resetTables()
	for _, r := range rules {
		k := myKey(r.schema, r.table)
		p.rules[k] = append(p.rules[k], r)
	}
	return p
}

func (p *myProcessor) resetTables() {
	p.tables, p.cache = map[uint64]*myTable{}, map[string][]myColumn{}
}

// handle processes one event. At the end of a transaction it returns the
// position after it and, if rules matched, the batch to invalidate first.
func (p *myProcessor) handle(e *replication.BinlogEvent) (*batch, *myPos, error) {
	return p.event(e.Header, e.Event, e.Header.LogPos)
}

func (p *myProcessor) event(h *replication.EventHeader, ev replication.Event, logPos uint32) (*batch, *myPos, error) {
	if h != nil && h.EventType == replication.XA_PREPARE_LOG_EVENT {
		// An XA transaction's rows are logged at XA PREPARE; they are
		// invalidated then, before the XA COMMIT (a later XA ROLLBACK only
		// means they were invalidated for nothing).
		return p.commit(logPos, nil)
	}
	switch ev := ev.(type) {
	case *replication.RotateEvent:
		p.file = string(ev.NextLogName)
		if !p.inTx {
			p.pos.File, p.pos.Pos = p.file, uint32(ev.Position)
			pos := p.pos
			return nil, &pos, nil
		}
	case *replication.MariadbGTIDEvent:
		p.inTx = !ev.IsStandalone() // MariaDB has no BEGIN after its GTID
	case *replication.TableMapEvent:
		return nil, nil, p.tableMap(ev)
	case *replication.RowsEvent:
		p.rows(h.EventType, ev)
	case *replication.XIDEvent:
		return p.commit(logPos, ev.GSet)
	case *replication.QueryEvent:
		return p.query(ev, logPos)
	case *replication.TransactionPayloadEvent: // binlog_transaction_compression
		var b *batch
		var pos *myPos
		for _, inner := range ev.Events {
			ib, ipos, err := p.event(inner.Header, inner.Event, logPos)
			if err != nil {
				return nil, nil, err
			}
			if ipos != nil {
				b, pos = ib, ipos
			}
		}
		return b, pos, nil
	}
	return nil, nil, nil
}

func (p *myProcessor) commit(logPos uint32, gset mysql.GTIDSet) (*batch, *myPos, error) {
	p.inTx = false
	p.pos.File = p.file
	if logPos > 0 {
		p.pos.Pos = logPos
	}
	if gset != nil {
		p.pos.GTID = gset.String()
	}
	pos := p.pos
	if p.c.empty() {
		return nil, &pos, nil
	}
	return p.c.take(), &pos, nil
}

func (p *myProcessor) query(ev *replication.QueryEvent, logPos uint32) (*batch, *myPos, error) {
	q := trimSQL(string(ev.Query))
	word := strings.ToUpper(firstWord(q))
	switch {
	case word == "BEGIN" || strings.HasPrefix(strings.ToUpper(q), "XA START"):
		p.inTx = true
		p.c.reset()
		return nil, nil, nil
	case word == "COMMIT":
		return p.commit(logPos, ev.GSet)
	case strings.EqualFold(q, "ROLLBACK"):
		// Only logged when a rolled-back transaction also changed a
		// non-transactional table; its other changes never happened.
		p.c.reset()
		return p.commit(logPos, ev.GSet)
	case p.inTx:
		return nil, nil, nil // SAVEPOINT and the like
	}
	// A statement on its own (DDL, XA COMMIT): it commits by itself.
	switch word {
	case "TRUNCATE":
		if schema, table, ok := truncateTarget(q, string(ev.Schema)); ok {
			if rules := p.rules[myKey(schema, table)]; rules != nil {
				p.c.truncate(rules, schema+"."+table)
			}
		}
		p.resetTables()
	case "ALTER", "CREATE", "DROP", "RENAME":
		p.resetTables() // columns may have changed
	}
	return p.commit(logPos, ev.GSet)
}

func (p *myProcessor) tableMap(ev *replication.TableMapEvent) error {
	schema, table := string(ev.Schema), string(ev.Table)
	k := myKey(schema, table)
	rules := p.rules[k]
	if rules == nil {
		delete(p.tables, ev.TableID)
		return nil
	}
	if t := p.tables[ev.TableID]; t != nil && t.key == k && (t.cols == nil || len(t.cols) == int(ev.ColumnCount)) {
		return nil
	}
	t := &myTable{key: k, name: schema + "." + table, rules: rules}
	n := int(ev.ColumnCount)
	if names := ev.ColumnNameString(); len(names) == n { // binlog_row_metadata = FULL
		enums, sets := ev.EnumStrValueMap(), ev.SetStrValueMap()
		t.cols = make([]myColumn, n)
		for i, name := range names {
			t.cols[i] = myColumn{name: name, enum: enums[i], set: sets[i]}
		}
	} else {
		cols, ok := p.cache[k]
		if !ok || len(cols) != n {
			var err error
			if cols, err = p.lookup(schema, table); err != nil {
				return err
			}
			p.cache[k] = cols
		}
		if len(cols) == n {
			t.cols = cols
		} else if !p.warned[k] {
			p.warned[k] = true
			p.warn("%s: the binlog has %d columns but the table now has %d (changed since?); only tags without {column} are invalidated for these changes. Set binlog_row_metadata = FULL to avoid this", t.name, n, len(cols))
		}
	}
	p.tables[ev.TableID] = t
	return nil
}

func (p *myProcessor) rows(et replication.EventType, ev *replication.RowsEvent) {
	t := p.tables[ev.TableID]
	if t == nil || len(ev.Rows) == 0 {
		return
	}
	var o op
	switch et {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2, replication.MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
		o = opInsert
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1, replication.PARTIAL_UPDATE_ROWS_EVENT:
		o = opUpdate
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2, replication.MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
		o = opDelete
	default:
		return
	}
	if t.cols == nil {
		p.c.row(t.rules, o, nil, nil) // static tags only
		return
	}
	row := func(i int) Row {
		var skipped []int
		if i < len(ev.SkippedColumns) {
			skipped = ev.SkippedColumns[i]
		}
		return t.row(ev.Rows[i], skipped)
	}
	switch o {
	case opInsert:
		for i := range ev.Rows {
			p.c.row(t.rules, o, row(i), nil)
		}
	case opDelete:
		for i := range ev.Rows {
			p.c.row(t.rules, o, nil, row(i))
		}
	case opUpdate: // before and after images alternate
		for i := 0; i+1 < len(ev.Rows); i += 2 {
			p.c.row(t.rules, o, row(i+1), row(i))
		}
	}
}

// row maps a row image to column names. Columns not in the image
// (binlog_row_image MINIMAL or NOBLOB) are marked unchanged, not NULL.
func (t *myTable) row(vals []any, skipped []int) Row {
	r := make(Row, len(t.cols))
	for i, c := range t.cols {
		if i >= len(vals) {
			break
		}
		if vals[i] == nil {
			r[c.name] = NullValue()
			continue
		}
		if s, ok := myText(vals[i], c); ok {
			r[c.name] = TextValue(s)
		} else {
			r[c.name] = UnchangedValue()
		}
	}
	for _, i := range skipped {
		if i < len(t.cols) {
			r[t.cols[i].name] = UnchangedValue()
		}
	}
	return r
}

// myText renders a decoded binlog value the way MySQL prints it, so tags
// match what the application builds from query results:
//
//   - strings, binary strings and BLOBs: as is
//   - integers, BIT and YEAR: decimal ("42", "-7"); unsigned columns are
//     decoded unsigned using the signedness the binlog carries (MySQL 8.0+,
//     MariaDB 10.5+)
//   - FLOAT and DOUBLE: shortest form that reads back the same ("1.5",
//     "0.1"), with an exponent only for very large or small values ("1e+21")
//   - DECIMAL: with its scale ("12.50")
//   - DATE "2006-01-02", TIME "15:04:05", DATETIME "2006-01-02 15:04:05",
//     with fractional seconds when the column has them; TIMESTAMP likewise,
//     in UTC
//   - JSON: its text as MySQL prints it
//   - ENUM: the member name; SET: member names joined with ","
//
// It returns false for a value that can't be rendered (a partial JSON
// update), which is treated as not sent.
func myText(v any, c myColumn) (string, bool) {
	if c.enum != nil {
		if i, ok := v.(int64); ok {
			if i == 0 {
				return "", true // the "invalid value" member
			}
			if i >= 1 && int(i) <= len(c.enum) {
				return c.enum[i-1], true
			}
		}
	}
	if c.set != nil {
		if bits, ok := v.(int64); ok {
			var names []string
			for i, m := range c.set {
				if i < 64 && bits&(1<<i) != 0 {
					names = append(names, m)
				}
			}
			return strings.Join(names, ","), true
		}
	}
	switch v := v.(type) {
	case string:
		return v, true
	case []byte:
		return string(v), true
	case int8:
		return strconv.FormatInt(int64(v), 10), true
	case int16:
		return strconv.FormatInt(int64(v), 10), true
	case int32:
		return strconv.FormatInt(int64(v), 10), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case int:
		return strconv.Itoa(v), true
	case uint8:
		return strconv.FormatUint(uint64(v), 10), true
	case uint16:
		return strconv.FormatUint(uint64(v), 10), true
	case uint32:
		return strconv.FormatUint(uint64(v), 10), true
	case uint64:
		return strconv.FormatUint(v, 10), true
	case float32:
		return formatFloat(float64(v), 32), true
	case float64:
		return formatFloat(v, 64), true
	case time.Time:
		return v.Format("2006-01-02 15:04:05.999999"), true
	case *replication.JsonDiff:
		return "", false
	case fmt.Stringer: // decimal.Decimal, if enabled
		return v.String(), true
	}
	return fmt.Sprint(v), true
}

func formatFloat(f float64, bits int) string {
	if a := math.Abs(f); a != 0 && (a < 1e-6 || a >= 1e21) {
		return strconv.FormatFloat(f, 'g', -1, bits)
	}
	return strconv.FormatFloat(f, 'f', -1, bits)
}

// trimSQL removes leading whitespace and /* comments */ and a trailing ";".
func trimSQL(q string) string {
	for {
		q = strings.TrimSpace(q)
		if !strings.HasPrefix(q, "/*") {
			break
		}
		end := strings.Index(q, "*/")
		if end < 0 {
			return ""
		}
		q = q[end+2:]
	}
	return strings.TrimSpace(strings.TrimSuffix(q, ";"))
}

func firstWord(q string) string {
	if i := strings.IndexFunc(q, func(r rune) bool { return r == ' ' || r == '\t' || r == '\n' || r == '\r' || r == '(' }); i >= 0 {
		return q[:i]
	}
	return q
}

// truncateTarget parses "TRUNCATE [TABLE] [db.]name"; names may be quoted
// with backticks.
func truncateTarget(q, defaultSchema string) (schema, table string, ok bool) {
	rest := strings.TrimSpace(q[len("TRUNCATE"):])
	if w := firstWord(rest); strings.EqualFold(w, "TABLE") {
		rest = strings.TrimSpace(rest[len(w):])
	}
	parts, ok := splitIdent(rest)
	switch {
	case !ok:
		return "", "", false
	case len(parts) == 1:
		return defaultSchema, parts[0], defaultSchema != ""
	case len(parts) == 2:
		return parts[0], parts[1], true
	}
	return "", "", false
}

// splitIdent splits a possibly quoted, possibly qualified identifier such
// as `db`.`t` or db.t, stopping at whitespace.
func splitIdent(s string) ([]string, bool) {
	var parts []string
	for {
		var name string
		if strings.HasPrefix(s, "`") {
			var b strings.Builder
			i := 1
			for ; i < len(s); i++ {
				if s[i] == '`' {
					if i+1 < len(s) && s[i+1] == '`' {
						b.WriteByte('`')
						i++
						continue
					}
					break
				}
				b.WriteByte(s[i])
			}
			if i >= len(s) {
				return nil, false
			}
			name, s = b.String(), s[i+1:]
		} else {
			end := strings.IndexAny(s, ". \t\r\n;")
			if end < 0 {
				end = len(s)
			}
			name, s = s[:end], s[end:]
		}
		if name == "" {
			return nil, false
		}
		parts = append(parts, name)
		if !strings.HasPrefix(s, ".") {
			return parts, true
		}
		s = s[1:]
	}
}
