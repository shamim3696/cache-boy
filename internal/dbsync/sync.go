package dbsync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Invalidator removes cache entries. It must be safe to call again with the
// same input: after a crash, the last transactions are replayed.
type Invalidator interface {
	Invalidate(ctx context.Context, tags, keys []string) error
}

// Syncer streams changes from Postgres and invalidates the cache.
//
// Delivery is at least once: Postgres is told a transaction was handled only
// after its invalidation succeeded, so if cache-boy is down the stream waits
// (Postgres keeps the WAL) and nothing is skipped.
type Syncer struct {
	cfg     *Config
	rules   []*rule
	inv     Invalidator
	log     *log.Logger
	verbose bool

	// stats
	txns, tagsSent, keysSent int64
}

func New(cfg *Config, inv Invalidator, logger *log.Logger, verbose bool) (*Syncer, error) {
	rules, err := cfg.compile()
	if err != nil {
		return nil, err
	}
	return &Syncer{cfg: cfg, rules: rules, inv: inv, log: logger, verbose: verbose}, nil
}

const (
	statusInterval = 10 * time.Second
	maxBackoff     = 30 * time.Second
	tagChunk       = 500 // tags or keys per command
)

// Run streams until ctx is cancelled, reconnecting after errors.
func (s *Syncer) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		started := time.Now()
		err := s.session(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		s.log.Printf("postgres: %v; reconnecting in %s", err, backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

// replicationURL adds replication=database, which logical replication needs.
func replicationURL(raw string) string {
	if strings.Contains(raw, "replication=") {
		return raw
	}
	if strings.HasPrefix(raw, "postgres://") || strings.HasPrefix(raw, "postgresql://") {
		u, err := url.Parse(raw)
		if err == nil {
			q := u.Query()
			q.Set("replication", "database")
			u.RawQuery = q.Encode()
			return u.String()
		}
	}
	return raw + " replication=database"
}

func connect(ctx context.Context, raw string) (*pgconn.PgConn, error) {
	return pgconn.Connect(ctx, replicationURL(raw))
}

func (s *Syncer) session(ctx context.Context) error {
	conn, err := connect(ctx, s.cfg.Postgres.URL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if err := s.setup(ctx, conn); err != nil {
		return err
	}
	pub := s.cfg.Postgres.Publication
	err = pglogrepl.StartReplication(ctx, conn, s.cfg.Postgres.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + pub + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	s.log.Printf("streaming changes from slot %q (publication %q, %d rules)", s.cfg.Postgres.Slot, pub, len(s.rules))
	return s.stream(ctx, conn)
}

func (s *Syncer) stream(ctx context.Context, conn *pgconn.PgConn) error {
	p := newProcessor(s.rules, s.log.Printf)
	var acked pglogrepl.LSN // everything up to here is handled
	status := func() error {
		return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: acked})
	}
	next := time.Now().Add(statusInterval)
	lastReport := time.Now()
	for {
		if !time.Now().Before(next) {
			if err := status(); err != nil {
				return fmt.Errorf("status update: %w", err)
			}
			next = time.Now().Add(statusInterval)
			if time.Since(lastReport) >= time.Minute && s.txns > 0 {
				s.log.Printf("so far: %d transactions, %d tags and %d keys invalidated", s.txns, s.tagsSent, s.keysSent)
				lastReport = time.Now()
			}
		}
		rctx, cancel := context.WithDeadline(ctx, next)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if pgconn.Timeout(err) {
				continue
			}
			return err
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("%s: %s", msg.Code, msg.Message)
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return err
				}
				// Between transactions everything sent so far is handled, so
				// the whole WAL up to the server's position can be released.
				if !p.inTx && ka.ServerWALEnd > acked {
					acked = ka.ServerWALEnd
				}
				if ka.ReplyRequested {
					next = time.Time{}
				}
			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return err
				}
				m, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					return fmt.Errorf("decode: %w", err)
				}
				if b := p.handle(m); b != nil {
					if err := s.flush(ctx, b, status); err != nil {
						return err
					}
					acked = b.end
				}
			}
		}
	}
}

// flush invalidates one transaction's tags and keys, retrying until it works.
// Between attempts it keeps the replication connection alive.
func (s *Syncer) flush(ctx context.Context, b *batch, status func() error) error {
	if len(b.tags)+len(b.keys) == 0 {
		return nil
	}
	sort.Strings(b.tags)
	sort.Strings(b.keys)
	backoff := 500 * time.Millisecond
	for {
		err := s.invalidate(ctx, b)
		if err == nil {
			s.txns++
			s.tagsSent += int64(len(b.tags))
			s.keysSent += int64(len(b.keys))
			if s.verbose {
				s.log.Printf("commit %s: %d changes → tags %v keys %v", b.end, b.changes, b.tags, b.keys)
			}
			return nil
		}
		s.log.Printf("cache: %v; retrying in %s (Postgres keeps the changes until then)", err, backoff)
		deadline := time.Now().Add(backoff)
		for time.Now().Before(deadline) {
			if err := status(); err != nil {
				return fmt.Errorf("status update: %w", err)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(min(time.Until(deadline), statusInterval)):
			}
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (s *Syncer) invalidate(ctx context.Context, b *batch) error {
	for i := 0; i < len(b.tags) || i < len(b.keys); i += tagChunk {
		var tags, keys []string
		if i < len(b.tags) {
			tags = b.tags[i:min(i+tagChunk, len(b.tags))]
		}
		if i < len(b.keys) {
			keys = b.keys[i:min(i+tagChunk, len(b.keys))]
		}
		if err := s.inv.Invalidate(ctx, tags, keys); err != nil {
			return err
		}
	}
	return nil
}

// --- setup ---

func (s *Syncer) setup(ctx context.Context, conn *pgconn.PgConn) error {
	if err := s.ensurePublication(ctx, conn); err != nil {
		return err
	}
	s.checkReplicaIdentity(ctx, conn)
	return s.ensureSlot(ctx, conn)
}

func query(ctx context.Context, conn *pgconn.PgConn, sql string) ([][][]byte, error) {
	res, err := conn.Exec(ctx, sql).ReadAll()
	if err != nil {
		return nil, err
	}
	var rows [][][]byte
	for _, r := range res {
		if r.Err != nil {
			return nil, r.Err
		}
		rows = append(rows, r.Rows...)
	}
	return rows, nil
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func quoteLit(s string) string   { return `'` + strings.ReplaceAll(s, `'`, `''`) + `'` }

// tables returns the distinct tables the rules watch, sorted.
func (s *Syncer) tables() []*rule {
	seen := map[string]bool{}
	var out []*rule
	for _, r := range s.rules {
		if !seen[r.name()] {
			seen[r.name()] = true
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name() < out[j].name() })
	return out
}

func tableSQL(r *rule) string { return quoteIdent(r.schema) + "." + quoteIdent(r.table) }

func (s *Syncer) ensurePublication(ctx context.Context, conn *pgconn.PgConn) error {
	pub := s.cfg.Postgres.Publication
	rows, err := query(ctx, conn, "SELECT puballtables FROM pg_publication WHERE pubname = "+quoteLit(pub))
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if len(rows) == 0 {
		var names []string
		for _, r := range s.tables() {
			names = append(names, tableSQL(r))
		}
		sql := "CREATE PUBLICATION " + quoteIdent(pub) + " FOR TABLE " + strings.Join(names, ", ")
		if _, err := query(ctx, conn, sql); err != nil {
			return fmt.Errorf("create publication %q: %w", pub, err)
		}
		s.log.Printf("created publication %q for %s", pub, strings.Join(names, ", "))
		return nil
	}
	if string(rows[0][0]) == "t" {
		return nil // FOR ALL TABLES
	}
	rows, err = query(ctx, conn, "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = "+quoteLit(pub))
	if err != nil {
		return fmt.Errorf("check publication tables: %w", err)
	}
	have := map[string]bool{}
	for _, r := range rows {
		have[string(r[0])+"."+string(r[1])] = true
	}
	for _, r := range s.tables() {
		if have[r.name()] {
			continue
		}
		if _, err := query(ctx, conn, "ALTER PUBLICATION "+quoteIdent(pub)+" ADD TABLE "+tableSQL(r)); err != nil {
			return fmt.Errorf("add %s to publication %q: %w", r.name(), pub, err)
		}
		s.log.Printf("added %s to publication %q", r.name(), pub)
	}
	return nil
}

// checkReplicaIdentity warns when updates or deletes can't produce every tag.
// Postgres only sends the old row's key columns unless the table has
// REPLICA IDENTITY FULL, so a tag built from another column (orders'
// user_id, say) can't be computed when a row is deleted.
func (s *Syncer) checkReplicaIdentity(ctx context.Context, conn *pgconn.PgConn) {
	for _, r := range s.tables() {
		var cols []string
		for _, rr := range s.rules {
			if rr.name() == r.name() && rr.on&(opUpdate|opDelete) != 0 {
				cols = append(cols, rr.columns()...)
			}
		}
		if len(cols) == 0 {
			continue
		}
		rows, err := query(ctx, conn, `SELECT c.relreplident,
  COALESCE((SELECT string_agg(a.attname, ',')
            FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = c.oid
              AND ((c.relreplident = 'd' AND i.indisprimary) OR (c.relreplident = 'i' AND i.indisreplident))), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = `+quoteLit(r.schema)+` AND c.relname = `+quoteLit(r.table))
		if err != nil || len(rows) == 0 {
			continue
		}
		ident := string(rows[0][0])
		if ident == "f" {
			continue
		}
		key := map[string]bool{}
		for _, k := range strings.Split(string(rows[0][1]), ",") {
			key[k] = true
		}
		var missing []string
		for _, c := range cols {
			if !key[c] && !contains(missing, c) {
				missing = append(missing, c)
			}
		}
		if len(missing) > 0 {
			s.log.Printf("warning: %s: deletes (and updates that change %s) can't invalidate tags built from %s, because Postgres only sends the old row's key. Fix: ALTER TABLE %s REPLICA IDENTITY FULL;",
				r.name(), strings.Join(missing, ", "), strings.Join(missing, ", "), tableSQL(r))
		}
	}
}

func contains(ss []string, s string) bool {
	for _, x := range ss {
		if x == s {
			return true
		}
	}
	return false
}

func (s *Syncer) ensureSlot(ctx context.Context, conn *pgconn.PgConn) error {
	slot := s.cfg.Postgres.Slot
	rows, err := query(ctx, conn, "SELECT plugin FROM pg_replication_slots WHERE slot_name = "+quoteLit(slot))
	if err != nil {
		return fmt.Errorf("check slot: %w", err)
	}
	if len(rows) > 0 {
		if p := string(rows[0][0]); p != "pgoutput" {
			return fmt.Errorf("replication slot %q uses plugin %q, not pgoutput; pick another postgres.slot", slot, p)
		}
		return nil
	}
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{
		Mode:           pglogrepl.LogicalReplication,
		SnapshotAction: "NOEXPORT_SNAPSHOT",
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "55000" {
			return fmt.Errorf("create replication slot: %w (set wal_level = logical in postgresql.conf and restart Postgres)", err)
		}
		return fmt.Errorf("create replication slot %q: %w", slot, err)
	}
	s.log.Printf("created replication slot %q; changes from now on are tracked", slot)
	return nil
}

// Uninstall drops the replication slot and the publication. A slot that is
// no longer read makes Postgres keep WAL forever, so run this when you stop
// using cache-boy-sync.
func Uninstall(ctx context.Context, cfg *Config, logger *log.Logger) error {
	conn, err := connect(ctx, cfg.Postgres.URL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	slot, pub := cfg.Postgres.Slot, cfg.Postgres.Publication
	rows, err := query(ctx, conn, "SELECT 1 FROM pg_replication_slots WHERE slot_name = "+quoteLit(slot))
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		if err := pglogrepl.DropReplicationSlot(ctx, conn, slot, pglogrepl.DropReplicationSlotOptions{Wait: true}); err != nil {
			return fmt.Errorf("drop slot %q: %w", slot, err)
		}
		logger.Printf("dropped replication slot %q", slot)
	} else {
		logger.Printf("replication slot %q doesn't exist", slot)
	}
	if _, err := query(ctx, conn, "DROP PUBLICATION IF EXISTS "+quoteIdent(pub)); err != nil {
		return fmt.Errorf("drop publication %q: %w", pub, err)
	}
	logger.Printf("dropped publication %q", pub)
	return nil
}
