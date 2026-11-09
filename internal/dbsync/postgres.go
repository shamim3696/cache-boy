package dbsync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// pgSource streams PostgreSQL changes over logical replication (pgoutput).
// Progress is kept in a replication slot, so nothing is missed across
// restarts.
type pgSource struct {
	cfg   *SourceConfig
	rules []*rule
	log   *log.Logger
}

func newPostgres(sc *SourceConfig, logger *log.Logger) (*pgSource, error) {
	rules, err := compileRules(sc, "public")
	if err != nil {
		return nil, err
	}
	return &pgSource{cfg: sc, rules: rules, log: logger}, nil
}

func (s *pgSource) name() string { return s.cfg.Name }

const statusInterval = 10 * time.Second

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

func pgConnect(ctx context.Context, raw string) (*pgconn.PgConn, error) {
	return pgconn.Connect(ctx, replicationURL(raw))
}

func (s *pgSource) session(ctx context.Context, k *sink) error {
	conn, err := pgConnect(ctx, s.cfg.URL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	if err := s.ensurePublication(ctx, conn); err != nil {
		return err
	}
	s.checkReplicaIdentity(ctx, conn)
	if err := s.ensureSlot(ctx, conn); err != nil {
		return err
	}
	pub := s.cfg.Publication
	err = pglogrepl.StartReplication(ctx, conn, s.cfg.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + pub + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	s.log.Printf("%s: streaming changes from slot %q (publication %q, %d rules)", s.name(), s.cfg.Slot, pub, len(s.rules))
	return s.stream(ctx, conn, k)
}

func (s *pgSource) stream(ctx context.Context, conn *pgconn.PgConn, k *sink) error {
	p := newPgProcessor(s.rules, func(f string, a ...any) { s.log.Printf(s.name()+": "+f, a...) })
	var acked pglogrepl.LSN // everything up to here is handled
	status := func() error {
		return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: acked})
	}
	next := time.Now().Add(statusInterval)
	for {
		if !time.Now().Before(next) {
			if err := status(); err != nil {
				return fmt.Errorf("status update: %w", err)
			}
			next = time.Now().Add(statusInterval)
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
				// the WAL up to the server's position can be released.
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
				if b, end := p.handle(m); b != nil {
					if err := k.flush(ctx, s.name(), b, status); err != nil {
						return err
					}
					acked = end
				}
			}
		}
	}
}

// --- setup ---

func pgQuery(ctx context.Context, conn *pgconn.PgConn, sql string) ([][][]byte, error) {
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

func tableSQL(r *rule) string { return quoteIdent(r.schema) + "." + quoteIdent(r.table) }

func (s *pgSource) ensurePublication(ctx context.Context, conn *pgconn.PgConn) error {
	pub := s.cfg.Publication
	rows, err := pgQuery(ctx, conn, "SELECT puballtables FROM pg_publication WHERE pubname = "+quoteLit(pub))
	if err != nil {
		return fmt.Errorf("check publication: %w", err)
	}
	if len(rows) == 0 {
		var names []string
		for _, r := range distinctTables(s.rules) {
			names = append(names, tableSQL(r))
		}
		sql := "CREATE PUBLICATION " + quoteIdent(pub) + " FOR TABLE " + strings.Join(names, ", ")
		if _, err := pgQuery(ctx, conn, sql); err != nil {
			return fmt.Errorf("create publication %q: %w", pub, err)
		}
		s.log.Printf("%s: created publication %q for %s", s.name(), pub, strings.Join(names, ", "))
		return nil
	}
	if string(rows[0][0]) == "t" {
		return nil // FOR ALL TABLES
	}
	rows, err = pgQuery(ctx, conn, "SELECT schemaname, tablename FROM pg_publication_tables WHERE pubname = "+quoteLit(pub))
	if err != nil {
		return fmt.Errorf("check publication tables: %w", err)
	}
	have := map[string]bool{}
	for _, r := range rows {
		have[string(r[0])+"."+string(r[1])] = true
	}
	for _, r := range distinctTables(s.rules) {
		if have[r.name()] {
			continue
		}
		if _, err := pgQuery(ctx, conn, "ALTER PUBLICATION "+quoteIdent(pub)+" ADD TABLE "+tableSQL(r)); err != nil {
			return fmt.Errorf("add %s to publication %q: %w", r.name(), pub, err)
		}
		s.log.Printf("%s: added %s to publication %q", s.name(), r.name(), pub)
	}
	return nil
}

// checkReplicaIdentity warns when updates or deletes can't produce every tag.
// Postgres only sends the old row's key columns unless the table has
// REPLICA IDENTITY FULL, so a tag built from another column (orders'
// user_id, say) can't be computed when a row is deleted.
func (s *pgSource) checkReplicaIdentity(ctx context.Context, conn *pgconn.PgConn) {
	for _, r := range distinctTables(s.rules) {
		var cols []string
		for _, rr := range s.rules {
			if rr.name() == r.name() && rr.on&(opUpdate|opDelete) != 0 {
				cols = append(cols, rr.columns()...)
			}
		}
		if len(cols) == 0 {
			continue
		}
		rows, err := pgQuery(ctx, conn, `SELECT c.relreplident,
  COALESCE((SELECT string_agg(a.attname, ',')
            FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = c.oid
              AND ((c.relreplident = 'd' AND i.indisprimary) OR (c.relreplident = 'i' AND i.indisreplident))), '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = `+quoteLit(r.schema)+` AND c.relname = `+quoteLit(r.table))
		if err != nil || len(rows) == 0 || string(rows[0][0]) == "f" {
			continue
		}
		key := map[string]bool{}
		for _, c := range strings.Split(string(rows[0][1]), ",") {
			key[c] = true
		}
		var missing []string
		for _, c := range cols {
			if !key[c] && !contains(missing, c) {
				missing = append(missing, c)
			}
		}
		if len(missing) > 0 {
			s.log.Printf("%s: warning: %s: deletes (and updates that change %s) can't invalidate tags built from %s, because Postgres only sends the old row's key. Fix: ALTER TABLE %s REPLICA IDENTITY FULL;",
				s.name(), r.name(), strings.Join(missing, ", "), strings.Join(missing, ", "), tableSQL(r))
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

func (s *pgSource) ensureSlot(ctx context.Context, conn *pgconn.PgConn) error {
	slot := s.cfg.Slot
	rows, err := pgQuery(ctx, conn, "SELECT plugin FROM pg_replication_slots WHERE slot_name = "+quoteLit(slot))
	if err != nil {
		return fmt.Errorf("check slot: %w", err)
	}
	if len(rows) > 0 {
		if p := string(rows[0][0]); p != "pgoutput" {
			return fmt.Errorf("replication slot %q uses plugin %q, not pgoutput; pick another slot name", slot, p)
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
	s.log.Printf("%s: created replication slot %q; changes from now on are tracked", s.name(), slot)
	return nil
}

func uninstallPostgres(ctx context.Context, sc *SourceConfig, logger *log.Logger) error {
	conn, err := pgConnect(ctx, sc.URL)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())
	rows, err := pgQuery(ctx, conn, "SELECT 1 FROM pg_replication_slots WHERE slot_name = "+quoteLit(sc.Slot))
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		if err := pglogrepl.DropReplicationSlot(ctx, conn, sc.Slot, pglogrepl.DropReplicationSlotOptions{Wait: true}); err != nil {
			return fmt.Errorf("drop slot %q: %w", sc.Slot, err)
		}
		logger.Printf("%s: dropped replication slot %q", sc.Name, sc.Slot)
	} else {
		logger.Printf("%s: replication slot %q doesn't exist", sc.Name, sc.Slot)
	}
	if _, err := pgQuery(ctx, conn, "DROP PUBLICATION IF EXISTS "+quoteIdent(sc.Publication)); err != nil {
		return fmt.Errorf("drop publication %q: %w", sc.Publication, err)
	}
	logger.Printf("%s: dropped publication %q", sc.Name, sc.Publication)
	return nil
}
