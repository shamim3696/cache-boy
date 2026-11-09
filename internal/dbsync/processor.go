package dbsync

import (
	"github.com/jackc/pglogrepl"
)

// batch is what one committed transaction invalidates.
type batch struct {
	tags, keys []string
	end        pglogrepl.LSN // acknowledge up to here once invalidated
	changes    int           // row changes in the transaction that matched a rule
}

type relation struct {
	name  string
	cols  []string
	rules []*rule
}

// processor turns pgoutput messages into one batch per transaction. It has no
// I/O, so it can be tested with hand-made messages.
type processor struct {
	rules map[string][]*rule // "schema.table" → rules
	rels  map[uint32]*relation

	inTx       bool
	tags, keys map[string]struct{}
	changes    int

	warn func(format string, args ...any)
}

func newProcessor(rules []*rule, warn func(string, ...any)) *processor {
	p := &processor{rules: map[string][]*rule{}, rels: map[uint32]*relation{}, warn: warn}
	for _, r := range rules {
		p.rules[r.name()] = append(p.rules[r.name()], r)
	}
	p.reset()
	return p
}

func (p *processor) reset() {
	p.tags, p.keys, p.changes = map[string]struct{}{}, map[string]struct{}{}, 0
}

// handle processes one message. It returns a batch when m is a COMMIT.
func (p *processor) handle(m pglogrepl.Message) *batch {
	switch m := m.(type) {
	case *pglogrepl.RelationMessage:
		rel := &relation{name: m.Namespace + "." + m.RelationName, rules: p.rules[m.Namespace+"."+m.RelationName]}
		for _, c := range m.Columns {
			rel.cols = append(rel.cols, c.Name)
		}
		p.rels[m.RelationID] = rel
	case *pglogrepl.BeginMessage:
		p.inTx = true
		p.reset()
	case *pglogrepl.InsertMessage:
		if rel := p.rels[m.RelationID]; rel != nil {
			p.apply(rel, opInsert, rel.row(m.Tuple), nil)
		}
	case *pglogrepl.UpdateMessage:
		if rel := p.rels[m.RelationID]; rel != nil {
			p.apply(rel, opUpdate, rel.row(m.NewTuple), rel.row(m.OldTuple))
		}
	case *pglogrepl.DeleteMessage:
		if rel := p.rels[m.RelationID]; rel != nil {
			p.apply(rel, opDelete, nil, rel.row(m.OldTuple))
		}
	case *pglogrepl.TruncateMessage:
		for _, id := range m.RelationIDs {
			rel := p.rels[id]
			if rel == nil {
				continue
			}
			for _, r := range rel.rules {
				if r.on&opTruncate == 0 {
					continue
				}
				p.changes++
				dynamic := false
				for _, t := range r.tags {
					if t.static() {
						p.tags[t.raw] = struct{}{}
					} else {
						dynamic = true
					}
				}
				for _, t := range r.keys {
					if t.static() {
						p.keys[t.raw] = struct{}{}
					} else {
						dynamic = true
					}
				}
				if dynamic && p.warn != nil {
					p.warn("TRUNCATE %s: tags and keys with {column} placeholders can't be computed without rows; only static ones were invalidated", rel.name)
				}
			}
		}
	case *pglogrepl.CommitMessage:
		p.inTx = false
		b := &batch{end: m.TransactionEndLSN, changes: p.changes}
		for t := range p.tags {
			b.tags = append(b.tags, t)
		}
		for k := range p.keys {
			b.keys = append(b.keys, k)
		}
		p.reset()
		return b
	}
	return nil
}

// apply adds the tags and keys of every matching rule. For updates both the
// old and the new row count: a row moving from user 1 to user 2 invalidates
// both. Columns Postgres didn't resend (unchanged TOAST values) are taken
// from the old row when it has them.
func (p *processor) apply(rel *relation, o op, newRow, oldRow map[string]value) {
	if newRow != nil && oldRow != nil {
		for c, v := range newRow {
			if v.unchanged {
				if ov, ok := oldRow[c]; ok && !ov.unchanged {
					newRow[c] = ov
				}
			}
		}
	}
	for _, r := range rel.rules {
		if r.on&o == 0 {
			continue
		}
		p.changes++
		for _, row := range []map[string]value{newRow, oldRow} {
			if row == nil {
				continue
			}
			for _, t := range r.tags {
				if s, ok := t.render(row); ok {
					p.tags[s] = struct{}{}
				}
			}
			for _, t := range r.keys {
				if s, ok := t.render(row); ok {
					p.keys[s] = struct{}{}
				}
			}
		}
		// Static templates apply even when no row is available.
		if newRow == nil && oldRow == nil {
			for _, t := range r.tags {
				if t.static() {
					p.tags[t.raw] = struct{}{}
				}
			}
		}
	}
}

// row maps a tuple's columns to the relation's column names. It returns nil
// for a missing tuple.
func (rel *relation) row(t *pglogrepl.TupleData) map[string]value {
	if t == nil {
		return nil
	}
	row := make(map[string]value, len(t.Columns))
	for i, c := range t.Columns {
		if i >= len(rel.cols) {
			break
		}
		switch c.DataType {
		case 'n':
			row[rel.cols[i]] = value{null: true}
		case 'u':
			row[rel.cols[i]] = value{unchanged: true}
		default: // 't' text, 'b' binary (not requested)
			row[rel.cols[i]] = value{text: string(c.Data)}
		}
	}
	return row
}
