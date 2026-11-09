package dbsync

import (
	"github.com/jackc/pglogrepl"
)

type relation struct {
	name  string
	cols  []string
	rules []*rule
}

// pgProcessor turns pgoutput messages into one batch per transaction. It has
// no I/O, so it can be tested with hand-made messages.
type pgProcessor struct {
	rules map[string][]*rule // "schema.table" → rules
	rels  map[uint32]*relation
	inTx  bool
	c     *collector
}

func newPgProcessor(rules []*rule, warn func(string, ...any)) *pgProcessor {
	p := &pgProcessor{rules: map[string][]*rule{}, rels: map[uint32]*relation{}, c: newCollector(warn)}
	for _, r := range rules {
		p.rules[r.name()] = append(p.rules[r.name()], r)
	}
	return p
}

// handle processes one message. On COMMIT it returns the transaction's batch
// and the LSN to acknowledge once it is invalidated.
func (p *pgProcessor) handle(m pglogrepl.Message) (*batch, pglogrepl.LSN) {
	switch m := m.(type) {
	case *pglogrepl.RelationMessage:
		name := m.Namespace + "." + m.RelationName
		rel := &relation{name: name, rules: p.rules[name]}
		for _, c := range m.Columns {
			rel.cols = append(rel.cols, c.Name)
		}
		p.rels[m.RelationID] = rel
	case *pglogrepl.BeginMessage:
		p.inTx = true
		p.c.reset()
	case *pglogrepl.InsertMessage:
		if rel := p.rels[m.RelationID]; rel != nil {
			p.c.row(rel.rules, opInsert, rel.row(m.Tuple), nil)
		}
	case *pglogrepl.UpdateMessage:
		if rel := p.rels[m.RelationID]; rel != nil {
			p.c.row(rel.rules, opUpdate, rel.row(m.NewTuple), rel.row(m.OldTuple))
		}
	case *pglogrepl.DeleteMessage:
		if rel := p.rels[m.RelationID]; rel != nil {
			p.c.row(rel.rules, opDelete, nil, rel.row(m.OldTuple))
		}
	case *pglogrepl.TruncateMessage:
		for _, id := range m.RelationIDs {
			if rel := p.rels[id]; rel != nil {
				p.c.truncate(rel.rules, rel.name)
			}
		}
	case *pglogrepl.CommitMessage:
		p.inTx = false
		return p.c.take(), m.TransactionEndLSN
	}
	return nil, 0
}

// row maps a tuple's columns to the relation's column names. It returns nil
// for a missing tuple.
func (rel *relation) row(t *pglogrepl.TupleData) Row {
	if t == nil {
		return nil
	}
	row := make(Row, len(t.Columns))
	for i, c := range t.Columns {
		if i >= len(rel.cols) {
			break
		}
		switch c.DataType {
		case 'n':
			row[rel.cols[i]] = NullValue()
		case 'u':
			row[rel.cols[i]] = UnchangedValue()
		default: // 't' text, 'b' binary (not requested)
			row[rel.cols[i]] = TextValue(string(c.Data))
		}
	}
	return row
}
