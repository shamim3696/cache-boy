package dbsync

// Row is one changed row or document: column (or dotted field path) → value.
type Row map[string]value

// TextValue, NullValue and UnchangedValue build Row values for sources.
func TextValue(s string) value { return value{text: s} }
func NullValue() value         { return value{null: true} }
func UnchangedValue() value    { return value{unchanged: true} }

// batch is what one committed transaction (or group of changes) invalidates.
type batch struct {
	tags, keys []string
	changes    int // row changes that matched a rule
}

// collector accumulates the tags and keys of changed rows until a commit.
// It is shared by all sources and has no I/O.
type collector struct {
	tags, keys map[string]struct{}
	changes    int
	warn       func(format string, args ...any)
}

func newCollector(warn func(string, ...any)) *collector {
	c := &collector{warn: warn}
	c.reset()
	return c
}

func (c *collector) reset() {
	c.tags, c.keys, c.changes = map[string]struct{}{}, map[string]struct{}{}, 0
}

// empty reports whether nothing has been collected since the last take.
func (c *collector) empty() bool { return c.changes == 0 && len(c.tags) == 0 && len(c.keys) == 0 }

// row adds the tags and keys of every rule that matches o. For updates both
// the old and the new row count: a row moving from user 1 to user 2
// invalidates both. Unchanged values in newRow (not resent by the database)
// are filled from oldRow when it has them.
func (c *collector) row(rules []*rule, o op, newRow, oldRow Row) {
	if newRow != nil && oldRow != nil {
		for k, v := range newRow {
			if v.unchanged {
				if ov, ok := oldRow[k]; ok && !ov.unchanged {
					newRow[k] = ov
				}
			}
		}
	}
	for _, r := range rules {
		if r.on&o == 0 {
			continue
		}
		c.changes++
		for _, row := range []Row{newRow, oldRow} {
			if row == nil {
				continue
			}
			for _, t := range r.tags {
				if s, ok := t.render(row); ok {
					c.tags[s] = struct{}{}
				}
			}
			for _, t := range r.keys {
				if s, ok := t.render(row); ok {
					c.keys[s] = struct{}{}
				}
			}
		}
		if newRow == nil && oldRow == nil {
			c.static(r)
		}
	}
}

// truncate handles a change without rows (TRUNCATE, a dropped collection):
// only static tags and keys can be invalidated.
func (c *collector) truncate(rules []*rule, table string) {
	for _, r := range rules {
		if r.on&opTruncate == 0 {
			continue
		}
		c.changes++
		if c.static(r) && c.warn != nil {
			c.warn("%s was truncated or dropped: tags and keys with {column} placeholders can't be computed without rows; only static ones were invalidated", table)
		}
	}
}

// static adds the rule's templates without placeholders and reports whether
// it skipped any with placeholders.
func (c *collector) static(r *rule) (skipped bool) {
	for _, t := range r.tags {
		if t.static() {
			c.tags[t.raw] = struct{}{}
		} else {
			skipped = true
		}
	}
	for _, t := range r.keys {
		if t.static() {
			c.keys[t.raw] = struct{}{}
		} else {
			skipped = true
		}
	}
	return skipped
}

// take returns the collected batch and starts a new one.
func (c *collector) take() *batch {
	b := &batch{changes: c.changes}
	for t := range c.tags {
		b.tags = append(b.tags, t)
	}
	for k := range c.keys {
		b.keys = append(b.keys, k)
	}
	c.reset()
	return b
}
