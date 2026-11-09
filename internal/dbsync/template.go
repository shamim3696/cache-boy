package dbsync

import (
	"fmt"
	"strings"
)

// template is a tag or key pattern such as "user:{id}" or "users:list".
// Each {column} is replaced with that column's value from the changed row.
type template struct {
	raw   string
	parts []part
	cols  []string // placeholder columns, in order
}

type part struct {
	lit string
	col string // set for a placeholder, lit is then unused
}

func parseTemplate(s string) (*template, error) {
	if s == "" {
		return nil, fmt.Errorf("empty tag or key")
	}
	t := &template{raw: s}
	rest := s
	for rest != "" {
		open := strings.IndexByte(rest, '{')
		close := strings.IndexByte(rest, '}')
		if open < 0 {
			if close >= 0 {
				return nil, fmt.Errorf("%q: unmatched '}'", s)
			}
			t.parts = append(t.parts, part{lit: rest})
			break
		}
		if close >= 0 && close < open {
			return nil, fmt.Errorf("%q: unmatched '}'", s)
		}
		if open > 0 {
			t.parts = append(t.parts, part{lit: rest[:open]})
		}
		end := strings.IndexByte(rest[open:], '}')
		if end < 0 {
			return nil, fmt.Errorf("%q: unclosed '{'", s)
		}
		col := rest[open+1 : open+end]
		if col == "" || strings.ContainsAny(col, "{ ") {
			return nil, fmt.Errorf("%q: bad column name %q", s, col)
		}
		t.parts = append(t.parts, part{col: col})
		t.cols = append(t.cols, col)
		rest = rest[open+end+1:]
	}
	return t, nil
}

// static reports whether the template has no placeholders, i.e. it names the
// same tag for every row (e.g. "users:list").
func (t *template) static() bool { return len(t.cols) == 0 }

// value is one column of a changed row.
type value struct {
	text      string
	null      bool
	unchanged bool // an unchanged TOASTed value: Postgres didn't send it
}

// render fills in the placeholders from row. It returns false when a column
// is missing, NULL or was not sent, since the tag can't be known then.
func (t *template) render(row map[string]value) (string, bool) {
	if t.static() {
		return t.raw, true
	}
	var b strings.Builder
	for _, p := range t.parts {
		if p.col == "" {
			b.WriteString(p.lit)
			continue
		}
		v, ok := row[p.col]
		if !ok || v.null || v.unchanged {
			return "", false
		}
		b.WriteString(v.text)
	}
	return b.String(), true
}
