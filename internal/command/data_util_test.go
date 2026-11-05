package command

import (
	"strings"
	"sync"
	"testing"

	"cache-boy/internal/protocol/resp"
)

// render turns a reply into a compact string for table tests:
// nil for null, [a b] for arrays and sets, {k:v} for maps, !MSG for errors.
func render(v resp.Value) string {
	switch v.Type {
	case resp.TypeNull:
		return "nil"
	case resp.TypeError:
		return "!" + v.Str
	case resp.TypeArray, resp.TypeSet, resp.TypePush:
		parts := make([]string, len(v.Elems))
		for i, e := range v.Elems {
			parts[i] = render(e)
		}
		return "[" + strings.Join(parts, " ") + "]"
	case resp.TypeMap:
		parts := make([]string, 0, len(v.Elems)/2)
		for i := 0; i+1 < len(v.Elems); i += 2 {
			parts = append(parts, render(v.Elems[i])+":"+render(v.Elems[i+1]))
		}
		return "{" + strings.Join(parts, " ") + "}"
	}
	return v.String()
}

// tc is one command (split on spaces) and its expected rendered reply. A
// want starting with "!" matches any error whose message starts with the rest.
type tc struct {
	cmd  string
	want string
}

func runCases(t *testing.T, e *Engine, c *Client, cases []tc) {
	t.Helper()
	for _, k := range cases {
		got := render(do(e, c, strings.Fields(k.cmd)...))
		if strings.HasPrefix(k.want, "!") {
			if !strings.HasPrefix(got, k.want) {
				t.Errorf("%s: got %q, want error %q", k.cmd, got, k.want)
			}
			continue
		}
		if got != k.want {
			t.Errorf("%s: got %q, want %q", k.cmd, got, k.want)
		}
	}
}

// recorder is a Propagator that remembers what was propagated.
type recorder struct {
	mu   sync.Mutex
	cmds []string
}

func (r *recorder) Propagate(args []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds = append(r.cmds, strings.Join(args, " "))
}

// take returns and clears the recorded commands.
func (r *recorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.cmds
	r.cmds = nil
	return out
}

func newDataEngine(t *testing.T) (*Engine, *Client) {
	t.Helper()
	e := newTestEngine(t)
	return e, e.NewClient("tcp", "test")
}
