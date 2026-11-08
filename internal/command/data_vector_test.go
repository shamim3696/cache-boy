package command

import (
	"math"
	"testing"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

func TestVectorCommands(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"VCARD v", "0"},
		{"VDIM v", "0"},
		{"VADD v a 1 0 0", "1"},
		{`VADD v b 0 1 0 ATTR {"color":"red","n":2}`, "1"},
		{`VADD v c 1 1 0 ATTR {"color":"blue"}`, "1"},
		{"VADD v a 1 0 0", "0"},
		{"VADD v d 1 0", "!ERR vector dimension 2 does not match index dimension 3"},
		{"VADD v d 1 x 0", "!ERR invalid vector component"},
		{"VADD v d ATTR {}", "!ERR zero-length vector"},
		{"VADD v d 1 0 0 ATTR {bad", "!ERR ATTR is not valid JSON"},
		{"VADD v d 1 0 0 ATTR {} extra", "!ERR syntax error"},
		{"VADD fresh d 1 x", "!ERR invalid vector component"},
		{"EXISTS fresh", "0"},
		{"VCARD v", "3"},
		{"VDIM v", "3"},
		{"VGET v b", "[0 1 0]"},
		{"VADD v e 0.5 -2.25 0", "1"},
		{"VGET v e", "[0.5 -2.25 0]"},
		{"VGET v nope", "nil"},
		{"VGET nope a", "nil"},
		{"VSEARCH v 1 1 0 0", "[[a 1]]"},
		{"VSEARCH v 0 1 0 0", "[]"},
		{"VSEARCH v 5 1 0 0 FILTER color=red", "[[b 0]]"},
		{"VSEARCH v 5 0 1 0 FILTER n=2", "[[b 1]]"},
		{"VSEARCH v 5 0 1 0 FILTER n=2.0", "[[b 1]]"},
		{"VSEARCH v 5 0 1 0 FILTER color=green", "[]"},
		{"VSEARCH v 5 0 1 0 FILTER nonsense", "!ERR FILTER must be attr=value"},
		{"VSEARCH v 5 0 1 0 FILTER a=1 extra", "!ERR syntax error"},
		{"VSEARCH v 2 1 0", "!ERR query dimension 2 does not match index dimension 3"},
		{"VSEARCH v -1 1 0 0", "!ERR K must be a non-negative integer"},
		{"VSEARCH v x 1 0 0", "!ERR K must be a non-negative integer"},
		{"VSEARCH nope 2 1 0 0", "[]"},
		{"VDEL v a", "1"},
		{"VDEL v a", "0"},
		{"VDEL nope a", "0"},
		{"VDEL v b", "1"},
		{"VDEL v c", "1"},
		{"VDEL v e", "1"},
		{"EXISTS v", "0"},
		{"VADD v a 1 2", "1"},
		{"VDIM v", "2"},
		{"SET str v", "OK"},
		{"VADD str a 1", "!WRONGTYPE"},
		{"VSEARCH str 1 1", "!WRONGTYPE"},
		{"VCARD str", "!WRONGTYPE"},
		{"TYPE v", "vectorset"},
	})
}

func TestVectorSearchOrder(t *testing.T) {
	e, c := newDataEngine(t)
	do(e, c, "VADD", "v", "x", "1", "0")
	do(e, c, "VADD", "v", "diag", "1", "1")
	do(e, c, "VADD", "v", "y", "0", "1")
	do(e, c, "VADD", "v", "negx", "-1", "0")
	v := do(e, c, "VSEARCH", "v", "3", "1", "0.1")
	if len(v.Elems) != 3 {
		t.Fatalf("VSEARCH: %s", render(v))
	}
	wantIDs := []string{"x", "diag", "y"}
	prev := math.Inf(1)
	for i, r := range v.Elems {
		if r.Elems[0].Str != wantIDs[i] || r.Elems[1].Type != resp.TypeDouble || r.Elems[1].Float > prev {
			t.Fatalf("VSEARCH order: %s", render(v))
		}
		prev = r.Elems[1].Float
	}
	if s := v.Elems[1].Elems[1].Float; math.Abs(s-1.1/math.Sqrt(2)/math.Sqrt(1.01)) > 1e-5 {
		t.Fatalf("cosine similarity of diag: %v", s)
	}
}
