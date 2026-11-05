package command

import (
	"testing"
)

func ftHistoryValues(t *testing.T, e *Engine, c *Client, args ...string) (versions, values []string) {
	t.Helper()
	v := do(e, c, args...)
	if v.IsError() {
		t.Fatalf("%v: %s", args, v.Str)
	}
	for _, el := range v.Elems {
		s := ftStrs(el)
		versions = append(versions, s[0])
		values = append(values, s[2])
		if el.Elems[1].Int <= 0 {
			t.Fatalf("bad timestamp %v", el)
		}
	}
	return
}

func TestFeatureHistory(t *testing.T) {
	e := newTestEngine(t)
	c := e.NewClient("tcp", "test")
	rec := &ftRecorder{}
	e.AddPropagator(rec)

	// Disabled by default.
	do(e, c, "_ft.set", "off", "1")
	ftArr(t, e, c, []string{}, "HISTORY", "off")

	mustStr(t, e, c, "OK", "CONFIG", "SET", "history-depth", "3")
	do(e, c, "_ft.set", "k", "v1")
	do(e, c, "_ft.set", "k", "v2")
	do(e, c, "_ft.set", "k", "v2") // same value: no new version
	do(e, c, "_ft.del", "k")
	do(e, c, "_ft.set", "k", "v3")
	vers, vals := ftHistoryValues(t, e, c, "HISTORY", "k")
	if !ftEq(vers, []string{"4", "3", "2"}) || !ftEq(vals, []string{"v3", "<nil>", "v2"}) {
		t.Fatalf("HISTORY %v %v", vers, vals)
	}
	vers, _ = ftHistoryValues(t, e, c, "HISTORY", "k", "COUNT", "1")
	if !ftEq(vers, []string{"4"}) {
		t.Fatalf("HISTORY COUNT %v", vers)
	}
	mustStr(t, e, c, "v2", "GETVERSION", "k", "2")
	if v := do(e, c, "GETVERSION", "k", "1"); !v.IsNull() { // trimmed
		t.Fatalf("GETVERSION 1: %v", v)
	}
	if v := do(e, c, "GETVERSION", "k", "3"); !v.IsNull() { // deletion
		t.Fatalf("GETVERSION 3: %v", v)
	}

	before := len(rec.all())
	mustStr(t, e, c, "OK", "RESTOREVERSION", "k", "2")
	mustStr(t, e, c, "v2", "_ft.get", "k")
	if got := rec.all()[before:]; len(got) != 1 || !ftEq(got[0], []string{"SET", "k", "v2"}) {
		t.Fatalf("propagated %q", got)
	}
	vers, vals = ftHistoryValues(t, e, c, "HISTORY", "k")
	if vers[0] != "5" || vals[0] != "v2" {
		t.Fatalf("after restore %v %v", vers, vals)
	}
	mustStr(t, e, c, "OK", "RESTOREVERSION", "k", "3") // a deletion
	mustStr(t, e, c, "", "_ft.get", "k")
	mustErr(t, e, c, "RESTOREVERSION", "k", "99")
	mustErr(t, e, c, "GETVERSION", "k", "abc")

	// History survives the key, and is per namespace.
	app := e.NewClient("tcp", "app")
	mustStr(t, e, app, "OK", "NAMESPACE", "USE", "app")
	ftArr(t, e, app, []string{}, "HISTORY", "k")
	do(e, app, "_ft.set", "k", "app-v")
	_, vals = ftHistoryValues(t, e, app, "HISTORY", "k")
	if !ftEq(vals, []string{"app-v"}) {
		t.Fatalf("namespaced history %v", vals)
	}

	// Non-string values are ignored.
	do(e, c, "_ft.hset", "h", "f", "v")
	ftArr(t, e, c, []string{}, "HISTORY", "h")

	mustStr(t, e, c, "1", "HISTORY.CLEAR", "k")
	mustStr(t, e, c, "0", "HISTORY.CLEAR", "k")
	ftArr(t, e, c, []string{}, "HISTORY", "k")

	// FLUSHALL records deletions.
	do(e, c, "_ft.set", "f", "x")
	mustStr(t, e, c, "OK", "FLUSHALL")
	_, vals = ftHistoryValues(t, e, c, "HISTORY", "f")
	if !ftEq(vals, []string{"<nil>", "x"}) {
		t.Fatalf("after FLUSHALL %v", vals)
	}
}
