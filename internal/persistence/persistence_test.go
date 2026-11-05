package persistence_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/config"
	"cache-boy/internal/persistence"
	"cache-boy/internal/protocol/resp"
	"cache-boy/internal/store"
)

func init() {
	command.Register(&command.Spec{Name: "ptset", Arity: 3, Flags: command.FlagWrite | command.FlagDenyOOM, FirstKey: 1, LastKey: 1, Group: "test",
		Handler: func(x *command.Ctx) resp.Value {
			x.DB().Put(x.Arg(1), x.DB().NewStringValue([]byte(x.Arg(2))))
			return resp.OK
		}})
}

func engine(t *testing.T, dir string, appendonly bool, save []config.SaveRule) *command.Engine {
	t.Helper()
	cfg := config.Default()
	cfg.Dir = dir
	cfg.AppendOnly = appendonly
	cfg.Save = save
	e := command.New(config.NewStore(cfg))
	t.Cleanup(e.Stop)
	return e
}

func get(e *command.Engine, key string) string {
	unlock := e.DB.LockAll()
	defer unlock()
	if en := e.DB.Peek(key); en != nil && en.Kind == store.KindString {
		return string(en.Str().Bytes())
	}
	return ""
}

func do(t *testing.T, e *command.Engine, c *command.Client, args ...string) resp.Value {
	t.Helper()
	v := e.Execute(c, args)
	if v.IsError() {
		t.Fatalf("%v: %s", args, v.Str)
	}
	return v
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func info(m *persistence.Manager, key string) string {
	for _, kv := range m.Info() {
		if kv[0] == key {
			return kv[1]
		}
	}
	return ""
}

func TestSnapshotOnly(t *testing.T) {
	dir := t.TempDir()
	e := engine(t, dir, false, nil)
	m, err := persistence.Setup(e)
	if err != nil {
		t.Fatal(err)
	}
	if e.Persist == nil {
		t.Fatal("e.Persist not set")
	}
	c := e.InternalClient("test", false)
	do(t, e, c, "ptset", "a", "1")
	do(t, e, c, "SAVE")
	if e.DB.Dirty() != 0 {
		t.Fatalf("dirty after SAVE: %d", e.DB.Dirty())
	}
	do(t, e, c, "ptset", "b", "2")
	if v := do(t, e, c, "BGSAVE"); v.Str != "Background saving started" {
		t.Fatalf("BGSAVE: %v", v)
	}
	waitFor(t, "bgsave", func() bool { return info(m, "rdb_bgsave_in_progress") == "0" && e.DB.Dirty() == 0 })
	if v := do(t, e, c, "LASTSAVE"); v.Int < time.Now().Unix()-5 {
		t.Fatalf("LASTSAVE: %v", v)
	}
	if v := e.Execute(c, []string{"BGREWRITEAOF"}); !v.IsError() {
		t.Fatal("BGREWRITEAOF without AOF should fail")
	}
	if info(m, "rdb_last_bgsave_status") != "ok" || info(m, "aof_enabled") != "0" {
		t.Fatalf("info: %v", m.Info())
	}
	do(t, e, c, "ptset", "c", "3")
	if err := m.Close(true); err != nil {
		t.Fatal(err)
	}

	e2 := engine(t, dir, false, nil)
	if _, err := persistence.Setup(e2); err != nil {
		t.Fatal(err)
	}
	if get(e2, "a") != "1" || get(e2, "b") != "2" || get(e2, "c") != "3" {
		t.Fatal("snapshot not reloaded")
	}
	if e2.DB.Dirty() != 0 {
		t.Fatal("loading counted as changes")
	}
}

func TestAOFAndRewrite(t *testing.T) {
	dir := t.TempDir()
	e := engine(t, dir, true, nil)
	m, err := persistence.Setup(e)
	if err != nil {
		t.Fatal(err)
	}
	c := e.InternalClient("test", false)
	for i := range 100 {
		do(t, e, c, "ptset", "k", strings.Repeat("x", i))
	}
	do(t, e, c, "BGREWRITEAOF")
	waitFor(t, "rewrite", func() bool { return info(m, "aof_rewrite_in_progress") == "0" })
	do(t, e, c, "ptset", "after", "yes")
	if info(m, "aof_last_bgrewrite_status") != "ok" {
		t.Fatalf("info: %v", m.Info())
	}
	if err := m.Close(false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "dump.cbdump")); !os.IsNotExist(err) {
		t.Fatal("Close(false) wrote a snapshot")
	}

	e2 := engine(t, dir, true, nil)
	m2, err := persistence.Setup(e2)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close(false)
	if get(e2, "k") != strings.Repeat("x", 99) || get(e2, "after") != "yes" {
		t.Fatalf("AOF not reloaded: k=%q after=%q", get(e2, "k"), get(e2, "after"))
	}
}

func TestSnapshotToAOFMigration(t *testing.T) {
	dir := t.TempDir()
	e := engine(t, dir, false, nil)
	m, _ := persistence.Setup(e)
	do(t, e, e.InternalClient("t", false), "ptset", "kept", "1")
	m.Close(true)

	// Turning appendonly on with only a snapshot: data is loaded from the
	// snapshot and written into the new AOF.
	e2 := engine(t, dir, true, nil)
	m2, err := persistence.Setup(e2)
	if err != nil {
		t.Fatal(err)
	}
	m2.Close(false)
	os.Remove(filepath.Join(dir, "dump.cbdump"))
	e3 := engine(t, dir, true, nil)
	m3, err := persistence.Setup(e3)
	if err != nil {
		t.Fatal(err)
	}
	defer m3.Close(false)
	if get(e3, "kept") != "1" {
		t.Fatal("data lost when enabling AOF")
	}
}

func TestSaveRules(t *testing.T) {
	dir := t.TempDir()
	e := engine(t, dir, false, []config.SaveRule{{Seconds: 0, Changes: 2}})
	m, err := persistence.Setup(e)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close(false)
	c := e.InternalClient("t", false)
	do(t, e, c, "ptset", "a", "1")
	do(t, e, c, "ptset", "b", "1")
	waitFor(t, "automatic save", func() bool {
		_, err := os.Stat(filepath.Join(dir, "dump.cbdump"))
		return err == nil && e.DB.Dirty() == 0
	})
}

func TestNotConfigured(t *testing.T) {
	e := engine(t, t.TempDir(), false, nil)
	c := e.InternalClient("t", false)
	for _, cmd := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF", "LASTSAVE"} {
		if v := e.Execute(c, []string{cmd}); v.Str != "ERR persistence is not configured" {
			t.Fatalf("%s: %v", cmd, v)
		}
	}
}

func TestCorruptSnapshotRefusesStart(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "dump.cbdump"), []byte("CACHEBOY\x01\x01garbage"), 0o644)
	if _, err := persistence.Setup(engine(t, dir, false, nil)); err == nil {
		t.Fatal("corrupt snapshot loaded")
	}
}
