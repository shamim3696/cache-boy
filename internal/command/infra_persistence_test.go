package command_test

import (
	"testing"
	"time"

	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/persistence"
)

func TestPersistenceCommands(t *testing.T) {
	dir := t.TempDir()
	setDir := func(c *config.Config) { c.Dir, c.AppendOnly, c.Save = dir, true, nil }
	e := infraEngine(t, setDir)
	m, err := persistence.Setup(e)
	if err != nil {
		t.Fatal(err)
	}
	c := e.NewClient("tcp", "test")
	mustOK(t, e, c, "infratset", "a", "1")
	mustOK(t, e, c, "SAVE")
	if v := mustOK(t, e, c, "LASTSAVE"); v.Int < time.Now().Unix()-5 {
		t.Fatalf("LASTSAVE: %v", v)
	}
	mustOK(t, e, c, "BGREWRITEAOF")
	eventually(t, "rewrite", func() bool { return infoField(e, "persistence", "aof_rewrite_in_progress") == "0" })
	mustOK(t, e, c, "infratset", "b", "2")
	mustOK(t, e, c, "MULTI")
	mustOK(t, e, c, "SAVE")
	if v := mustOK(t, e, c, "EXEC"); len(v.Elems) != 1 || !v.Elems[0].IsError() {
		t.Fatalf("SAVE inside MULTI must fail instead of deadlocking: %v", v)
	}
	if infoField(e, "persistence", "aof_enabled") != "1" {
		t.Fatal("INFO persistence")
	}
	if err := m.Close(false); err != nil {
		t.Fatal(err)
	}

	e2 := infraEngine(t, setDir)
	m2, err := persistence.Setup(e2)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close(false)
	c2 := e2.NewClient("tcp", "test")
	if getVal(e2, c2, "a") != "1" || getVal(e2, c2, "b") != "2" {
		t.Fatal("data not restored from AOF")
	}
}
