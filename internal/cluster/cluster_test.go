package cluster

import (
	"strings"
	"testing"

	"github.com/shamim3696/cache-boy/internal/store"
)

func TestSlots(t *testing.T) {
	cases := map[string]int{
		"foo":  12182,
		"bar":  5061,
		"":     0,
		"{}":   15257, // empty tag: the whole key is hashed
		"{a}b": Slot("a"),
	}
	for k, want := range cases {
		if got := Slot(k); got != want {
			t.Errorf("Slot(%q) = %d, want %d", k, got, want)
		}
	}
	if Slot("{user1}.a") != Slot("{user1}.b") || Slot("{user1}.a") != Slot("user1") {
		t.Fatal("hash tags not honoured")
	}
	if HashTag("a{b}{c}") != "b" || HashTag("a{}b") != "a{}b" || HashTag("a{b") != "a{b" {
		t.Fatal("HashTag edge cases")
	}
	if CRC16("123456789") != 0x31C3 { // XMODEM check value
		t.Fatalf("CRC16 = %x", CRC16("123456789"))
	}
}

func testRouter(t *testing.T) *Router {
	t.Helper()
	r, err := New(true, "n1", []string{
		"n1 127.0.0.1:7001 0-8191",
		"n2 127.0.0.1:7002 8192-12000,12001-16383",
	}, store.New(4))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func TestCheck(t *testing.T) {
	r := testRouter(t)
	if err := r.Check([]string{"bar"}); err != nil { // slot 5061: ours
		t.Fatal(err)
	}
	err := r.Check([]string{"foo"})
	if err == nil || err.Error() != "MOVED 12182 127.0.0.1:7002" {
		t.Fatalf("foo: %v", err)
	}
	err = r.Check([]string{"foo", "bar"})
	if err == nil || err.Error() != "CROSSSLOT Keys in request don't hash to the same slot" {
		t.Fatalf("cross slot: %v", err)
	}
	if err := r.Check([]string{"{bar}1", "{bar}2"}); err != nil {
		t.Fatal(err)
	}
	// Namespaced keys hash like the key the client sent.
	if err := r.Check([]string{"\x00ns\x00foo"}); err == nil || !strings.HasPrefix(err.Error(), "MOVED 12182") {
		t.Fatalf("namespaced key: %v", err)
	}

	off, _ := New(false, "", nil, nil)
	if off.Enabled() || off.Check([]string{"foo", "bar"}) != nil {
		t.Fatal("disabled router must not redirect")
	}
	partial, _ := New(true, "a", []string{"a 127.0.0.1:1 0-100"}, store.New(1))
	if err := partial.Check([]string{"foo"}); err == nil || !strings.HasPrefix(err.Error(), "CLUSTERDOWN") {
		t.Fatalf("unassigned slot: %v", err)
	}
}

func TestConfigErrors(t *testing.T) {
	for _, nodes := range [][]string{
		{"n1 127.0.0.1:1 0-10", "n2 127.0.0.1:2 5-20"}, // overlap
		{"n1 127.0.0.1:1 0-16384"},                     // out of range
		{"n1 nohostport 0-1"},
		{"n2 127.0.0.1:2 0-1"}, // self missing
	} {
		if _, err := New(true, "n1", nodes, nil); err == nil {
			t.Errorf("%v: expected error", nodes)
		}
	}
}

func TestCommand(t *testing.T) {
	r := testRouter(t)
	if v := r.Command([]string{"KEYSLOT", "foo"}); v.Int != 12182 {
		t.Fatalf("KEYSLOT: %v", v)
	}
	if v := r.Command([]string{"MYID"}); v.Str != "n1" {
		t.Fatalf("MYID: %v", v)
	}
	if v := r.Command([]string{"INFO"}); !strings.Contains(v.Str, "cluster_state:ok") || !strings.Contains(v.Str, "cluster_known_nodes:2") {
		t.Fatalf("INFO: %v", v.Str)
	}
	v := r.Command([]string{"SLOTS"})
	if len(v.Elems) != 3 || v.Elems[0].Elems[0].Int != 0 || v.Elems[2].Elems[1].Int != 16383 {
		t.Fatalf("SLOTS: %v", v)
	}
	if v := r.Command([]string{"NODES"}); !strings.Contains(v.Str, "n1 127.0.0.1:7001@17001 myself,master") || !strings.Contains(v.Str, "8192-12000 12001-16383") {
		t.Fatalf("NODES: %q", v.Str)
	}
	if v := r.Command([]string{"SHARDS"}); len(v.Elems) != 2 {
		t.Fatalf("SHARDS: %v", v)
	}
	unlock := r.db.LockAll()
	r.db.Put("{bar}x", r.db.NewStringValue([]byte("1")))
	r.db.Put("{bar}y", r.db.NewStringValue([]byte("1")))
	if v := r.Command([]string{"COUNTKEYSINSLOT", "5061"}); v.Int != 2 {
		t.Fatalf("COUNTKEYSINSLOT: %v", v)
	}
	if v := r.Command([]string{"GETKEYSINSLOT", "5061", "1"}); len(v.Elems) != 1 {
		t.Fatalf("GETKEYSINSLOT: %v", v)
	}
	unlock()
	if v := r.Command([]string{"NOPE"}); !v.IsError() {
		t.Fatal("unknown subcommand")
	}
}
