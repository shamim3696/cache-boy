package command_test

import (
	"strings"
	"testing"

	"cache-boy/internal/cluster"
	"cache-boy/internal/config"
)

func TestClusterCommands(t *testing.T) {
	e := infraEngine(t, func(c *config.Config) {
		c.ClusterEnabled = true
		c.ClusterNodeID = "n1"
		c.ClusterNodes = []string{"n1 127.0.0.1:7001 0-8191", "n2 127.0.0.1:7002 8192-16383"}
	})
	if _, err := cluster.Setup(e); err != nil {
		t.Fatal(err)
	}
	c := e.NewClient("tcp", "test")

	if v := mustOK(t, e, c, "CLUSTER", "KEYSLOT", "foo"); v.Int != 12182 {
		t.Fatalf("KEYSLOT: %v", v)
	}
	if v := exec(e, c, "infratset", "foo", "1"); v.Str != "MOVED 12182 127.0.0.1:7002" {
		t.Fatalf("MOVED: %v", v)
	}
	if v := exec(e, c, "infratset", "bar", "1"); v.IsError() { // slot 5061 is ours
		t.Fatalf("local key: %v", v)
	}
	mustOK(t, e, c, "infratset", "{bar}2", "1")
	if v := mustOK(t, e, c, "CLUSTER", "COUNTKEYSINSLOT", "5061"); v.Int != 2 {
		t.Fatalf("COUNTKEYSINSLOT: %v", v)
	}
	if v := mustOK(t, e, c, "CLUSTER", "INFO"); !strings.Contains(v.Str, "cluster_state:ok") {
		t.Fatalf("CLUSTER INFO: %v", v)
	}
	if v := mustOK(t, e, c, "CLUSTER", "MYID"); v.Str != "n1" {
		t.Fatalf("MYID: %v", v)
	}
	mustOK(t, e, c, "READONLY")
	mustOK(t, e, c, "READWRITE")
	if infoField(e, "cluster", "cluster_enabled") != "1" {
		t.Fatal("INFO cluster")
	}

	// Internal master clients (replication, AOF) bypass routing.
	m := e.InternalClient("master", true)
	mustOK(t, e, m, "infratset", "foo", "1")
}

func TestClusterDisabled(t *testing.T) {
	e := infraEngine(t, nil)
	c := e.NewClient("tcp", "test")
	want := "ERR This instance has cluster support disabled"
	if v := exec(e, c, "CLUSTER", "INFO"); v.Str != want {
		t.Fatalf("no router: %v", v)
	}
	if _, err := cluster.Setup(e); err != nil {
		t.Fatal(err)
	}
	if v := exec(e, c, "CLUSTER", "KEYSLOT", "foo"); v.Str != want {
		t.Fatalf("disabled router: %v", v)
	}
	mustOK(t, e, c, "infratset", "foo", "1")
}
