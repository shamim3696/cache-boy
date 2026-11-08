package main

import (
	"testing"

	"github.com/shamim3696/cache-boy/internal/api/apitest"
	"github.com/shamim3696/cache-boy/internal/command"
	cbconfig "github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/server"
)

func TestRunTest(t *testing.T) {
	apitest.Register()
	e := command.New(cbconfig.NewStore(cbconfig.Default()))
	srv := server.New(e)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer func() { srv.Close(); e.Stop() }()

	cfg, err := parseFlags([]string{"-c", "4", "-n", "1003", "-P", "8", "-r", "100", "-t", "ping,set,get"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range cfg.tests {
		res, err := runTest(srv.Addr().String(), cfg, test)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.latencies) != 1003 || res.errors != 0 {
			t.Fatalf("%s: %d requests, %d errors (%s)", test, len(res.latencies), res.errors, res.firstErr)
		}
		// (Latencies can be 0 with a coarse clock, e.g. on Windows.)
		if res.percentile(50) < 0 || res.percentile(100) < res.percentile(99) || res.percentile(99) < res.percentile(50) {
			t.Fatalf("%s: bad percentiles", test)
		}
	}
	if _, err := parseFlags([]string{"-t", "nope"}); err == nil {
		t.Fatal("unknown test accepted")
	}
}
