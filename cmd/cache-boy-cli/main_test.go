package main

import (
	"bytes"
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
	"github.com/shamim3696/cache-boy/internal/server"
)

func TestFormatReply(t *testing.T) {
	cases := []struct {
		v    resp.Value
		want string
	}{
		{resp.OK, "OK"},
		{resp.Bulk("a\"b\n\x01é"), `"a\"b\n\x01\xc3\xa9"`},
		{resp.Int(3), "(integer) 3"},
		{resp.Double(1.5), "(double) 1.5"},
		{resp.Null(), "(nil)"},
		{resp.Bool(true), "(true)"},
		{resp.Err("ERR boom"), "(error) ERR boom"},
		{resp.Array(), "(empty array)"},
		{resp.Array(resp.Bulk("a"), resp.Bulk("b")), "1) \"a\"\n2) \"b\""},
		{resp.Array(resp.Array(resp.Bulk("a"), resp.Int(1)), resp.Bulk("c")), "1) 1) \"a\"\n   2) (integer) 1\n2) \"c\""},
		{resp.Map(resp.Bulk("k"), resp.Bulk("v"), resp.Bulk("n"), resp.Array(resp.Int(1), resp.Int(2))),
			"1# \"k\" => \"v\"\n2# \"n\" => 1) (integer) 1\n   2) (integer) 2"},
		{resp.StringSet([]string{"x"}), "1~ \"x\""},
	}
	for _, c := range cases {
		if got := formatReply(c.v); got != c.want {
			t.Errorf("got:\n%s\nwant:\n%s", got, c.want)
		}
	}
	elems := make([]resp.Value, 10)
	for i := range elems {
		elems[i] = resp.Int(int64(i))
	}
	if got := strings.Split(formatReply(resp.Array(elems...)), "\n"); got[0] != " 1) (integer) 0" || got[9] != "10) (integer) 9" {
		t.Errorf("padding: %q", got)
	}
	if got := formatRaw(resp.Array(resp.Bulk("a"), resp.Int(2), resp.Null())); got != "a\n2\n" {
		t.Errorf("raw: %q", got)
	}
}

func TestREPL(t *testing.T) {
	e := command.New(config.NewStore(config.Default()))
	srv := server.New(e)
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	go srv.Serve()
	defer func() { srv.Close(); e.Stop() }()
	host, port, _ := net.SplitHostPort(srv.Addr().String())
	p, _ := strconv.Atoi(port)

	var out bytes.Buffer
	c := &cli{host: host, port: p, proto: 2, out: &out}
	if err := c.connect(); err != nil {
		t.Fatal(err)
	}
	in := strings.NewReader("PING\nECHO \"hello world\"\nNAMESPACE USE app\nNAMESPACE CURRENT\nNOSUCH\nhelp ping\nquit\nPING\n")
	c.runLines(in, true)
	got := out.String()
	addr := srv.Addr().String()
	for _, want := range []string{
		addr + "> PONG\n",
		"\"hello world\"\n",
		addr + "> OK\n" + addr + "[app]> \"app\"\n",
		"(error) ERR unknown command 'NOSUCH'",
		"  PING\n  summary: Returns PONG",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	if strings.Count(got, "PONG\n") != 1 {
		t.Errorf("commands after quit ran:\n%s", got)
	}
}
