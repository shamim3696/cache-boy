package apiutil

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

func TestJSON(t *testing.T) {
	cases := []struct {
		v    resp.Value
		want string
	}{
		{resp.Simple("OK"), `"OK"`},
		{resp.Bulk("a\"b\n\x01\xff"), `"a\"b\n\u0001�"`},
		{resp.Int(-7), `-7`},
		{resp.Double(1.5), `1.5`},
		{resp.Double(math.Inf(1)), `"inf"`},
		{resp.Double(math.Inf(-1)), `"-inf"`},
		{resp.Double(math.NaN()), `"nan"`},
		{resp.Bool(true), `true`},
		{resp.Null(), `null`},
		{resp.NullArray(), `null`},
		{resp.None(), `null`},
		{resp.Array(), `[]`},
		{resp.Array(resp.Int(1), resp.Array(resp.Bulk("x")), resp.Err("ERR boom")), `[1,["x"],{"error":"ERR boom"}]`},
		{resp.StringSet([]string{"a"}), `["a"]`},
		{resp.Push(resp.Bulk("message"), resp.Bulk("ch"), resp.Bulk("hi")), `["message","ch","hi"]`},
		{resp.Map(resp.Bulk("z"), resp.Int(1), resp.Int(2), resp.Null(), resp.Bulk("a"), resp.Map()), `{"z":1,"2":null,"a":{}}`},
		{resp.Multi(resp.Push(resp.Bulk("subscribe"), resp.Bulk("a"), resp.Int(1))), `[["subscribe","a",1]]`},
	}
	for _, c := range cases {
		got := string(JSON(c.v))
		if got != c.want {
			t.Errorf("%v: got %s, want %s", c.v, got, c.want)
		}
		if !json.Valid([]byte(got)) {
			t.Errorf("invalid JSON: %s", got)
		}
	}
}

func TestParseCommand(t *testing.T) {
	args, err := ParseCommand([]byte(` ["SET", "k", 12, -1.5, "é"] `))
	if err != nil || len(args) != 5 || args[2] != "12" || args[3] != "-1.5" || args[4] != "é" {
		t.Fatalf("%q %v", args, err)
	}
	for _, bad := range []string{`[]`, `{}`, `[true]`, `[null]`, `[[1]]`, `["a"`} {
		if _, err := ParseCommand([]byte(bad)); err == nil {
			t.Errorf("%s: expected error", bad)
		}
	}
	cmds, err := ParseCommands([]byte(`[["PING"],["ECHO",1]]`))
	if err != nil || len(cmds) != 2 || cmds[1][1] != "1" {
		t.Fatalf("%q %v", cmds, err)
	}
}
