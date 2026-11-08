package command

import (
	"strconv"
	"strings"
	"testing"
)

func TestEncodingTransitions(t *testing.T) {
	e, c := newDataEngine(t)
	long := strings.Repeat("v", 65)
	runCases(t, e, c, []tc{
		{"HSET h f v", "1"},
		{"OBJECT ENCODING h", "listpack"},
		{"HSET h f " + long, "0"},
		{"OBJECT ENCODING h", "hashtable"},
		{"HGET h f", long},
		{"HDEL h f", "1"},
		{"EXISTS h", "0"},

		{"SADD s 1 2 3", "3"},
		{"OBJECT ENCODING s", "intset"},
		{"SADD s 007", "1"},
		{"OBJECT ENCODING s", "listpack"},
		{"SMEMBERS s", "[007 1 2 3]"},
		{"SADD s " + long, "1"},
		{"OBJECT ENCODING s", "hashtable"},
		{"SADD n -0", "1"},
		{"OBJECT ENCODING n", "listpack"},

		{"SET k 10", "OK"},
		{"OBJECT ENCODING k", "int"},
		{"INCRBY k 5", "15"},
		{"OBJECT ENCODING k", "int"},
		{"GET k", "15"},
		{"STRLEN k", "2"},
		{"APPEND k x", "3"},
		{"OBJECT ENCODING k", "embstr"},
		{"INCR k", "!ERR value is not an integer"},
		{"SET k +5", "OK"},
		{"OBJECT ENCODING k", "embstr"},
		{"INCR k", "6"},
		{"OBJECT ENCODING k", "int"},
		{"SET k 007", "OK"},
		{"DECR k", "6"},
		{"INCR fresh", "1"},
		{"OBJECT ENCODING fresh", "int"},
		{"SET k 9223372036854775807", "OK"},
		{"INCR k", "!ERR increment or decrement would overflow"},
		{"GET k", "9223372036854775807"},
		{"SETRANGE k 0 x", "19"},
		{"GET k", "x223372036854775807"},
	})

	for i := range 128 {
		do(e, c, "HSET", "big", "f"+strconv.Itoa(i), "v")
		do(e, c, "SADD", "ints", strconv.Itoa(i))
		do(e, c, "SADD", "strs", "m"+strconv.Itoa(i))
	}
	mustStr(t, e, c, "listpack", "OBJECT", "ENCODING", "big")
	mustStr(t, e, c, "listpack", "OBJECT", "ENCODING", "strs")
	do(e, c, "HSET", "big", "f128", "v")
	do(e, c, "SADD", "strs", "m128")
	mustStr(t, e, c, "hashtable", "OBJECT", "ENCODING", "big")
	mustStr(t, e, c, "hashtable", "OBJECT", "ENCODING", "strs")
	for i := 128; i < 512; i++ {
		do(e, c, "SADD", "ints", strconv.Itoa(i))
	}
	mustStr(t, e, c, "intset", "OBJECT", "ENCODING", "ints")
	mustStr(t, e, c, "512", "SCARD", "ints")
	do(e, c, "SADD", "ints", "512")
	mustStr(t, e, c, "hashtable", "OBJECT", "ENCODING", "ints")
	mustStr(t, e, c, "513", "SCARD", "ints")
}

// TestCompactValuesSurviveCopy checks that commands which rebuild values
// from Map, Members or Bytes keep every encoding's contents.
func TestCompactValuesSurviveCopy(t *testing.T) {
	e, c := newDataEngine(t)
	runCases(t, e, c, []tc{
		{"HSET h a 1 b 2", "2"},
		{"SADD s 3 1 2", "3"},
		{"SADD t x 1", "2"},
		{"SET i 42", "OK"},
		{"COPY h h2", "1"},
		{"COPY s s2", "1"},
		{"COPY t t2", "1"},
		{"COPY i i2", "1"},
		{"HGETALL h2", "{a:1 b:2}"},
		{"SMEMBERS s2", "[1 2 3]"},
		{"SMEMBERS t2", "[1 x]"},
		{"GET i2", "42"},
		{"OBJECT ENCODING s2", "intset"},
		{"SINTER s t", "[1]"},
		{"SUNION s t", "[1 2 3 x]"},
		{"SDIFF s t", "[2 3]"},
	})
}
