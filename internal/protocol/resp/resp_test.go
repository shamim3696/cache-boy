package resp

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

// oneByte delivers input one byte per Read, like a slow TCP stream.
type oneByte struct{ r io.Reader }

func (o oneByte) Read(p []byte) (int, error) { return o.r.Read(p[:1]) }

func TestReadCommandChunked(t *testing.T) {
	in := "*3\r\n$3\r\nSET\r\n$1\r\na\r\n$5\r\nhe\r\nl\r\n" + "PING\r\n" + "set 'x y' \"a\\nb\"\r\n"
	r := NewReader(oneByte{strings.NewReader(in)})
	want := [][]string{{"SET", "a", "he\r\nl"}, {"PING"}, {"set", "x y", "a\nb"}}
	for _, w := range want {
		got, err := r.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(got, "|") != strings.Join(w, "|") {
			t.Fatalf("got %q want %q", got, w)
		}
	}
	if _, err := r.ReadCommand(); err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
}

func TestProtocolErrors(t *testing.T) {
	for _, in := range []string{"*1\r\n:5\r\n", "*1\r\n$abc\r\n", "*1\r\n$3\r\nabcd\r\n", "\"unclosed\r\n"} {
		_, err := NewReader(strings.NewReader(in)).ReadCommand()
		if _, ok := err.(*ProtocolError); !ok {
			t.Errorf("%q: want ProtocolError, got %v", in, err)
		}
	}
}

func TestEncodeDowngrade(t *testing.T) {
	v := Map(Bulk("k"), Double(1.5), Bulk("b"), Bool(true), Bulk("n"), Null())
	if got := string(Append(nil, v, 3)); got != "%3\r\n$1\r\nk\r\n,1.5\r\n$1\r\nb\r\n#t\r\n$1\r\nn\r\n_\r\n" {
		t.Fatalf("resp3: %q", got)
	}
	if got := string(Append(nil, v, 2)); got != "*6\r\n$1\r\nk\r\n$3\r\n1.5\r\n$1\r\nb\r\n:1\r\n$1\r\nn\r\n$-1\r\n" {
		t.Fatalf("resp2: %q", got)
	}
	if got := string(Append(nil, NullArray(), 2)); got != "*-1\r\n" {
		t.Fatalf("null array: %q", got)
	}
	if got := string(Append(nil, Simple("a\r\nb"), 2)); got != "+a  b\r\n" {
		t.Fatalf("simple string framing: %q", got)
	}
}

func TestRoundTrip(t *testing.T) {
	vals := []Value{
		Simple("OK"), Err("ERR bad"), Int(-42), Bulk(""), Bulk("bin\x00\r\n"),
		Array(Int(1), Array(Bulk("x"))), SetOf(Bulk("a")), Double(-2.25), Bool(false), Null(),
		Map(Bulk("a"), Int(1)), Push(Bulk("message"), Bulk("ch"), Bulk("hi")),
	}
	var buf bytes.Buffer
	for _, v := range vals {
		buf.Write(Append(nil, v, 3))
	}
	r := NewReader(oneByte{&buf})
	for _, want := range vals {
		got, err := r.ReadValue()
		if err != nil {
			t.Fatal(err)
		}
		if string(Append(nil, got, 3)) != string(Append(nil, want, 3)) {
			t.Fatalf("round trip: got %+v want %+v", got, want)
		}
	}
}

func TestErrCode(t *testing.T) {
	if Err("WRONGTYPE x").Str != "WRONGTYPE x" || Err("bad thing").Str != "ERR bad thing" {
		t.Fatal("error code prefixing")
	}
}
