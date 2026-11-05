package resp

import (
	"bytes"
	"testing"
)

// FuzzReadCommand feeds arbitrary bytes to the command parser: it must
// return commands or an error, never panic or allocate unbounded memory.
func FuzzReadCommand(f *testing.F) {
	for _, s := range []string{
		"*2\r\n$3\r\nGET\r\n$1\r\na\r\n", "PING\r\n", "set 'a b' \"c\x41\"\r\n",
		"*-1\r\n", "*1\r\n$-1\r\n", "*99999999999\r\n", "$5\r\nab", "\"unclosed\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 100; i++ {
			if _, err := r.ReadCommand(); err != nil {
				return
			}
		}
	})
}

// FuzzReadValue does the same for the full RESP3 value parser (clients, replication).
func FuzzReadValue(f *testing.F) {
	for _, s := range []string{
		"+OK\r\n", "%1\r\n+a\r\n:1\r\n", ">2\r\n+a\r\n_\r\n", ",1.5\r\n", "#t\r\n",
		"*2\r\n*1\r\n:1\r\n$-1\r\n", "=7\r\ntxt:abc\r\n", "!3\r\nerr\r\n", "(123\r\n",
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data))
		for i := 0; i < 100; i++ {
			v, err := r.ReadValue()
			if err != nil {
				return
			}
			Append(nil, v, 2) // encoding a parsed value must not panic either
			Append(nil, v, 3)
		}
	})
}
