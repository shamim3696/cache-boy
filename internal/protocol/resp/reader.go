package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

const (
	MaxBulkLen  = 512 << 20 // 512 MB, same as Redis
	MaxArrayLen = 1 << 20
)

// ProtocolError means the peer sent bytes that are not valid RESP. The
// connection should be closed after replying.
type ProtocolError struct{ Msg string }

func (e *ProtocolError) Error() string { return "Protocol error: " + e.Msg }

func protoErr(format string, args ...any) error {
	return &ProtocolError{Msg: fmt.Sprintf(format, args...)}
}

type Reader struct {
	r *bufio.Reader
}

func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 32*1024)}
}

// Buffered reports bytes already received but not parsed. The server uses it
// to batch replies of pipelined commands into one flush.
func (r *Reader) Buffered() int { return r.r.Buffered() }

// ReadCommand reads one client command: either a RESP array of bulk strings
// or an inline command ("SET a 1\r\n", as typed in telnet).
func (r *Reader) ReadCommand() ([]string, error) {
	for {
		b, err := r.r.Peek(1)
		if err != nil {
			return nil, err
		}
		if b[0] != '*' {
			line, err := r.readLine()
			if err != nil {
				return nil, err
			}
			args, err := SplitInline(line)
			if err != nil {
				return nil, err
			}
			if len(args) == 0 {
				continue // empty line
			}
			return args, nil
		}
		r.r.Discard(1)
		n, err := r.readInt()
		if err != nil {
			return nil, err
		}
		if n <= 0 {
			continue
		}
		if n > MaxArrayLen {
			return nil, protoErr("invalid multibulk length")
		}
		args := make([]string, n)
		for i := range args {
			c, err := r.r.ReadByte()
			if err != nil {
				return nil, err
			}
			if c != '$' {
				return nil, protoErr("expected '$', got '%c'", c)
			}
			s, null, err := r.readBulkBody()
			if err != nil {
				return nil, err
			}
			if null {
				return nil, protoErr("null bulk string in command")
			}
			args[i] = s
		}
		return args, nil
	}
}

// ReadValue reads any RESP2/RESP3 value (used by clients and replication).
func (r *Reader) ReadValue() (Value, error) {
	c, err := r.r.ReadByte()
	if err != nil {
		return Value{}, err
	}
	switch Type(c) {
	case TypeSimple, TypeError:
		line, err := r.readLine()
		return Value{Type: Type(c), Str: line}, err
	case TypeInteger:
		n, err := r.readInt()
		return Int(n), err
	case TypeBulk, '=', '!': // bulk, verbatim string, bulk error
		s, null, err := r.readBulkBody()
		if err != nil {
			return Value{}, err
		}
		if null {
			return Null(), nil
		}
		if c == '!' {
			return Value{Type: TypeError, Str: s}, nil
		}
		if c == '=' && len(s) >= 4 {
			s = s[4:] // drop "txt:" format prefix
		}
		return Bulk(s), nil
	case TypeArray, TypeSet, TypePush, TypeMap:
		n, err := r.readInt()
		if err != nil {
			return Value{}, err
		}
		if n < 0 {
			return NullArray(), nil
		}
		if Type(c) == TypeMap {
			n *= 2
		}
		if n > MaxArrayLen {
			return Value{}, protoErr("aggregate too large")
		}
		elems := make([]Value, n)
		for i := range elems {
			if elems[i], err = r.ReadValue(); err != nil {
				return Value{}, err
			}
		}
		return Value{Type: Type(c), Elems: elems}, nil
	case TypeNull:
		_, err := r.readLine()
		return Null(), err
	case TypeBool:
		line, err := r.readLine()
		return Bool(line == "t"), err
	case TypeDouble:
		line, err := r.readLine()
		if err != nil {
			return Value{}, err
		}
		f, err := strconv.ParseFloat(line, 64)
		return Double(f), err
	case '(': // big number: keep as string
		line, err := r.readLine()
		return Bulk(line), err
	}
	return Value{}, protoErr("unknown type byte '%c'", c)
}

func (r *Reader) readLine() (string, error) {
	line, err := r.r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		return "", protoErr("line too long")
	}
	if err != nil {
		return "", err
	}
	n := len(line) - 1
	if n > 0 && line[n-1] == '\r' {
		n--
	}
	return string(line[:n]), nil
}

func (r *Reader) readInt() (int64, error) {
	line, err := r.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.ParseInt(line, 10, 64)
	if err != nil {
		return 0, protoErr("invalid integer %q", line)
	}
	return n, nil
}

func (r *Reader) readBulkBody() (s string, null bool, err error) {
	n, err := r.readInt()
	if err != nil {
		return "", false, err
	}
	if n < 0 {
		return "", true, nil
	}
	if n > MaxBulkLen {
		return "", false, protoErr("invalid bulk length")
	}
	buf := make([]byte, n+2)
	if _, err := io.ReadFull(r.r, buf); err != nil {
		return "", false, err
	}
	if buf[n] != '\r' || buf[n+1] != '\n' {
		return "", false, protoErr("bulk string not terminated by CRLF")
	}
	return string(buf[:n]), false, nil
}

// SplitInline splits an inline command line, honouring "double" and 'single'
// quotes and \n \t \xHH escapes inside double quotes, like redis-cli.
func SplitInline(line string) ([]string, error) {
	var args []string
	i := 0
	for {
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		if i >= len(line) {
			return args, nil
		}
		var cur []byte
		switch line[i] {
		case '"':
			i++
			for {
				if i >= len(line) {
					return nil, protoErr("unbalanced quotes in request")
				}
				c := line[i]
				if c == '"' {
					i++
					break
				}
				if c == '\\' && i+1 < len(line) {
					i++
					switch line[i] {
					case 'n':
						c = '\n'
					case 'r':
						c = '\r'
					case 't':
						c = '\t'
					case 'x':
						if i+2 < len(line) {
							if v, err := strconv.ParseUint(line[i+1:i+3], 16, 8); err == nil {
								c = byte(v)
								i += 2
								break
							}
						}
						c = 'x'
					default:
						c = line[i]
					}
				}
				cur = append(cur, c)
				i++
			}
		case '\'':
			i++
			for {
				if i >= len(line) {
					return nil, protoErr("unbalanced quotes in request")
				}
				if line[i] == '\'' {
					i++
					break
				}
				if line[i] == '\\' && i+1 < len(line) && line[i+1] == '\'' {
					i++
				}
				cur = append(cur, line[i])
				i++
			}
		default:
			for i < len(line) && line[i] != ' ' && line[i] != '\t' {
				cur = append(cur, line[i])
				i++
			}
		}
		args = append(args, string(cur))
	}
}
