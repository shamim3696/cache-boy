package resp

import (
	"bufio"
	"io"
	"strconv"
)

// Writer encodes values for one connection. Proto is 2 or 3.
type Writer struct {
	w     *bufio.Writer
	buf   []byte
	Proto int
}

func NewWriter(w io.Writer, proto int) *Writer {
	return &Writer{w: bufio.NewWriterSize(w, 32*1024), Proto: proto}
}

func (w *Writer) Write(v Value) error {
	w.buf = Append(w.buf[:0], v, w.Proto)
	_, err := w.w.Write(w.buf)
	return err
}

// WriteCommand encodes a command as an array of bulk strings (client side).
func (w *Writer) WriteCommand(args ...string) error {
	w.buf = AppendCommand(w.buf[:0], args)
	_, err := w.w.Write(w.buf)
	return err
}

func (w *Writer) Flush() error { return w.w.Flush() }

// AppendCommand appends args encoded as a RESP array of bulk strings.
func AppendCommand(b []byte, args []string) []byte {
	b = appendHeader(b, '*', int64(len(args)))
	for _, a := range args {
		b = appendHeader(b, '$', int64(len(a)))
		b = append(b, a...)
		b = append(b, '\r', '\n')
	}
	return b
}

// Append appends the encoding of v to b using the given protocol version.
func Append(b []byte, v Value, proto int) []byte {
	switch v.Type {
	case TypeSimple:
		b = append(b, '+')
		b = appendLine(b, v.Str)
	case TypeError:
		b = append(b, '-')
		b = appendLine(b, v.Str)
	case TypeInteger:
		b = appendHeader(b, ':', v.Int)
	case TypeBulk:
		b = appendHeader(b, '$', int64(len(v.Str)))
		b = append(b, v.Str...)
		b = append(b, '\r', '\n')
	case TypeArray:
		b = appendAggregate(b, '*', v.Elems, proto)
	case TypeNull:
		switch {
		case proto >= 3:
			b = append(b, '_', '\r', '\n')
		case v.nullArray:
			b = append(b, "*-1\r\n"...)
		default:
			b = append(b, "$-1\r\n"...)
		}
	case TypeBool:
		if proto >= 3 {
			if v.Bool {
				b = append(b, "#t\r\n"...)
			} else {
				b = append(b, "#f\r\n"...)
			}
		} else if v.Bool {
			b = append(b, ":1\r\n"...)
		} else {
			b = append(b, ":0\r\n"...)
		}
	case TypeDouble:
		s := FormatFloat(v.Float)
		if proto >= 3 {
			b = append(b, ',')
			b = appendLine(b, s)
		} else {
			b = appendHeader(b, '$', int64(len(s)))
			b = append(b, s...)
			b = append(b, '\r', '\n')
		}
	case TypeMap:
		if proto >= 3 {
			b = appendHeader(b, '%', int64(len(v.Elems)/2))
			for _, e := range v.Elems {
				b = Append(b, e, proto)
			}
		} else {
			b = appendAggregate(b, '*', v.Elems, proto)
		}
	case TypeSet:
		marker := byte('*')
		if proto >= 3 {
			marker = '~'
		}
		b = appendAggregate(b, marker, v.Elems, proto)
	case TypePush:
		marker := byte('*')
		if proto >= 3 {
			marker = '>'
		}
		b = appendAggregate(b, marker, v.Elems, proto)
	case typeMulti:
		for _, e := range v.Elems {
			b = Append(b, e, proto)
		}
	case typeNone:
	}
	return b
}

func appendAggregate(b []byte, marker byte, elems []Value, proto int) []byte {
	b = appendHeader(b, marker, int64(len(elems)))
	for _, e := range elems {
		b = Append(b, e, proto)
	}
	return b
}

func appendHeader(b []byte, marker byte, n int64) []byte {
	b = append(b, marker)
	b = strconv.AppendInt(b, n, 10)
	return append(b, '\r', '\n')
}

// appendLine writes a simple string/error, replacing CR/LF which would break framing.
func appendLine(b []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '\r' || c == '\n' {
			c = ' '
		}
		b = append(b, c)
	}
	return append(b, '\r', '\n')
}
