package main

import (
	"strconv"
	"strings"

	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

// formatReply renders a reply like redis-cli does on a terminal.
func formatReply(v resp.Value) string {
	switch v.Type {
	case resp.TypeError:
		return "(error) " + v.Str
	case resp.TypeSimple:
		return v.Str
	case resp.TypeBulk:
		return quote(v.Str)
	case resp.TypeInteger:
		return "(integer) " + strconv.FormatInt(v.Int, 10)
	case resp.TypeDouble:
		return "(double) " + resp.FormatFloat(v.Float)
	case resp.TypeBool:
		if v.Bool {
			return "(true)"
		}
		return "(false)"
	case resp.TypeNull:
		return "(nil)"
	case resp.TypeMap:
		n := len(v.Elems) / 2
		if n == 0 {
			return "(empty hash)"
		}
		w := len(strconv.Itoa(n))
		var lines []string
		for i := 0; i < n; i++ {
			label := pad(i+1, w) + "# "
			item := formatReply(v.Elems[2*i]) + " => " + formatReply(v.Elems[2*i+1])
			lines = appendIndented(lines, label, item)
		}
		return strings.Join(lines, "\n")
	}
	// array, set, push
	if len(v.Elems) == 0 {
		if v.Type == resp.TypeSet {
			return "(empty set)"
		}
		return "(empty array)"
	}
	marker := ") "
	if v.Type == resp.TypeSet {
		marker = "~ "
	}
	w := len(strconv.Itoa(len(v.Elems)))
	var lines []string
	for i, e := range v.Elems {
		lines = appendIndented(lines, pad(i+1, w)+marker, formatReply(e))
	}
	return strings.Join(lines, "\n")
}

// appendIndented adds label+item, indenting item's continuation lines under it.
func appendIndented(lines []string, label, item string) []string {
	sub := strings.Split(item, "\n")
	lines = append(lines, label+sub[0])
	ind := strings.Repeat(" ", len(label))
	for _, s := range sub[1:] {
		lines = append(lines, ind+s)
	}
	return lines
}

func pad(n, w int) string {
	s := strconv.Itoa(n)
	return strings.Repeat(" ", w-len(s)) + s
}

// formatRaw renders a reply like redis-cli --raw: no quotes or type hints,
// one element per line.
func formatRaw(v resp.Value) string {
	switch v.Type {
	case resp.TypeError, resp.TypeSimple, resp.TypeBulk:
		return v.Str
	case resp.TypeInteger:
		return strconv.FormatInt(v.Int, 10)
	case resp.TypeDouble:
		return resp.FormatFloat(v.Float)
	case resp.TypeBool:
		if v.Bool {
			return "1"
		}
		return "0"
	case resp.TypeNull:
		return ""
	}
	parts := make([]string, len(v.Elems))
	for i, e := range v.Elems {
		parts[i] = formatRaw(e)
	}
	return strings.Join(parts, "\n")
}

// quote escapes a string like redis-cli (sdscatrepr): printable ASCII as is,
// \n \r \t \a \b \" \\ escaped, other bytes as \xHH.
func quote(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	const hex = "0123456789abcdef"
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '\\', '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		case '\a':
			b.WriteString(`\a`)
		case '\b':
			b.WriteString(`\b`)
		default:
			if c >= 0x20 && c < 0x7f {
				b.WriteByte(c)
			} else {
				b.WriteString(`\x`)
				b.WriteByte(hex[c>>4])
				b.WriteByte(hex[c&0xf])
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
