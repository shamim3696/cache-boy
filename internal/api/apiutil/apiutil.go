// Package apiutil holds what the HTTP API and the WebSocket endpoint share:
// RESP → JSON conversion, JSON → command parsing, and credential checks.
package apiutil

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"cache-boy/internal/auth"
	"cache-boy/internal/command"
	"cache-boy/internal/feature/namespace"
	"cache-boy/internal/protocol/resp"
)

// MaxBody is the largest request body / WebSocket message accepted (16 MB).
const MaxBody = 16 << 20

// typeMulti is resp's internal "several replies" type (SUBSCRIBE a b). It is
// unexported in package resp; it is rendered as an array.
const typeMulti resp.Type = 1

// AppendJSON appends v encoded as JSON:
//
//	simple/bulk → string, integer → number, double → number ("inf", "-inf",
//	"nan" as strings), bool → bool, null → null, array/set/push → array,
//	map → object (keys stringified, order kept), error → {"error": "..."}.
func AppendJSON(b []byte, v resp.Value) []byte {
	switch v.Type {
	case resp.TypeSimple, resp.TypeBulk:
		return AppendString(b, v.Str)
	case resp.TypeError:
		b = append(b, `{"error":`...)
		b = AppendString(b, v.Str)
		return append(b, '}')
	case resp.TypeInteger:
		return strconv.AppendInt(b, v.Int, 10)
	case resp.TypeDouble:
		return AppendFloat(b, v.Float)
	case resp.TypeBool:
		return strconv.AppendBool(b, v.Bool)
	case resp.TypeNull:
		return append(b, "null"...)
	case resp.TypeMap:
		b = append(b, '{')
		for i := 0; i+1 < len(v.Elems); i += 2 {
			if i > 0 {
				b = append(b, ',')
			}
			b = AppendString(b, keyString(v.Elems[i]))
			b = append(b, ':')
			b = AppendJSON(b, v.Elems[i+1])
		}
		return append(b, '}')
	}
	if v.Elems != nil || v.Type == resp.TypeArray || v.Type == resp.TypeSet || v.Type == resp.TypePush || v.Type == typeMulti {
		b = append(b, '[')
		for i, e := range v.Elems {
			if i > 0 {
				b = append(b, ',')
			}
			b = AppendJSON(b, e)
		}
		return append(b, ']')
	}
	return append(b, "null"...) // "no reply" and anything unknown
}

// JSON returns v as JSON text.
func JSON(v resp.Value) json.RawMessage { return AppendJSON(nil, v) }

// AppendFloat writes a JSON number; non-finite values become strings.
func AppendFloat(b []byte, f float64) []byte {
	switch {
	case math.IsNaN(f):
		return append(b, `"nan"`...)
	case math.IsInf(f, 1):
		return append(b, `"inf"`...)
	case math.IsInf(f, -1):
		return append(b, `"-inf"`...)
	}
	return strconv.AppendFloat(b, f, 'g', -1, 64)
}

func keyString(k resp.Value) string {
	switch k.Type {
	case resp.TypeSimple, resp.TypeBulk, resp.TypeError, resp.TypeInteger, resp.TypeDouble, resp.TypeBool, resp.TypeNull:
		return k.String()
	}
	return string(JSON(k))
}

const hexDigits = "0123456789abcdef"

// AppendString appends s as a JSON string. Invalid UTF-8 becomes U+FFFD.
func AppendString(b []byte, s string) []byte {
	b = append(b, '"')
	start := 0
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			if c >= 0x20 && c != '"' && c != '\\' {
				i++
				continue
			}
			b = append(b, s[start:i]...)
			switch c {
			case '"', '\\':
				b = append(b, '\\', c)
			case '\n':
				b = append(b, '\\', 'n')
			case '\r':
				b = append(b, '\\', 'r')
			case '\t':
				b = append(b, '\\', 't')
			default:
				b = append(b, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			}
			i++
			start = i
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b = append(b, s[start:i]...)
			b = append(b, `�`...)
			i += size
			start = i
			continue
		}
		if r == ' ' || r == ' ' {
			b = append(b, s[start:i]...)
			b = append(b, '\\', 'u', '2', '0', '2', hexDigits[r&0xf])
			i += size
			start = i
			continue
		}
		i += size
	}
	b = append(b, s[start:]...)
	return append(b, '"')
}

// ParseCommand decodes a JSON array of strings/numbers into command args.
func ParseCommand(raw []byte) ([]string, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, errors.New("command must be a JSON array of strings or numbers")
	}
	if len(items) == 0 {
		return nil, errors.New("empty command")
	}
	args := make([]string, len(items))
	for i, it := range items {
		it = bytes.TrimSpace(it)
		if len(it) == 0 {
			return nil, errors.New("empty argument")
		}
		switch it[0] {
		case '"':
			if err := json.Unmarshal(it, &args[i]); err != nil {
				return nil, fmt.Errorf("argument %d: %v", i, err)
			}
		case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			var n json.Number
			if err := json.Unmarshal(it, &n); err != nil {
				return nil, fmt.Errorf("argument %d: %v", i, err)
			}
			args[i] = n.String()
		default:
			return nil, fmt.Errorf("argument %d must be a string or a number", i)
		}
	}
	return args, nil
}

// ParseCommands decodes a JSON array of commands (pipeline / multi bodies).
func ParseCommands(raw []byte) ([][]string, error) {
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, errors.New("body must be a JSON array of commands, e.g. [[\"SET\",\"a\",\"1\"],[\"GET\",\"a\"]]")
	}
	if len(items) == 0 {
		return nil, errors.New("empty command list")
	}
	out := make([][]string, len(items))
	for i, it := range items {
		args, err := ParseCommand(it)
		if err != nil {
			return nil, fmt.Errorf("command %d: %v", i, err)
		}
		out[i] = args
	}
	return out, nil
}

// ErrUnauthorized means credentials were missing or wrong.
var ErrUnauthorized = errors.New("NOAUTH Authentication required.")

// Login checks credentials. token is "password" (default user) or
// "user:password". An empty token is accepted only when the server has no
// password. Returns the user to run commands as.
func Login(e *command.Engine, token string) (string, error) {
	if token == "" {
		if e.Auth.AutoLogin() {
			return auth.DefaultUser, nil
		}
		return "", ErrUnauthorized
	}
	if e.Auth.Authenticate(auth.DefaultUser, token) == nil {
		return auth.DefaultUser, nil
	}
	if u, p, ok := strings.Cut(token, ":"); ok && e.Auth.Authenticate(u, p) == nil {
		return u, nil
	}
	return "", errors.New("WRONGPASS invalid username-password pair or user is disabled.")
}

// LoginUser checks an explicit user/password pair.
func LoginUser(e *command.Engine, user, pass string) (string, error) {
	if user == "" {
		user = auth.DefaultUser
	}
	if err := e.Auth.Authenticate(user, pass); err != nil {
		return "", err
	}
	return user, nil
}

// RequestUser authenticates an HTTP request from its Authorization header
// ("Bearer <password>", "Bearer <user:password>" or Basic user:pass) and,
// when allowQuery is set, a ?token= parameter.
func RequestUser(e *command.Engine, r *http.Request, allowQuery bool) (string, error) {
	h := strings.TrimSpace(r.Header.Get("Authorization"))
	switch {
	case h == "":
		if allowQuery {
			if t := r.URL.Query().Get("token"); t != "" {
				return Login(e, t)
			}
		}
		return Login(e, "")
	case len(h) > 7 && strings.EqualFold(h[:7], "bearer "):
		return Login(e, strings.TrimSpace(h[7:]))
	case len(h) > 6 && strings.EqualFold(h[:6], "basic "):
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(h[6:]))
		if err != nil {
			return "", ErrUnauthorized
		}
		u, p, _ := strings.Cut(string(raw), ":")
		return LoginUser(e, u, p)
	}
	return "", ErrUnauthorized
}

// Namespace validates the X-CacheBoy-Namespace header value. "" and
// "default" select the default namespace.
func Namespace(ns string) (string, error) {
	if ns == "" || ns == "default" {
		return "", nil
	}
	if !namespace.Valid(ns) {
		return "", errors.New("ERR invalid namespace name")
	}
	return ns, nil
}

// ErrorStatus maps a command error to an HTTP status.
func ErrorStatus(msg string) int {
	switch {
	case strings.HasPrefix(msg, "NOPERM"):
		return http.StatusForbidden
	case strings.HasPrefix(msg, "NOAUTH"), strings.HasPrefix(msg, "WRONGPASS"):
		return http.StatusUnauthorized
	}
	return http.StatusOK
}
