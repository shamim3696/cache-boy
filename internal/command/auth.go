package command

import (
	"strings"

	"github.com/shamim3696/cache-boy/internal/auth"
	"github.com/shamim3696/cache-boy/internal/protocol/resp"
)

func init() {
	Register(
		&Spec{Name: "auth", Arity: -2, Flags: FlagNoAuth | FlagNoScript, Group: "connection", Summary: "AUTH [username] password", Handler: authCmd},
		&Spec{Name: "acl", Arity: -2, Flags: FlagAdmin | FlagNoScript, Group: "server", Summary: "ACL WHOAMI | USERS | LIST | GETUSER | SETUSER | DELUSER | CAT", Handler: aclCmd},
	)
}

func authCmd(x *Ctx) resp.Value {
	user, pass := auth.DefaultUser, x.Arg(1)
	switch x.NArgs() {
	case 2:
	case 3:
		user, pass = x.Arg(1), x.Arg(2)
	default:
		return ErrSyntax
	}
	if x.NArgs() == 2 && x.E.Auth.AutoLogin() {
		return resp.Err("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	if err := x.E.Auth.Authenticate(user, pass); err != nil {
		return resp.Err(err.Error())
	}
	x.C.User = user
	return resp.OK
}

func aclCmd(x *Ctx) resp.Value {
	a := x.E.Auth
	switch x.ArgLower(1) {
	case "whoami":
		return resp.Bulk(x.C.User)
	case "users":
		return resp.Strings(a.Users())
	case "list":
		var lines []string
		for _, u := range a.Users() {
			if d, ok := a.Describe(u); ok {
				lines = append(lines, d)
			}
		}
		return resp.Strings(lines)
	case "getuser":
		d, ok := a.Describe(x.Arg(2))
		if !ok {
			return resp.Null()
		}
		return resp.Bulk(d)
	case "setuser":
		if x.NArgs() < 3 {
			return wrongArgs("acl|setuser")
		}
		if err := a.SetUser(x.Arg(2), x.Args[3:]); err != nil {
			return resp.Err(err.Error())
		}
		return resp.OK
	case "deluser":
		n := 0
		for _, u := range x.Args[2:] {
			if a.DelUser(u) {
				n++
				for _, c := range x.E.Clients() {
					if c.User == u {
						c.Close()
					}
				}
			}
		}
		return resp.Int(int64(n))
	case "cat":
		seen := map[string]bool{}
		for _, s := range All() {
			for _, c := range s.Categories() {
				seen[c] = true
			}
		}
		var cats []string
		for c := range seen {
			cats = append(cats, c)
		}
		if x.NArgs() > 2 { // commands in a category
			want := strings.ToLower(strings.TrimPrefix(x.Arg(2), "@"))
			cats = nil
			for _, s := range All() {
				for _, c := range s.Categories() {
					if c == want {
						cats = append(cats, s.Name)
						break
					}
				}
			}
		}
		return resp.Strings(sortStrings(cats))
	}
	return resp.Errf("ERR unknown subcommand '%s'. Try ACL HELP.", x.Arg(1))
}
