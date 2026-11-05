package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"cache-boy/internal/glob"
)

// SetUser creates or modifies a user with ACL rules, e.g.
//
//	ACL SETUSER alice on >secret ~cache:* +@read +set -flushall
//
// Rules: on, off, >pass, <pass, nopass, resetpass, ~pattern, allkeys,
// resetkeys, +cmd, -cmd, +@category, -@category, allcommands (+@all),
// nocommands (-@all), reset.
func (s *Store) SetUser(name string, rules []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var u *User
	if old, ok := s.users[name]; ok {
		u = old.clone()
	} else {
		u = newUser(name)
	}
	for _, r := range rules {
		if err := u.apply(r); err != nil {
			return err
		}
	}
	s.users[name] = u
	return nil
}

func (u *User) apply(rule string) error {
	lower := strings.ToLower(rule)
	switch {
	case lower == "on":
		u.Enabled = true
	case lower == "off":
		u.Enabled = false
	case lower == "nopass":
		u.NoPass, u.passwords = true, nil
	case lower == "resetpass":
		u.NoPass, u.passwords = false, nil
	case strings.HasPrefix(rule, ">"):
		h := sha256.Sum256([]byte(rule[1:]))
		u.passwords = append(u.passwords, h)
		u.NoPass = false
	case strings.HasPrefix(rule, "<"):
		h := sha256.Sum256([]byte(rule[1:]))
		kept := u.passwords[:0]
		for _, p := range u.passwords {
			if p != h {
				kept = append(kept, p)
			}
		}
		u.passwords = kept
	case lower == "allkeys" || rule == "~*":
		u.AllKeys, u.KeyPatterns = true, nil
	case lower == "resetkeys":
		u.AllKeys, u.KeyPatterns = false, nil
	case strings.HasPrefix(rule, "~"):
		u.KeyPatterns = append(u.KeyPatterns, rule[1:])
	case lower == "allcommands" || lower == "+@all":
		u.AllCommands = true
		u.allowedCmds, u.allowedCats = map[string]bool{}, map[string]bool{}
	case lower == "nocommands" || lower == "-@all":
		u.AllCommands = false
		u.allowedCmds, u.allowedCats = map[string]bool{}, map[string]bool{}
	case strings.HasPrefix(lower, "+@"):
		u.allowedCats[lower[2:]] = true
	case strings.HasPrefix(lower, "-@"):
		u.allowedCats[lower[2:]] = false
	case strings.HasPrefix(lower, "+"):
		u.allowedCmds[lower[1:]] = true
	case strings.HasPrefix(lower, "-"):
		u.allowedCmds[lower[1:]] = false
	case lower == "reset":
		*u = *newUser(u.Name)
	default:
		return fmt.Errorf("ERR Error in ACL SETUSER modifier '%s': Syntax error", rule)
	}
	return nil
}

// Check returns an error if user may not run command (with its categories)
// on the given keys.
func (s *Store) Check(user, command string, categories []string, keys []string) error {
	u, ok := s.User(user)
	if !ok || !u.Enabled {
		return fmt.Errorf("NOPERM user '%s' is disabled or deleted", user)
	}
	if !u.canRun(command, categories) {
		return fmt.Errorf("NOPERM User %s has no permissions to run the '%s' command", user, command)
	}
	if !u.AllKeys {
		for _, k := range keys {
			if !u.canAccess(k) {
				return fmt.Errorf("NOPERM No permissions to access a key")
			}
		}
	}
	return nil
}

func (u *User) canRun(command string, categories []string) bool {
	if allowed, ok := u.allowedCmds[command]; ok {
		return allowed
	}
	// A denied category beats an allowed one.
	verdict := u.AllCommands
	for _, c := range categories {
		if allowed, ok := u.allowedCats[c]; ok {
			if !allowed {
				return false
			}
			verdict = true
		}
	}
	return verdict
}

func (u *User) canAccess(key string) bool {
	for _, p := range u.KeyPatterns {
		if glob.Match(p, key) {
			return true
		}
	}
	return false
}

// Describe returns the user's rules in ACL LIST format.
func (s *Store) Describe(name string) (string, bool) {
	u, ok := s.User(name)
	if !ok {
		return "", false
	}
	parts := []string{"user", u.Name}
	if u.Enabled {
		parts = append(parts, "on")
	} else {
		parts = append(parts, "off")
	}
	if u.NoPass {
		parts = append(parts, "nopass")
	}
	for _, p := range u.passwords {
		parts = append(parts, "#"+hex.EncodeToString(p[:]))
	}
	if u.AllKeys {
		parts = append(parts, "~*")
	} else {
		for _, p := range u.KeyPatterns {
			parts = append(parts, "~"+p)
		}
	}
	if u.AllCommands {
		parts = append(parts, "+@all")
	} else {
		parts = append(parts, "-@all")
	}
	cats := sortedKeys(u.allowedCats)
	for _, c := range cats {
		parts = append(parts, sign(u.allowedCats[c])+"@"+c)
	}
	for _, c := range sortedKeys(u.allowedCmds) {
		parts = append(parts, sign(u.allowedCmds[c])+c)
	}
	return strings.Join(parts, " "), true
}

func sign(b bool) string {
	if b {
		return "+"
	}
	return "-"
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
