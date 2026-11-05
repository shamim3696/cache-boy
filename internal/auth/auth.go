// Package auth manages users and passwords (AUTH, requirepass) and per-user
// permissions (ACL). See acl.go for the rule language.
package auth

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"sort"
	"sync"
)

const DefaultUser = "default"

var (
	ErrWrongPass  = errors.New("WRONGPASS invalid username-password pair or user is disabled.")
	ErrNoSuchUser = errors.New("ERR no such user")
)

type User struct {
	Name        string
	Enabled     bool
	NoPass      bool
	passwords   [][32]byte // sha256 of each accepted password
	AllCommands bool
	allowedCmds map[string]bool // explicit +cmd / -cmd (true/false)
	allowedCats map[string]bool // +@cat / -@cat
	AllKeys     bool
	KeyPatterns []string
}

func newUser(name string) *User {
	return &User{Name: name, allowedCmds: map[string]bool{}, allowedCats: map[string]bool{}}
}

func (u *User) clone() *User {
	c := *u
	c.passwords = append([][32]byte(nil), u.passwords...)
	c.allowedCmds = make(map[string]bool, len(u.allowedCmds))
	for k, v := range u.allowedCmds {
		c.allowedCmds[k] = v
	}
	c.allowedCats = make(map[string]bool, len(u.allowedCats))
	for k, v := range u.allowedCats {
		c.allowedCats[k] = v
	}
	c.KeyPatterns = append([]string(nil), u.KeyPatterns...)
	return &c
}

func (u *User) checkPassword(pass string) bool {
	if u.NoPass {
		return true
	}
	h := sha256.Sum256([]byte(pass))
	for _, p := range u.passwords {
		if subtle.ConstantTimeCompare(p[:], h[:]) == 1 {
			return true
		}
	}
	return false
}

// Store holds all users. Users are copy-on-write so readers never see a half-updated user.
type Store struct {
	mu    sync.RWMutex
	users map[string]*User
}

// New creates the store with a "default" user that has every permission and
// either no password or requirepass.
func New(requirepass string) *Store {
	s := &Store{users: map[string]*User{}}
	u := newUser(DefaultUser)
	u.Enabled, u.AllCommands, u.AllKeys = true, true, true
	s.users[DefaultUser] = u
	s.SetDefaultPassword(requirepass)
	return s
}

// SetDefaultPassword is used by requirepass / CONFIG SET requirepass.
func (s *Store) SetDefaultPassword(pass string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.users[DefaultUser].clone()
	if pass == "" {
		u.NoPass, u.passwords = true, nil
	} else {
		u.NoPass, u.passwords = false, [][32]byte{sha256.Sum256([]byte(pass))}
	}
	s.users[DefaultUser] = u
}

// User returns the current version of a user.
func (s *Store) User(name string) (*User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[name]
	return u, ok
}

// Authenticate checks a username/password pair.
func (s *Store) Authenticate(name, pass string) error {
	u, ok := s.User(name)
	if !ok || !u.Enabled || !u.checkPassword(pass) {
		return ErrWrongPass
	}
	return nil
}

// AutoLogin reports whether new connections are authenticated as "default"
// without AUTH (true when the default user has no password).
func (s *Store) AutoLogin() bool {
	u, _ := s.User(DefaultUser)
	return u.Enabled && u.NoPass
}

// Users returns user names, sorted.
func (s *Store) Users() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.users))
	for n := range s.users {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func (s *Store) DelUser(name string) bool {
	if name == DefaultUser {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.users[name]
	delete(s.users, name)
	return ok
}
