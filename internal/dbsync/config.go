// Package dbsync keeps cache-boy in step with databases. It reads each
// database's change stream (PostgreSQL logical replication, MySQL binlog,
// MongoDB change streams) and, for every committed change to a configured
// table or collection, invalidates the cache tags and keys that rules derive
// from the changed row.
//
// It is off unless the config says auto_invalidate: true. Without it,
// applications invalidate the cache themselves (TAG.INVALIDATE / DEL).
package dbsync

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config is the db-sync configuration file.
type Config struct {
	// AutoInvalidate is the master switch. When false (the default) nothing
	// is invalidated automatically and the cache is only changed by the
	// application.
	AutoInvalidate bool           `yaml:"auto_invalidate"`
	Cache          CacheConfig    `yaml:"cache"`
	Sources        []SourceConfig `yaml:"sources"`
}

type CacheConfig struct {
	Addr      string `yaml:"addr"` // default 127.0.0.1:6380; unused when embedded in the server
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Namespace string `yaml:"namespace"`
}

// Source types.
const (
	TypePostgres = "postgres"
	TypeMySQL    = "mysql"
	TypeMongoDB  = "mongodb"
)

// SourceConfig is one database to watch.
type SourceConfig struct {
	// Name identifies the source in logs and in the default state file name.
	// Default: the type, plus an index when there are several of one type.
	Name string `yaml:"name"`
	Type string `yaml:"type"` // postgres | mysql | mongodb
	URL  string `yaml:"url"`

	// PostgreSQL: replication slot and publication (default "cache_boy").
	Slot        string `yaml:"slot"`
	Publication string `yaml:"publication"`

	// MySQL: the replica server id this reader uses; it must be unique among
	// the server's replicas. Default 1673 + source index.
	ServerID uint32 `yaml:"server_id"`

	// MongoDB: database to watch (default: the database in the URL path).
	Database string `yaml:"database"`

	// MySQL and MongoDB keep their stream position in this file so a
	// restart resumes where it stopped. Default "<name>.cbsync-state" in
	// the state directory (the server's dir when embedded).
	StateFile string `yaml:"state_file"`

	Rules []RuleConfig `yaml:"rules"`
}

type RuleConfig struct {
	// Table is "name" or "schema.name" (PostgreSQL schema, MySQL database).
	// For MongoDB use Collection (Table is accepted too).
	Table      string `yaml:"table"`
	Collection string `yaml:"collection"`
	// Tags are invalidated with TAG.INVALIDATE; Keys are deleted with DEL.
	// Both may use {column} placeholders; MongoDB fields may be dotted
	// paths such as {profile.city}.
	Tags []string `yaml:"tags"`
	Keys []string `yaml:"keys"`
	// On limits the rule to some operations: insert, update, delete,
	// truncate. Default: all of them.
	On []string `yaml:"on"`
}

type op uint8

const (
	opInsert op = 1 << iota
	opUpdate
	opDelete
	opTruncate
	opAll = opInsert | opUpdate | opDelete | opTruncate
)

var opNames = map[string]op{"insert": opInsert, "update": opUpdate, "delete": opDelete, "truncate": opTruncate}

// rule is a compiled RuleConfig.
type rule struct {
	schema, table string // schema: PostgreSQL schema, MySQL/MongoDB database
	tags, keys    []*template
	on            op
}

func (r *rule) name() string { return r.schema + "." + r.table }

var identRe = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Load reads a YAML config file. ${VAR} references are replaced with
// environment variables first, so passwords can stay out of the file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse parses and validates a YAML config.
func Parse(data []byte) (*Config, error) {
	data = []byte(os.ExpandEnv(string(data)))
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true) // catch typos such as "tag:" instead of "tags:"
	var c Config
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
	if c.Cache.Addr == "" {
		c.Cache.Addr = "127.0.0.1:6380"
	}
	if len(c.Sources) == 0 {
		return nil, errors.New("config: at least one source is required")
	}
	count := map[string]int{}
	for _, s := range c.Sources {
		count[s.Type]++
	}
	seen := map[string]int{}
	names := map[string]bool{}
	for i := range c.Sources {
		s := &c.Sources[i]
		s.Type = strings.ToLower(s.Type)
		switch s.Type {
		case TypePostgres, TypeMySQL, TypeMongoDB:
		case "postgresql", "pg":
			s.Type = TypePostgres
		case "mongo":
			s.Type = TypeMongoDB
		default:
			return nil, fmt.Errorf("config: sources[%d]: type must be postgres, mysql or mongodb, not %q", i, s.Type)
		}
		if s.Name == "" {
			s.Name = s.Type
			if count[s.Type] > 1 {
				s.Name = fmt.Sprintf("%s-%d", s.Type, seen[s.Type]+1)
			}
		}
		seen[s.Type]++
		if names[s.Name] {
			return nil, fmt.Errorf("config: two sources are named %q", s.Name)
		}
		names[s.Name] = true
		if s.URL == "" {
			return nil, fmt.Errorf("config: source %q: url is required", s.Name)
		}
		if s.Slot == "" {
			s.Slot = "cache_boy"
		}
		if s.Publication == "" {
			s.Publication = "cache_boy"
		}
		if s.ServerID == 0 {
			s.ServerID = 1673 + uint32(i)
		}
		if s.Type == TypePostgres {
			if !identRe.MatchString(s.Slot) {
				return nil, fmt.Errorf("config: source %q: slot %q must be lowercase letters, digits and _", s.Name, s.Slot)
			}
			if !identRe.MatchString(s.Publication) {
				return nil, fmt.Errorf("config: source %q: publication %q must be lowercase letters, digits and _", s.Name, s.Publication)
			}
		}
		if _, err := compileRules(s, "x"); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// compileRules compiles a source's rules. defaultSchema is used for tables
// given without one (PostgreSQL "public", the MySQL/MongoDB database).
func compileRules(s *SourceConfig, defaultSchema string) ([]*rule, error) {
	if len(s.Rules) == 0 {
		return nil, fmt.Errorf("config: source %q: at least one rule is required", s.Name)
	}
	var rules []*rule
	for i, rc := range s.Rules {
		where := fmt.Sprintf("config: source %q: rules[%d]", s.Name, i)
		name := rc.Table
		if rc.Collection != "" {
			if name != "" {
				return nil, fmt.Errorf("%s: give table or collection, not both", where)
			}
			name = rc.Collection
		}
		if name == "" {
			return nil, fmt.Errorf("%s: table (or collection) is required", where)
		}
		r := &rule{schema: defaultSchema, table: name, on: opAll}
		if sc, t, ok := strings.Cut(name, "."); ok {
			r.schema, r.table = sc, t
		}
		where = fmt.Sprintf("config: source %q: rule for %s", s.Name, name)
		if len(rc.Tags)+len(rc.Keys) == 0 {
			return nil, fmt.Errorf("%s: needs tags or keys", where)
		}
		for _, str := range rc.Tags {
			t, err := parseTemplate(str)
			if err != nil {
				return nil, fmt.Errorf("%s: tag %w", where, err)
			}
			r.tags = append(r.tags, t)
		}
		for _, str := range rc.Keys {
			t, err := parseTemplate(str)
			if err != nil {
				return nil, fmt.Errorf("%s: key %w", where, err)
			}
			r.keys = append(r.keys, t)
		}
		if len(rc.On) > 0 {
			r.on = 0
			for _, str := range rc.On {
				o, ok := opNames[strings.ToLower(str)]
				if !ok {
					return nil, fmt.Errorf("%s: unknown operation %q (insert, update, delete, truncate)", where, str)
				}
				r.on |= o
			}
		}
		rules = append(rules, r)
	}
	return rules, nil
}

// columns returns the placeholder columns the rule's templates use.
func (r *rule) columns() []string {
	seen := map[string]bool{}
	var out []string
	for _, ts := range [][]*template{r.tags, r.keys} {
		for _, t := range ts {
			for _, c := range t.cols {
				if !seen[c] {
					seen[c] = true
					out = append(out, c)
				}
			}
		}
	}
	return out
}

// distinctTables returns one rule per watched table, sorted by name.
func distinctTables(rules []*rule) []*rule {
	seen := map[string]bool{}
	var out []*rule
	for _, r := range rules {
		if !seen[r.name()] {
			seen[r.name()] = true
			out = append(out, r)
		}
	}
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].name() < out[j-1].name(); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out
}
