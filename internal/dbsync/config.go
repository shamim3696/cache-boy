// Package dbsync keeps cache-boy in step with a PostgreSQL database. It reads
// the database's change stream (logical replication, pgoutput) and, for every
// committed change to a configured table, invalidates the cache tags and keys
// that rules derive from the changed row.
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

// Config is the cache-boy-sync configuration file.
type Config struct {
	Postgres PostgresConfig `yaml:"postgres"`
	Cache    CacheConfig    `yaml:"cache"`
	Rules    []RuleConfig   `yaml:"rules"`
}

type PostgresConfig struct {
	// URL is a libpq connection string or URL. The user needs the
	// REPLICATION attribute (or to be a superuser) and, to create the
	// publication, ownership of the tables.
	URL string `yaml:"url"`
	// Slot is the replication slot that remembers how far the stream has been
	// processed, so nothing is missed across restarts. Default "cache_boy".
	Slot string `yaml:"slot"`
	// Publication lists the tables Postgres streams changes for. It is
	// created, or extended with missing tables, at startup. Default "cache_boy".
	Publication string `yaml:"publication"`
}

type CacheConfig struct {
	Addr      string `yaml:"addr"` // default 127.0.0.1:6380
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
	Namespace string `yaml:"namespace"`
}

type RuleConfig struct {
	// Table is "name" (schema public) or "schema.name".
	Table string `yaml:"table"`
	// Tags are invalidated with TAG.INVALIDATE; Keys are deleted with DEL.
	// Both may use {column} placeholders.
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
	schema, table string
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
	if c.Postgres.Slot == "" {
		c.Postgres.Slot = "cache_boy"
	}
	if c.Postgres.Publication == "" {
		c.Postgres.Publication = "cache_boy"
	}
	if c.Cache.Addr == "" {
		c.Cache.Addr = "127.0.0.1:6380"
	}
	if _, err := c.compile(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) compile() ([]*rule, error) {
	if c.Postgres.URL == "" {
		return nil, errors.New("config: postgres.url is required")
	}
	if !identRe.MatchString(c.Postgres.Slot) {
		return nil, fmt.Errorf("config: postgres.slot %q must be lowercase letters, digits and _", c.Postgres.Slot)
	}
	if !identRe.MatchString(c.Postgres.Publication) {
		return nil, fmt.Errorf("config: postgres.publication %q must be lowercase letters, digits and _", c.Postgres.Publication)
	}
	if len(c.Rules) == 0 {
		return nil, errors.New("config: at least one rule is required")
	}
	var rules []*rule
	for i, rc := range c.Rules {
		where := fmt.Sprintf("config: rules[%d]", i)
		if rc.Table == "" {
			return nil, fmt.Errorf("%s: table is required", where)
		}
		r := &rule{schema: "public", table: rc.Table, on: opAll}
		if s, t, ok := strings.Cut(rc.Table, "."); ok {
			r.schema, r.table = s, t
		}
		where = fmt.Sprintf("config: rule for %s", r.name())
		if len(rc.Tags)+len(rc.Keys) == 0 {
			return nil, fmt.Errorf("%s: needs tags or keys", where)
		}
		for _, s := range rc.Tags {
			t, err := parseTemplate(s)
			if err != nil {
				return nil, fmt.Errorf("%s: tag %w", where, err)
			}
			r.tags = append(r.tags, t)
		}
		for _, s := range rc.Keys {
			t, err := parseTemplate(s)
			if err != nil {
				return nil, fmt.Errorf("%s: key %w", where, err)
			}
			r.keys = append(r.keys, t)
		}
		if len(rc.On) > 0 {
			r.on = 0
			for _, s := range rc.On {
				o, ok := opNames[strings.ToLower(s)]
				if !ok {
					return nil, fmt.Errorf("%s: unknown operation %q (insert, update, delete, truncate)", where, s)
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
