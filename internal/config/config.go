// Package config holds server settings. Values come from defaults, then an
// optional config file ("key value" per line, like redis.conf), then
// command-line flags (--key value). CONFIG SET can change most values at runtime.
package config

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"cache-boy/internal/glob"
)

// SaveRule triggers a snapshot after Seconds if at least Changes writes happened.
type SaveRule struct {
	Seconds int
	Changes int
}

type Config struct {
	Bind     string
	Port     int
	HTTPPort int // REST + WebSocket + dashboard; 0 disables
	Shards   int // keyspace shards (power of two)

	RequirePass string
	MaxClients  int
	Timeout     int // idle client timeout in seconds; 0 = never

	MaxMemory        int64  // bytes; 0 = unlimited
	MaxMemoryPolicy  string // noeviction, {allkeys,volatile}-{lru,lfu,tinylfu,random}, volatile-ttl
	MaxMemorySamples int
	HZ               int // background task frequency (active expiry etc.)

	Dir                      string
	DBFilename               string
	Save                     []SaveRule
	AppendOnly               bool
	AppendFilename           string
	AppendFsync              string // always, everysec, no
	AutoAOFRewritePercentage int
	AutoAOFRewriteMinSize    int64

	CompressionThreshold int // compress string values >= this many bytes; 0 = off
	HistoryDepth         int // versions kept per string key; 0 = off

	ReplicaOf  string // "host port" or "" for leader
	MasterAuth string // password used to authenticate to the leader
	MasterUser string // ACL user for MasterAuth ("" = default)
	// ReplBacklogSize is how many bytes of recent writes a leader keeps so a
	// reconnecting replica can catch up without a full resync.
	ReplBacklogSize int64

	ClusterEnabled bool
	ClusterNodeID  string
	ClusterNodes   []string // "id host:port slotStart-slotEnd"
	RaftPort       int
	RaftPeers      []string // "id raftHost:raftPort dataHost:dataPort"

	SlowlogLogSlowerThan int64 // microseconds; <0 disables
	SlowlogMaxLen        int

	ScriptTimeoutMs int
	LogLevel        string
}

func Default() *Config {
	return &Config{
		Bind:                     "127.0.0.1",
		Port:                     6380,
		HTTPPort:                 6381,
		Shards:                   64,
		MaxClients:               10000,
		MaxMemoryPolicy:          "noeviction",
		MaxMemorySamples:         5,
		HZ:                       10,
		Dir:                      ".",
		DBFilename:               "dump.cbdump",
		Save:                     []SaveRule{{3600, 1}, {300, 100}, {60, 10000}},
		AppendFilename:           "appendonly.aof",
		AppendFsync:              "everysec",
		AutoAOFRewritePercentage: 100,
		AutoAOFRewriteMinSize:    64 << 20,
		ReplBacklogSize:          1 << 20,
		CompressionThreshold:     0,
		SlowlogLogSlowerThan:     10000,
		SlowlogMaxLen:            128,
		ScriptTimeoutMs:          5000,
		LogLevel:                 "notice",
	}
}

// Store gives lock-free reads of an immutable snapshot; Set swaps in a copy.
type Store struct {
	v  atomic.Pointer[Config]
	mu sync.Mutex
}

func NewStore(c *Config) *Store {
	s := &Store{}
	s.v.Store(c)
	return s
}

// Get returns the current config. Never modify the returned value.
func (s *Store) Get() *Config { return s.v.Load() }

// Set changes one parameter at runtime (CONFIG SET).
func (s *Store) Set(name, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	c := *s.v.Load()
	if err := c.set(strings.ToLower(name), []string{value}); err != nil {
		return err
	}
	s.v.Store(&c)
	return nil
}

// Load reads a config file into c.
func (c *Config) Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		if err := c.set(strings.ToLower(fields[0]), fields[1:]); err != nil {
			return fmt.Errorf("%s:%d: %w", path, line, err)
		}
	}
	return sc.Err()
}

// ApplyArgs applies "--key value [value...]" style arguments.
func (c *Config) ApplyArgs(args []string) error {
	for i := 0; i < len(args); {
		if !strings.HasPrefix(args[i], "--") {
			return fmt.Errorf("unexpected argument %q", args[i])
		}
		name := strings.ToLower(strings.TrimPrefix(args[i], "--"))
		j := i + 1
		for j < len(args) && !strings.HasPrefix(args[j], "--") {
			j++
		}
		vals := args[i+1 : j]
		if len(vals) == 0 {
			vals = []string{"yes"} // bare flag, e.g. --appendonly
		}
		if err := c.set(name, vals); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func (c *Config) set(name string, vals []string) error {
	v := strings.Join(vals, " ")
	var err error
	switch name {
	case "bind":
		c.Bind = v
	case "port":
		c.Port, err = strconv.Atoi(v)
	case "http-port":
		c.HTTPPort, err = strconv.Atoi(v)
	case "shards":
		c.Shards, err = strconv.Atoi(v)
	case "requirepass":
		c.RequirePass = v
	case "maxclients":
		c.MaxClients, err = strconv.Atoi(v)
	case "timeout":
		c.Timeout, err = strconv.Atoi(v)
	case "maxmemory":
		c.MaxMemory, err = ParseBytes(v)
	case "maxmemory-policy":
		switch v {
		case "noeviction", "allkeys-lru", "allkeys-lfu", "allkeys-random", "allkeys-tinylfu",
			"volatile-lru", "volatile-lfu", "volatile-random", "volatile-ttl", "volatile-tinylfu":
			c.MaxMemoryPolicy = v
		default:
			err = fmt.Errorf("invalid maxmemory-policy %q", v)
		}
	case "maxmemory-samples":
		c.MaxMemorySamples, err = strconv.Atoi(v)
	case "hz":
		c.HZ, err = strconv.Atoi(v)
	case "dir":
		c.Dir = v
	case "dbfilename":
		c.DBFilename = v
	case "save":
		c.Save, err = parseSave(vals)
	case "appendonly":
		c.AppendOnly, err = parseBool(v)
	case "appendfilename":
		c.AppendFilename = v
	case "appendfsync":
		if v != "always" && v != "everysec" && v != "no" {
			err = fmt.Errorf("invalid appendfsync %q", v)
		}
		c.AppendFsync = v
	case "auto-aof-rewrite-percentage":
		c.AutoAOFRewritePercentage, err = strconv.Atoi(v)
	case "auto-aof-rewrite-min-size":
		c.AutoAOFRewriteMinSize, err = ParseBytes(v)
	case "compression-threshold":
		c.CompressionThreshold, err = strconv.Atoi(v)
	case "history-depth":
		c.HistoryDepth, err = strconv.Atoi(v)
	case "replicaof", "slaveof":
		if strings.EqualFold(v, "no one") {
			v = ""
		}
		c.ReplicaOf = v
	case "repl-backlog-size":
		c.ReplBacklogSize, err = ParseBytes(v)
	case "masterauth":
		c.MasterAuth = v
	case "masteruser":
		c.MasterUser = v
	case "cluster-enabled":
		c.ClusterEnabled, err = parseBool(v)
	case "cluster-node-id":
		c.ClusterNodeID = v
	case "cluster-node":
		c.ClusterNodes = append(c.ClusterNodes, v)
	case "raft-port":
		c.RaftPort, err = strconv.Atoi(v)
	case "raft-peer":
		c.RaftPeers = append(c.RaftPeers, v)
	case "slowlog-log-slower-than":
		c.SlowlogLogSlowerThan, err = strconv.ParseInt(v, 10, 64)
	case "slowlog-max-len":
		c.SlowlogMaxLen, err = strconv.Atoi(v)
	case "script-timeout":
		c.ScriptTimeoutMs, err = strconv.Atoi(v)
	case "loglevel":
		c.LogLevel = v
	default:
		return fmt.Errorf("unknown config parameter %q", name)
	}
	if err != nil {
		return fmt.Errorf("invalid value for %s: %w", name, err)
	}
	return nil
}

// Params returns parameter name → value for CONFIG GET, filtered by a glob pattern.
func (c *Config) Params(pattern string) [][2]string {
	all := map[string]string{
		"bind":                        c.Bind,
		"port":                        strconv.Itoa(c.Port),
		"http-port":                   strconv.Itoa(c.HTTPPort),
		"shards":                      strconv.Itoa(c.Shards),
		"requirepass":                 c.RequirePass,
		"maxclients":                  strconv.Itoa(c.MaxClients),
		"timeout":                     strconv.Itoa(c.Timeout),
		"maxmemory":                   strconv.FormatInt(c.MaxMemory, 10),
		"maxmemory-policy":            c.MaxMemoryPolicy,
		"maxmemory-samples":           strconv.Itoa(c.MaxMemorySamples),
		"hz":                          strconv.Itoa(c.HZ),
		"dir":                         c.Dir,
		"dbfilename":                  c.DBFilename,
		"save":                        formatSave(c.Save),
		"appendonly":                  formatBool(c.AppendOnly),
		"appendfilename":              c.AppendFilename,
		"appendfsync":                 c.AppendFsync,
		"auto-aof-rewrite-percentage": strconv.Itoa(c.AutoAOFRewritePercentage),
		"auto-aof-rewrite-min-size":   strconv.FormatInt(c.AutoAOFRewriteMinSize, 10),
		"compression-threshold":       strconv.Itoa(c.CompressionThreshold),
		"history-depth":               strconv.Itoa(c.HistoryDepth),
		"replicaof":                   c.ReplicaOf,
		"masterauth":                  c.MasterAuth,
		"repl-backlog-size":           strconv.FormatInt(c.ReplBacklogSize, 10),
		"masteruser":                  c.MasterUser,
		"cluster-enabled":             formatBool(c.ClusterEnabled),
		"cluster-node-id":             c.ClusterNodeID,
		"raft-port":                   strconv.Itoa(c.RaftPort),
		"slowlog-log-slower-than":     strconv.FormatInt(c.SlowlogLogSlowerThan, 10),
		"slowlog-max-len":             strconv.Itoa(c.SlowlogMaxLen),
		"script-timeout":              strconv.Itoa(c.ScriptTimeoutMs),
		"loglevel":                    c.LogLevel,
	}
	var out [][2]string
	for k, v := range all {
		if glob.Match(strings.ToLower(pattern), k) {
			out = append(out, [2]string{k, v})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// ParseBytes accepts plain numbers or units: 100mb, 1gb, 512kb.
func ParseBytes(s string) (int64, error) {
	s = strings.ToLower(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{{"gb", 1 << 30}, {"mb", 1 << 20}, {"kb", 1 << 10}, {"g", 1e9}, {"m", 1e6}, {"k", 1e3}, {"b", 1}} {
		if strings.HasSuffix(s, u.suffix) {
			mult = u.mult
			s = strings.TrimSuffix(s, u.suffix)
			break
		}
	}
	n, err := strconv.ParseInt(s, 10, 64)
	return n * mult, err
}

func parseBool(v string) (bool, error) {
	switch strings.ToLower(v) {
	case "yes", "true", "1", "on":
		return true, nil
	case "no", "false", "0", "off":
		return false, nil
	}
	return false, fmt.Errorf("expected yes/no, got %q", v)
}

func formatBool(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

func parseSave(vals []string) ([]SaveRule, error) {
	if len(vals) == 1 && (vals[0] == "" || vals[0] == `""`) {
		return nil, nil
	}
	if len(vals) == 1 {
		vals = strings.Fields(vals[0])
	}
	if len(vals)%2 != 0 {
		return nil, fmt.Errorf("save needs pairs of <seconds> <changes>")
	}
	var rules []SaveRule
	for i := 0; i < len(vals); i += 2 {
		s, err1 := strconv.Atoi(vals[i])
		n, err2 := strconv.Atoi(vals[i+1])
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("invalid save rule %q %q", vals[i], vals[i+1])
		}
		rules = append(rules, SaveRule{s, n})
	}
	return rules, nil
}

func formatSave(rules []SaveRule) string {
	parts := make([]string, 0, len(rules)*2)
	for _, r := range rules {
		parts = append(parts, strconv.Itoa(r.Seconds), strconv.Itoa(r.Changes))
	}
	return strings.Join(parts, " ")
}
