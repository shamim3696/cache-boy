// Package semcache is a semantic cache for LLM apps: it stores responses by
// the embedding of the prompt, and a lookup returns the cached response of
// the most similar earlier prompt if its cosine similarity is above a
// threshold. "What's the capital of France?" can then hit the cached answer
// of "capital of france?" and skip a slow, paid model call.
//
// The server never calls an embedding model: clients send vectors, so any
// model (OpenAI, local sentence-transformers...) works. An optional exact
// prompt string short-circuits the vector search for repeated prompts.
package semcache

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"

	"github.com/shamim3696/cache-boy/internal/datatype/vector"
)

const (
	DefaultThreshold  = 0.92
	DefaultMaxEntries = 10000
)

var ErrNoCache = errors.New("ERR no such semantic cache")

type entry struct {
	id       string
	prompt   string
	response string
	expireAt int64 // unix ms; 0 = never
	lastUse  int64 // cache tick, not time: ordering must be exact
	hits     int64
}

type cache struct {
	idx        *vector.Index
	entries    map[string]*entry
	byPrompt   map[string]string // exact prompt → id
	threshold  float32
	maxEntries int
	hits       int64
	misses     int64
	tick       int64
}

func newCache() *cache {
	return &cache{
		idx:        vector.New(),
		entries:    map[string]*entry{},
		byPrompt:   map[string]string{},
		threshold:  DefaultThreshold,
		maxEntries: DefaultMaxEntries,
	}
}

// Manager holds every named cache. Methods take an emit func, called while
// the mutex is held, so propagated commands keep execution order.
//
// LRU eviction depends on reads, which aren't propagated, so only the node
// that serves clients evicts (and propagates SCACHE.DEL); replicas and AOF
// replay pass evict=false and follow those DELs.
type Manager struct {
	mu     sync.Mutex
	caches map[string]*cache
	seq    int64
}

func New() *Manager { return &Manager{caches: map[string]*cache{}} }

// Result of a lookup.
type Result struct {
	Hit      bool
	ID       string
	Response string
	Score    float32
}

// Set stores a response. id "" generates one. Returns the id.
func (m *Manager) Set(name, id, prompt string, vec []float32, response string, expireAt, now int64, evict bool, emit func(args ...string)) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.caches[name]
	if c == nil {
		c = newCache()
		m.caches[name] = c
	}
	if id == "" {
		m.seq++
		id = strconv.FormatInt(now, 10) + "-" + strconv.FormatInt(m.seq, 10)
	}
	if old, ok := c.entries[id]; ok && old.prompt != "" {
		delete(c.byPrompt, old.prompt)
	}
	if _, err := c.idx.Add(id, vec, ""); err != nil {
		return "", err
	}
	c.tick++
	c.entries[id] = &entry{id: id, prompt: prompt, response: response, expireAt: expireAt, lastUse: c.tick}
	if prompt != "" {
		c.byPrompt[prompt] = id
	}
	emit(setArgs(name, c.entries[id], vec)...)
	if evict {
		c.evict(now, func(victim string) { emit("SCACHE.DEL", name, victim) })
	}
	return id, nil
}

// Get finds the most similar entry. threshold 0 uses the cache's threshold.
func (m *Manager) Get(name string, vec []float32, prompt string, threshold float32, now int64) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.caches[name]
	if c == nil {
		return Result{}, nil
	}
	if threshold <= 0 {
		threshold = c.threshold
	}
	if id, ok := c.byPrompt[prompt]; ok && prompt != "" {
		if e := c.live(id, now); e != nil {
			return c.hit(e, 1, now), nil
		}
	}
	if c.idx.Len() == 0 {
		c.misses++
		return Result{}, nil
	}
	var expired []string
	res, err := c.idx.Search(vec, 1, func(id, _ string) bool {
		if e := c.entries[id]; e != nil && e.expireAt > 0 && e.expireAt <= now {
			expired = append(expired, id)
			return false
		}
		return true
	})
	for _, id := range expired {
		c.remove(id)
	}
	if err != nil {
		return Result{}, err
	}
	if len(res) == 0 || res[0].Score < threshold {
		c.misses++
		if len(res) > 0 {
			return Result{ID: res[0].ID, Score: res[0].Score}, nil
		}
		return Result{}, nil
	}
	return c.hit(c.entries[res[0].ID], res[0].Score, now), nil
}

// Del removes one entry.
func (m *Manager) Del(name, id string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.caches[name]
	return c != nil && c.remove(id)
}

// Configure sets the threshold (0 keeps it) and max entries (0 keeps it).
func (m *Manager) Configure(name string, threshold float32, maxEntries int, now int64, evict bool, emit func(args ...string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.caches[name]
	if c == nil {
		c = newCache()
		m.caches[name] = c
	}
	if threshold > 0 {
		c.threshold = threshold
	}
	if maxEntries > 0 {
		c.maxEntries = maxEntries
	}
	emit(configArgs(name, c)...)
	if evict {
		c.evict(now, func(victim string) { emit("SCACHE.DEL", name, victim) })
	}
}

// Drop deletes a whole cache and returns how many entries it had.
func (m *Manager) Drop(name string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.caches[name]
	delete(m.caches, name)
	if c == nil {
		return 0
	}
	return len(c.entries)
}

// Info describes a cache.
type Info struct {
	Entries    int
	Dim        int
	Threshold  float32
	MaxEntries int
	Hits       int64
	Misses     int64
}

func (m *Manager) Info(name string) (Info, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c := m.caches[name]
	if c == nil {
		return Info{}, ErrNoCache
	}
	return Info{len(c.entries), c.idx.Dim(), c.threshold, c.maxEntries, c.hits, c.misses}, nil
}

// Names lists caches (stored names).
func (m *Manager) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.caches))
	for n := range m.caches {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Dump emits commands that recreate every cache (snapshots, AOF rewrite, replica sync).
func (m *Manager) Dump(emit func(args []string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, c := range m.caches {
		emit(configArgs(name, c))
		for id, e := range c.entries {
			vec, _, _ := c.idx.Get(id)
			emit(setArgs(name, e, vec))
		}
	}
}

// --- internals ---

func (c *cache) live(id string, now int64) *entry {
	e := c.entries[id]
	if e == nil {
		return nil
	}
	if e.expireAt > 0 && e.expireAt <= now {
		c.remove(id)
		return nil
	}
	return e
}

func (c *cache) hit(e *entry, score float32, now int64) Result {
	c.hits++
	c.tick++
	e.hits++
	e.lastUse = c.tick
	return Result{Hit: true, ID: e.id, Response: e.response, Score: score}
}

func (c *cache) remove(id string) bool {
	e, ok := c.entries[id]
	if !ok {
		return false
	}
	delete(c.entries, id)
	if e.prompt != "" && c.byPrompt[e.prompt] == id {
		delete(c.byPrompt, e.prompt)
	}
	c.idx.Remove(id)
	return true
}

// evict drops expired entries, then least recently used ones over maxEntries.
func (c *cache) evict(now int64, onEvict func(id string)) {
	if len(c.entries) <= c.maxEntries {
		return
	}
	for id, e := range c.entries {
		if e.expireAt > 0 && e.expireAt <= now {
			c.remove(id)
		}
	}
	if over := len(c.entries) - c.maxEntries; over > 0 {
		all := make([]*entry, 0, len(c.entries))
		for _, e := range c.entries {
			all = append(all, e)
		}
		sort.Slice(all, func(i, j int) bool { return all[i].lastUse < all[j].lastUse })
		for _, e := range all[:over] {
			c.remove(e.id)
			onEvict(e.id)
		}
	}
}

func setArgs(name string, e *entry, vec []float32) []string {
	args := []string{"SCACHE.SET", name, FormatVector(vec), e.response, "ID", e.id}
	if e.prompt != "" {
		args = append(args, "PROMPT", e.prompt)
	}
	if e.expireAt > 0 {
		args = append(args, "PXAT", strconv.FormatInt(e.expireAt, 10))
	}
	return args
}

func configArgs(name string, c *cache) []string {
	return []string{"SCACHE.CONFIG", name,
		"THRESHOLD", strconv.FormatFloat(float64(c.threshold), 'f', -1, 32),
		"MAXENTRIES", strconv.Itoa(c.maxEntries)}
}

// FormatVector encodes a vector as a JSON array.
func FormatVector(v []float32) string {
	b := make([]byte, 0, len(v)*10+2)
	b = append(b, '[')
	for i, f := range v {
		if i > 0 {
			b = append(b, ',')
		}
		b = strconv.AppendFloat(b, float64(f), 'g', -1, 32)
	}
	return string(append(b, ']'))
}

// ParseVector accepts "[0.1, 0.2]" or "0.1,0.2".
func ParseVector(s string) ([]float32, error) {
	if len(s) >= 2 && s[0] == '[' && s[len(s)-1] == ']' {
		s = s[1 : len(s)-1]
	}
	var out []float32
	start := 0
	for i := 0; i <= len(s); i++ {
		if i < len(s) && s[i] != ',' {
			continue
		}
		part := trimSpace(s[start:i])
		start = i + 1
		if part == "" {
			continue
		}
		f, err := strconv.ParseFloat(part, 32)
		if err != nil {
			return nil, fmt.Errorf("ERR invalid vector element %q", part)
		}
		out = append(out, float32(f))
	}
	if len(out) == 0 {
		return nil, errors.New("ERR empty vector")
	}
	return out, nil
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\n') {
		s = s[:len(s)-1]
	}
	return s
}
