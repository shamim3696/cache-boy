// Package observability collects metrics (per-command calls and latency),
// the slow log, and MONITOR streaming (tracing.go).
package observability

import (
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Latency histogram bucket upper bounds in microseconds.
var buckets = []int64{10, 50, 100, 250, 500, 1000, 5000, 10000, 50000, 100000, 1000000}

type CommandStats struct {
	Calls    atomic.Int64
	Errors   atomic.Int64
	Usec     atomic.Int64
	Buckets  [12]atomic.Int64 // len(buckets)+1, last is +Inf
}

type Metrics struct {
	Started           time.Time
	TotalCommands     atomic.Int64
	TotalConnections  atomic.Int64
	RejectedConns     atomic.Int64
	ConnectedClients  atomic.Int64
	NetInputBytes     atomic.Int64
	NetOutputBytes    atomic.Int64

	mu       sync.RWMutex
	commands map[string]*CommandStats
}

func NewMetrics() *Metrics {
	return &Metrics{Started: time.Now(), commands: map[string]*CommandStats{}}
}

// Observe records one executed command.
func (m *Metrics) Observe(cmd string, dur time.Duration, failed bool) {
	m.TotalCommands.Add(1)
	st := m.stats(cmd)
	st.Calls.Add(1)
	us := dur.Microseconds()
	st.Usec.Add(us)
	if failed {
		st.Errors.Add(1)
	}
	i := sort.Search(len(buckets), func(i int) bool { return us <= buckets[i] })
	st.Buckets[i].Add(1)
}

func (m *Metrics) stats(cmd string) *CommandStats {
	m.mu.RLock()
	st := m.commands[cmd]
	m.mu.RUnlock()
	if st != nil {
		return st
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if st = m.commands[cmd]; st == nil {
		st = &CommandStats{}
		m.commands[cmd] = st
	}
	return st
}

// CommandSnapshot is a plain copy of CommandStats.
type CommandSnapshot struct {
	Name   string
	Calls  int64
	Errors int64
	Usec   int64
}

// Commands returns stats for every command that has run, sorted by name.
func (m *Metrics) Commands() []CommandSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]CommandSnapshot, 0, len(m.commands))
	for name, st := range m.commands {
		out = append(out, CommandSnapshot{name, st.Calls.Load(), st.Errors.Load(), st.Usec.Load()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// ResetCommands clears per-command stats (CONFIG RESETSTAT).
func (m *Metrics) ResetCommands() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.commands = map[string]*CommandStats{}
}

// Gauge is an extra value written by WritePrometheus (memory, keys, ...).
type Gauge struct {
	Name, Help string
	Value      float64
}

// WritePrometheus writes all metrics in the Prometheus text format.
func (m *Metrics) WritePrometheus(w io.Writer, gauges []Gauge) {
	for _, g := range gauges {
		fmt.Fprintf(w, "# HELP cacheboy_%s %s\n# TYPE cacheboy_%s gauge\ncacheboy_%s %g\n", g.Name, g.Help, g.Name, g.Name, g.Value)
	}
	counter := func(name, help string, v int64) {
		fmt.Fprintf(w, "# HELP cacheboy_%s %s\n# TYPE cacheboy_%s counter\ncacheboy_%s %d\n", name, help, name, name, v)
	}
	counter("commands_total", "Commands processed.", m.TotalCommands.Load())
	counter("connections_total", "Connections accepted.", m.TotalConnections.Load())
	counter("net_input_bytes_total", "Bytes read from clients.", m.NetInputBytes.Load())
	counter("net_output_bytes_total", "Bytes written to clients.", m.NetOutputBytes.Load())

	m.mu.RLock()
	defer m.mu.RUnlock()
	names := make([]string, 0, len(m.commands))
	for n := range m.commands {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintf(w, "# HELP cacheboy_command_duration_seconds Command latency.\n# TYPE cacheboy_command_duration_seconds histogram\n")
	for _, n := range names {
		st := m.commands[n]
		var cum int64
		for i, b := range buckets {
			cum += st.Buckets[i].Load()
			fmt.Fprintf(w, "cacheboy_command_duration_seconds_bucket{cmd=%q,le=\"%g\"} %d\n", n, float64(b)/1e6, cum)
		}
		cum += st.Buckets[len(buckets)].Load()
		fmt.Fprintf(w, "cacheboy_command_duration_seconds_bucket{cmd=%q,le=\"+Inf\"} %d\n", n, cum)
		fmt.Fprintf(w, "cacheboy_command_duration_seconds_sum{cmd=%q} %g\n", n, float64(st.Usec.Load())/1e6)
		fmt.Fprintf(w, "cacheboy_command_duration_seconds_count{cmd=%q} %d\n", n, st.Calls.Load())
	}
	fmt.Fprintf(w, "# HELP cacheboy_command_errors_total Commands that returned an error.\n# TYPE cacheboy_command_errors_total counter\n")
	for _, n := range names {
		fmt.Fprintf(w, "cacheboy_command_errors_total{cmd=%q} %d\n", n, m.commands[n].Errors.Load())
	}
}
