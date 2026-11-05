package observability

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cache-boy/internal/protocol/resp"
)

// Subscriber receives MONITOR lines. Push must not block.
type Subscriber interface {
	Push(v resp.Value) bool
	ClientID() int64
}

// Monitor streams every executed command to MONITOR clients, in the same
// format as Redis: 1700000000.123456 [0 127.0.0.1:5000] "SET" "a" "1"
type Monitor struct {
	mu   sync.RWMutex
	subs map[int64]Subscriber
}

func NewMonitor() *Monitor { return &Monitor{subs: map[int64]Subscriber{}} }

func (m *Monitor) Add(s Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subs[s.ClientID()] = s
}

func (m *Monitor) Remove(s Subscriber) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.subs, s.ClientID())
}

// Active reports whether anyone is monitoring (cheap check before formatting).
func (m *Monitor) Active() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.subs) > 0
}

// Feed sends one command to all monitors, except the client that ran it.
func (m *Monitor) Feed(fromID int64, addr string, args []string) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.subs) == 0 {
		return
	}
	now := time.Now()
	var b strings.Builder
	fmt.Fprintf(&b, "%d.%06d [0 %s]", now.Unix(), now.Nanosecond()/1000, addr)
	for _, a := range args {
		b.WriteByte(' ')
		b.WriteString(strconv.Quote(a))
	}
	line := resp.Simple(b.String())
	for id, s := range m.subs {
		if id != fromID {
			s.Push(line)
		}
	}
}
