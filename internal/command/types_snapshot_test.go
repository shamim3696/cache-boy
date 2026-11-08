package command_test

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/persistence/snapshot"
	"github.com/shamim3696/cache-boy/internal/store"
)

type cmdLog struct {
	mu   sync.Mutex
	cmds [][]string
}

func (l *cmdLog) Propagate(args []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cmds = append(l.cmds, append([]string(nil), args...))
}

// encoded returns the binary encoding of a bloom or HLL key ("" if missing).
func encoded(e *command.Engine, key string) string {
	unlock := e.DB.Lock(key)
	defer unlock()
	en := e.DB.Peek(key)
	switch {
	case en == nil:
		return ""
	case en.Kind == store.KindBloom:
		return string(en.Bloom().Marshal())
	case en.Kind == store.KindHLL:
		return string(en.HLL().Marshal())
	}
	return "kind " + en.Kind.String()
}

func fillProbabilistic(e *command.Engine, c *command.Client) {
	exec(e, c, "BF.RESERVE", "bf", "0.001", "50", "EXPANSION", "3")
	exec(e, c, "PFADD", "small", "a", "b", "c")
	for i := range 2000 {
		s := strconv.Itoa(i)
		exec(e, c, "BF.ADD", "bf", "item:"+s)
		exec(e, c, "BF.MADD", "bfdefault", "x:"+s, "y:"+s)
		exec(e, c, "PFADD", "big", "el:"+s)
		exec(e, c, "TS.ADD", "ts", strconv.Itoa(1_000_000+i*1000), strconv.FormatFloat(float64(i%50)/10, 'f', -1, 64))
	}
	exec(e, c, "TS.ADD", "ts", "1500500", "42") // out of order, into a sealed chunk
	exec(e, c, "PFMERGE", "merged", "small", "big")
}

func TestProbabilisticSnapshotRoundTrip(t *testing.T) {
	e := infraEngine(t, nil)
	c := e.NewClient("tcp", "test")
	fillProbabilistic(e, c)
	snap, _, err := snapshot.Bytes(e)
	if err != nil {
		t.Fatal(err)
	}
	e2 := infraEngine(t, nil)
	if err := snapshot.Load(bytes.NewReader(snap), e2); err != nil {
		t.Fatal(err)
	}
	c2 := e2.NewClient("tcp", "test")
	for _, k := range []string{"bf", "bfdefault", "small", "big", "merged"} {
		if encoded(e, k) != encoded(e2, k) || encoded(e2, k) == "" {
			t.Errorf("%s changed in the snapshot round trip", k)
		}
	}
	for _, cmd := range [][]string{
		{"BF.INFO", "bf"}, {"BF.CARD", "bfdefault"}, {"BF.MEXISTS", "bf", "item:0", "item:1999", "nope"},
		{"PFCOUNT", "big"}, {"PFCOUNT", "merged"}, {"TYPE", "bf"}, {"TYPE", "big"},
		{"TS.RANGE", "ts", "-", "+"}, {"TS.INFO", "ts"},
	} {
		a, b := exec(e, c, cmd...), exec(e2, c2, cmd...)
		if a.IsError() || fmt.Sprint(a) != fmt.Sprint(b) {
			t.Errorf("%s: %v before, %v after", strings.Join(cmd, " "), a, b)
		}
	}
}

// TestProbabilisticReplayIsDeterministic replays the propagated stream on a
// second engine, as a replica or an AOF load does, and expects identical bits.
func TestProbabilisticReplayIsDeterministic(t *testing.T) {
	e := infraEngine(t, nil)
	log := &cmdLog{}
	e.AddPropagator(log)
	fillProbabilistic(e, e.NewClient("tcp", "test"))

	replica := infraEngine(t, nil)
	rc := replica.NewClient("tcp", "replica")
	for _, cmd := range log.cmds {
		if v := exec(replica, rc, cmd...); v.IsError() {
			t.Fatalf("%v: %v", cmd, v)
		}
	}
	for _, k := range []string{"bf", "bfdefault", "small", "big", "merged"} {
		if encoded(e, k) != encoded(replica, k) {
			t.Errorf("%s differs on the replica", k)
		}
	}
}
