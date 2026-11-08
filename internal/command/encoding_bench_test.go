package command

import (
	"strconv"
	"testing"

	"cache-boy/internal/config"
)

func newBenchEngine(b *testing.B) (*Engine, *Client) {
	e := New(config.NewStore(config.Default()))
	b.Cleanup(e.Stop)
	return e, e.NewClient("tcp", "bench")
}

func BenchmarkCmdHSetSmall(b *testing.B) {
	e, c := newBenchEngine(b)
	for i := 0; b.Loop(); i++ {
		e.Execute(c, []string{"HSET", "h" + strconv.Itoa(i%1000), "field" + strconv.Itoa(i%5), "value"})
	}
}

func BenchmarkCmdHGetSmall(b *testing.B) {
	e, c := newBenchEngine(b)
	for i := range 1000 {
		for f := range 5 {
			e.Execute(c, []string{"HSET", "h" + strconv.Itoa(i), "field" + strconv.Itoa(f), "value"})
		}
	}
	args := []string{"HGET", "h1", "field3"}
	for b.Loop() {
		e.Execute(c, args)
	}
}

func BenchmarkCmdHGetAllSmall(b *testing.B) {
	e, c := newBenchEngine(b)
	for f := range 5 {
		e.Execute(c, []string{"HSET", "h", "field" + strconv.Itoa(f), "value"})
	}
	args := []string{"HGETALL", "h"}
	for b.Loop() {
		e.Execute(c, args)
	}
}

func BenchmarkCmdSAddInts(b *testing.B) {
	e, c := newBenchEngine(b)
	for i := 0; b.Loop(); i++ {
		e.Execute(c, []string{"SADD", "s" + strconv.Itoa(i%1000), strconv.Itoa(i % 10)})
	}
}

func BenchmarkCmdSIsMemberInts(b *testing.B) {
	e, c := newBenchEngine(b)
	for i := range 10 {
		e.Execute(c, []string{"SADD", "s", strconv.Itoa(i * 7)})
	}
	args := []string{"SISMEMBER", "s", "42"}
	for b.Loop() {
		e.Execute(c, args)
	}
}

func BenchmarkCmdSIsMemberStrings(b *testing.B) {
	e, c := newBenchEngine(b)
	for i := range 10 {
		e.Execute(c, []string{"SADD", "s", "tag:" + strconv.Itoa(i)})
	}
	args := []string{"SISMEMBER", "s", "tag:7"}
	for b.Loop() {
		e.Execute(c, args)
	}
}

func BenchmarkCmdIncr(b *testing.B) {
	e, c := newBenchEngine(b)
	args := []string{"INCR", "counter"}
	for b.Loop() {
		e.Execute(c, args)
	}
}

func BenchmarkCmdGetInt(b *testing.B) {
	e, c := newBenchEngine(b)
	e.Execute(c, []string{"SET", "n", "123456"})
	args := []string{"GET", "n"}
	for b.Loop() {
		e.Execute(c, args)
	}
}
