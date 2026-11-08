package store

import (
	"strconv"
	"testing"
)

var benchFields = func() []string {
	out := make([]string, 8)
	for i := range out {
		out[i] = "field" + strconv.Itoa(i)
	}
	return out
}()

func BenchmarkHashSmallSet(b *testing.B) {
	h := NewHash()
	for i := 0; b.Loop(); i++ {
		h.Set(benchFields[i%5], "value")
	}
}

func BenchmarkHashSmallGet(b *testing.B) {
	h := NewHash()
	for i := range 5 {
		h.Set(benchFields[i], "value-"+strconv.Itoa(i))
	}
	for i := 0; b.Loop(); i++ {
		h.Get(benchFields[i%8])
	}
}

func BenchmarkSetIntAddHas(b *testing.B) {
	members := make([]string, 20)
	for i := range members {
		members[i] = strconv.Itoa(i * 37)
	}
	s := NewSet()
	for i := 0; b.Loop(); i++ {
		s.Add(members[i%10])
		s.Has(members[i%20])
	}
}

func BenchmarkSetStrAddHas(b *testing.B) {
	members := make([]string, 20)
	for i := range members {
		members[i] = "tag:" + strconv.Itoa(i)
	}
	s := NewSet()
	for i := 0; b.Loop(); i++ {
		s.Add(members[i%10])
		s.Has(members[i%20])
	}
}
