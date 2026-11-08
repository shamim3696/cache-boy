package store

import (
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func TestParseInt(t *testing.T) {
	ints := []string{"0", "1", "-1", "42", "-42", "9223372036854775807", "-9223372036854775808", "1000000"}
	for _, s := range ints {
		v, ok := parseInt(s)
		if !ok || strconv.FormatInt(v, 10) != s {
			t.Errorf("parseInt(%q) = %d, %v", s, v, ok)
		}
		if _, ok := parseInt([]byte(s)); !ok {
			t.Errorf("parseInt([]byte(%q)) failed", s)
		}
	}
	notInts := []string{"", "-", "+", "007", "00", "-0", "-007", "+5", " 5", "5 ", "1e3", "1.0", "0x10", "abc",
		"9223372036854775808", "-9223372036854775809", "18446744073709551616", "99999999999999999999", "123456789012345678901"}
	for _, s := range notInts {
		if v, ok := parseInt(s); ok {
			t.Errorf("parseInt(%q) = %d, want not an int", s, v)
		}
	}
}

func TestStringEncodings(t *testing.T) {
	cases := []struct{ in, enc string }{
		{"12345", "int"},
		{"-9223372036854775808", "int"},
		{"9223372036854775808", "embstr"},
		{"007", "embstr"},
		{"-0", "embstr"},
		{"+5", "embstr"},
		{"", "embstr"},
		{"hello", "embstr"},
		{strings.Repeat("x", 44), "embstr"},
		{strings.Repeat("x", 45), "raw"},
	}
	for _, c := range cases {
		s := NewString([]byte(c.in), 0)
		if s.Encoding() != c.enc || string(s.Bytes()) != c.in || s.Len() != len(c.in) {
			t.Errorf("%q: encoding %s, bytes %q, len %d", c.in, s.Encoding(), s.Bytes(), s.Len())
		}
		_, isInt := s.Int()
		if isInt != (c.enc == "int") {
			t.Errorf("%q: Int() ok = %v", c.in, isInt)
		}
	}
	if s := NewString(nil, 0); s.Bytes() == nil || s.Len() != 0 {
		t.Fatal("empty string must stay a non-nil raw value")
	}

	s := NewString([]byte("41"), 0)
	if v, ok := s.Int(); !ok || v != 41 || s.MemSize() != 32 {
		t.Fatalf("Int() = %d, %v; MemSize %d", v, ok, s.MemSize())
	}
	s.SetInt(math.MinInt64)
	if string(s.Bytes()) != "-9223372036854775808" || s.Len() != 20 {
		t.Fatalf("after SetInt: %q len %d", s.Bytes(), s.Len())
	}

	big := strings.Repeat("abc", 1000)
	c := NewString([]byte(big), 100)
	if c.Encoding() != "compressed" || !c.Compressed() || string(c.Bytes()) != big || c.Len() != len(big) {
		t.Fatalf("compressed: enc %s len %d", c.Encoding(), c.Len())
	}
	if c.MemSize() >= int64(len(big)) {
		t.Fatalf("compressed MemSize %d not smaller than the value", c.MemSize())
	}
	// Integers win over compression; they are tiny anyway.
	if NewString([]byte("1234567890"), 1).Encoding() != "int" {
		t.Fatal("int not int-encoded with a low compression threshold")
	}
}

func TestHashThresholds(t *testing.T) {
	h := NewHash()
	for i := range hashMaxListpackEntries {
		h.Set("f"+strconv.Itoa(i), "v")
	}
	if h.Encoding() != "listpack" || h.Len() != 128 {
		t.Fatalf("128 fields: %s len %d", h.Encoding(), h.Len())
	}
	h.Set("f0", "updated") // overwriting doesn't add an entry
	if h.Encoding() != "listpack" {
		t.Fatal("overwrite converted")
	}
	h.Set("f128", "v")
	if h.Encoding() != "hashtable" || h.Len() != 129 {
		t.Fatalf("129 fields: %s len %d", h.Encoding(), h.Len())
	}
	if v, _ := h.Get("f0"); v != "updated" {
		t.Fatalf("lost value on conversion: %q", v)
	}
	for i := range 129 {
		h.Del("f" + strconv.Itoa(i))
	}
	h.Set("a", "b")
	if h.Encoding() != "hashtable" {
		t.Fatal("hash converted back to listpack")
	}

	for _, c := range []struct {
		field, value string
		enc          string
	}{
		{"f", strings.Repeat("v", 64), "listpack"},
		{"f", strings.Repeat("v", 65), "hashtable"},
		{strings.Repeat("f", 64), "v", "listpack"},
		{strings.Repeat("f", 65), "v", "hashtable"},
	} {
		h := NewHash()
		h.Set("a", "b")
		h.Set(c.field, c.value)
		if h.Encoding() != c.enc {
			t.Errorf("field %d bytes, value %d bytes: %s, want %s", len(c.field), len(c.value), h.Encoding(), c.enc)
		}
		if v, ok := h.Get(c.field); !ok || v != c.value || h.Len() != 2 {
			t.Errorf("Get after Set: %q %v", v, ok)
		}
	}

	h = NewHash()
	h.Set("a", "short")
	h.Set("a", strings.Repeat("v", 65)) // a long value for an existing field
	if h.Encoding() != "hashtable" || h.Len() != 1 {
		t.Fatalf("long overwrite: %s len %d", h.Encoding(), h.Len())
	}
}

func TestSetThresholds(t *testing.T) {
	s := NewSet()
	for i := range setMaxIntsetEntries {
		s.Add(strconv.Itoa(i * 3))
	}
	if s.Encoding() != "intset" || s.Len() != 512 {
		t.Fatalf("512 ints: %s len %d", s.Encoding(), s.Len())
	}
	s.Add("0") // duplicate
	if s.Encoding() != "intset" {
		t.Fatal("duplicate converted")
	}
	s.Add("1")
	if s.Encoding() != "hashtable" || s.Len() != 513 || !s.Has("1533") {
		t.Fatalf("513 ints: %s len %d", s.Encoding(), s.Len())
	}

	// A non-integer moves a small intset to a listpack, a big one to a map.
	s = NewSet()
	for i := range setMaxListpackEntries - 1 {
		s.Add(strconv.Itoa(i))
	}
	s.Add("x")
	if s.Encoding() != "listpack" || s.Len() != 128 || !s.Has("126") || s.Has("127") || !s.Has("x") {
		t.Fatalf("127 ints + x: %s len %d", s.Encoding(), s.Len())
	}
	s.Add("y")
	if s.Encoding() != "hashtable" || s.Len() != 129 {
		t.Fatalf("listpack 129: %s len %d", s.Encoding(), s.Len())
	}
	s = NewSet()
	for i := range setMaxListpackEntries {
		s.Add(strconv.Itoa(i))
	}
	s.Add("x")
	if s.Encoding() != "hashtable" || s.Len() != 129 {
		t.Fatalf("128 ints + x: %s len %d", s.Encoding(), s.Len())
	}

	for _, c := range []struct{ member, enc string }{
		{"5", "intset"},
		{"-9223372036854775808", "intset"},
		{"007", "listpack"},
		{"-0", "listpack"},
		{"+5", "listpack"},
		{"9223372036854775808", "listpack"},
		{strings.Repeat("m", 64), "listpack"},
		{strings.Repeat("m", 65), "hashtable"},
	} {
		s := NewSet()
		s.Add("1")
		s.Add(c.member)
		if s.Encoding() != c.enc || !s.Has(c.member) || s.Len() != 2 {
			t.Errorf("1 + %q: %s len %d", c.member, s.Encoding(), s.Len())
		}
	}

	s = NewSet()
	s.Add("7")
	if s.Has("007") || s.Has("+7") || s.Remove("07") {
		t.Fatal("non-canonical forms matched an intset member")
	}
}

func TestIntsetWidths(t *testing.T) {
	s := NewSet()
	want := []int64{5, -3, math.MaxInt16, math.MinInt16, 1 << 20, -(1 << 20), math.MaxInt64, math.MinInt64, 0}
	for _, v := range want {
		s.Add(strconv.FormatInt(v, 10))
	}
	if s.Encoding() != "intset" || s.data[0] != 8 || s.Len() != len(want) {
		t.Fatalf("enc %s width %d len %d", s.Encoding(), s.data[0], s.Len())
	}
	slices.Sort(want)
	for i, v := range want {
		if s.intAt(i) != v {
			t.Fatalf("index %d = %d, want %d", i, s.intAt(i), v)
		}
	}
	s = NewSet()
	s.Add("1")
	s.Add("70000")
	if s.data[0] != 4 || !s.Has("1") || !s.Has("70000") || s.Has("2") || s.Has("99999999999") {
		t.Fatalf("width %d", s.data[0])
	}
}

// TestHashModel checks random operations against a plain map, with field
// universes and value lengths that keep some hashes packed and push others
// over the thresholds.
func TestHashModel(t *testing.T) {
	for seed := range uint64(40) {
		r := rand.New(rand.NewPCG(seed, 1))
		universe := []int{4, 50, 140, 300}[seed%4]
		longOdds := []int{0, 200}[seed/4%2] // 0 means never long values
		h, model := NewHash(), map[string]string{}
		for range 3000 {
			f := "field:" + strconv.Itoa(r.IntN(universe))
			switch r.IntN(10) {
			case 0, 1, 2:
				if h.Del(f) != hasKey(model, f) {
					t.Fatalf("seed %d: Del(%q) disagreed", seed, f)
				}
				delete(model, f)
			default:
				n := r.IntN(20)
				if longOdds > 0 && r.IntN(longOdds) == 0 {
					n = 60 + r.IntN(10)
				}
				v := strings.Repeat(string(rune('a'+r.IntN(26))), n)
				if h.Set(f, v) == hasKey(model, f) {
					t.Fatalf("seed %d: Set(%q) new-ness disagreed", seed, f)
				}
				model[f] = v
			}
			if h.Len() != len(model) {
				t.Fatalf("seed %d: Len %d, want %d", seed, h.Len(), len(model))
			}
		}
		for i := range universe {
			f := "field:" + strconv.Itoa(i)
			v, ok := h.Get(f)
			mv, mok := model[f]
			if ok != mok || v != mv {
				t.Fatalf("seed %d: Get(%q) = %q %v, want %q %v", seed, f, v, ok, mv, mok)
			}
		}
		if !maps.Equal(h.Map(), model) {
			t.Fatalf("seed %d (%s): Map differs", seed, h.Encoding())
		}
		got := map[string]string{}
		h.Range(func(f, v string) bool { got[f] = v; return true })
		if !maps.Equal(got, model) {
			t.Fatalf("seed %d (%s): Range differs", seed, h.Encoding())
		}
		if h.MemSize() <= 0 {
			t.Fatalf("seed %d: MemSize %d", seed, h.MemSize())
		}
	}
}

func hasKey(m map[string]string, k string) bool { _, ok := m[k]; return ok }

// TestSetModel checks random operations against a plain map. Each seed picks
// a member mix: integers only (intset), short strings (listpack), or mixed
// with non-canonical integers and long members (conversions).
func TestSetModel(t *testing.T) {
	for seed := range uint64(48) {
		r := rand.New(rand.NewPCG(seed, 2))
		universe := []int{8, 100, 200, 700}[seed%4]
		mix := seed / 4 % 3
		member := func() string {
			n := r.IntN(universe)
			switch {
			case mix == 0 || r.IntN(3) == 0:
				return strconv.Itoa(n*7919 - universe*3000) // includes negatives and wide values
			case mix == 2 && r.IntN(50) == 0:
				return strings.Repeat("L", 60+n%10)
			case mix == 2 && r.IntN(4) == 0:
				return []string{"0", "00", "-0", "+"}[n%4] + strconv.Itoa(n)
			}
			return "m:" + strconv.Itoa(n)
		}
		s, model := NewSet(), map[string]bool{}
		var seen []string
		for range 4000 {
			m := member()
			seen = append(seen, m)
			switch r.IntN(10) {
			case 0, 1, 2:
				if s.Remove(m) != model[m] {
					t.Fatalf("seed %d: Remove(%q) disagreed", seed, m)
				}
				delete(model, m)
			case 3:
				if s.Has(m) != model[m] {
					t.Fatalf("seed %d (%s): Has(%q) disagreed", seed, s.Encoding(), m)
				}
			default:
				if s.Add(m) == model[m] {
					t.Fatalf("seed %d (%s): Add(%q) new-ness disagreed", seed, s.Encoding(), m)
				}
				model[m] = true
			}
			if s.Len() != len(model) {
				t.Fatalf("seed %d: Len %d, want %d", seed, s.Len(), len(model))
			}
			if s.Len() > 0 {
				if m, ok := s.Random(); !ok || !model[m] {
					t.Fatalf("seed %d: Random() = %q, %v", seed, m, ok)
				}
			}
		}
		for _, m := range seen {
			if s.Has(m) != model[m] {
				t.Fatalf("seed %d (%s): Has(%q) disagreed", seed, s.Encoding(), m)
			}
		}
		members := s.Members()
		slices.Sort(members)
		if want := slices.Sorted(maps.Keys(model)); !slices.Equal(members, want) {
			t.Fatalf("seed %d (%s): Members differ", seed, s.Encoding())
		}
		n := 0
		s.Range(func(m string) bool { n++; return model[m] })
		if n != len(model) {
			t.Fatalf("seed %d: Range visited %d of %d", seed, n, len(model))
		}
	}
}

func TestRangeStopsEarly(t *testing.T) {
	h := NewHash()
	s := NewSet()
	for i := range 5 {
		h.Set(strconv.Itoa(i), "v")
		s.Add(strconv.Itoa(i))
	}
	s2 := NewSet()
	s2.Add("a")
	s2.Add("b")
	n := 0
	h.Range(func(string, string) bool { n++; return false })
	s.Range(func(string) bool { n++; return false })
	s2.Range(func(string) bool { n++; return false })
	if n != 3 {
		t.Fatalf("Range ignored false: %d calls", n)
	}
}

func TestCompactMemSize(t *testing.T) {
	h := NewHash()
	for i := range 5 {
		h.Set("field"+strconv.Itoa(i), "value-"+strconv.Itoa(i))
	}
	s := NewSet()
	for i := range 10 {
		s.Add(strconv.Itoa(i))
	}
	if h.MemSize() > 160 || s.MemSize() > 64 {
		t.Fatalf("hash %d bytes, intset %d bytes", h.MemSize(), s.MemSize())
	}
}
