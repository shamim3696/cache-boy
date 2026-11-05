package glob

import "testing"

func TestMatch(t *testing.T) {
	cases := []struct {
		p, s string
		want bool
	}{
		{"*", "", true}, {"*", "anything", true}, {"h?llo", "hello", true}, {"h?llo", "hllo", false},
		{"h*llo", "heeeello", true}, {"h[ae]llo", "hallo", true}, {"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true}, {"h[^e]llo", "hello", false}, {"h[a-c]llo", "hbllo", true},
		{`h\*llo`, "h*llo", true}, {`h\*llo`, "hello", false}, {"user:*:name", "user:42:name", true},
		{"a*b*c", "abxbc", true}, {"a*b*c", "acb", false}, {"", "", true}, {"", "a", false},
	}
	for _, c := range cases {
		if got := Match(c.p, c.s); got != c.want {
			t.Errorf("Match(%q, %q) = %v, want %v", c.p, c.s, got, c.want)
		}
	}
}
