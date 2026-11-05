// Package glob implements Redis-style glob matching: * ? [abc] [^a] [a-z] and \ escapes.
package glob

// Match reports whether s matches pattern.
func Match(pattern, s string) bool {
	return match(pattern, s, 0)
}

func match(p, s string, depth int) bool {
	if depth > 1000 { // guard against pathological patterns
		return false
	}
	for len(p) > 0 {
		switch p[0] {
		case '*':
			for len(p) > 1 && p[1] == '*' {
				p = p[1:]
			}
			if len(p) == 1 {
				return true
			}
			for i := 0; i <= len(s); i++ {
				if match(p[1:], s[i:], depth+1) {
					return true
				}
			}
			return false
		case '?':
			if len(s) == 0 {
				return false
			}
			s = s[1:]
			p = p[1:]
		case '[':
			if len(s) == 0 {
				return false
			}
			end, ok := matchClass(p, s[0])
			if !ok {
				return false
			}
			p = p[end:]
			s = s[1:]
		case '\\':
			if len(p) >= 2 {
				p = p[1:]
			}
			fallthrough
		default:
			if len(s) == 0 || p[0] != s[0] {
				return false
			}
			s = s[1:]
			p = p[1:]
		}
	}
	return len(s) == 0
}

// matchClass matches c against the [...] class at the start of p and returns
// the length of the class in p.
func matchClass(p string, c byte) (int, bool) {
	i := 1
	negate := false
	if i < len(p) && p[i] == '^' {
		negate = true
		i++
	}
	matched := false
	for i < len(p) && p[i] != ']' {
		if p[i] == '\\' && i+1 < len(p) {
			i++
			if p[i] == c {
				matched = true
			}
			i++
			continue
		}
		if i+2 < len(p) && p[i+1] == '-' && p[i+2] != ']' {
			lo, hi := p[i], p[i+2]
			if lo > hi {
				lo, hi = hi, lo
			}
			if c >= lo && c <= hi {
				matched = true
			}
			i += 3
			continue
		}
		if p[i] == c {
			matched = true
		}
		i++
	}
	if i < len(p) {
		i++ // skip ']'
	}
	return i, matched != negate
}
