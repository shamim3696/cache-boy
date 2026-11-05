// Package namespace isolates keys of different apps/tenants sharing one
// server. A connection in namespace "shop" that writes key "cart:1" really
// stores "\x00shop\x00cart:1". The dispatcher adds the prefix to every key
// argument, so command handlers don't need to know about namespaces.
package namespace

import "strings"

const sep = "\x00"

// Prefix returns the storage prefix for ns ("" for the default namespace).
func Prefix(ns string) string {
	if ns == "" {
		return ""
	}
	return sep + ns + sep
}

// Strip removes ns's prefix from a stored key. ok is false if the key is
// not in ns. In the default namespace, keys of other namespaces are hidden.
func Strip(ns, key string) (string, bool) {
	if ns == "" {
		return key, !strings.HasPrefix(key, sep)
	}
	p := Prefix(ns)
	if !strings.HasPrefix(key, p) {
		return "", false
	}
	return key[len(p):], true
}

// Of returns the namespace a stored key belongs to ("" = default).
func Of(key string) string {
	if !strings.HasPrefix(key, sep) {
		return ""
	}
	end := strings.Index(key[1:], sep)
	if end < 0 {
		return ""
	}
	return key[1 : end+1]
}

// Valid reports whether name can be used as a namespace.
func Valid(name string) bool {
	return name != "" && len(name) <= 64 && !strings.Contains(name, sep)
}
