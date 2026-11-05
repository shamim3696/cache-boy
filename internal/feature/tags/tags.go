// Package tags: STUB, replaced by the real implementation.
package tags

import "cache-boy/internal/store"

type Index struct{}

func New(db *store.DB) *Index { return &Index{} }

// Tag attaches tags to key and returns how many were new.
func (x *Index) Tag(key string, tags ...string) int { return 0 }
