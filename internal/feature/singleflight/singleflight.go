// Package singleflight: STUB, replaced by the real implementation.
package singleflight

import "cache-boy/internal/store"

type Group struct{}

func New(db *store.DB) *Group { return &Group{} }
