// Package history: STUB, replaced by the real implementation.
package history

import (
	"cache-boy/internal/config"
	"cache-boy/internal/store"
)

type Store struct{}

func New(db *store.DB, cfg *config.Store) *Store { return &Store{} }
