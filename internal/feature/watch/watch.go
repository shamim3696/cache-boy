// Package watch: STUB, replaced by the real implementation.
package watch

import (
	"cache-boy/internal/pubsub"
	"cache-boy/internal/store"
)

type Hub struct{}

func New(db *store.DB, ps *pubsub.Hub) *Hub { return &Hub{} }

func (h *Hub) Close() {}
