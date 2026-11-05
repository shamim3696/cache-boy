// Package expiry runs active expiration. Expired keys are also removed lazily
// when touched (store.DB.Get), but keys nobody reads would stay in memory
// forever without this background sweep.
package expiry

import (
	"time"

	"cache-boy/internal/config"
	"cache-boy/internal/store"
)

const samplePerShard = 20

// Run sweeps HZ times per second until stop is closed.
func Run(db *store.DB, cfg *config.Store, stop <-chan struct{}) {
	for {
		hz := cfg.Get().HZ
		if hz <= 0 {
			hz = 10
		}
		select {
		case <-stop:
			return
		case <-time.After(time.Second / time.Duration(hz)):
			db.ActiveExpireCycle(samplePerShard)
		}
	}
}
