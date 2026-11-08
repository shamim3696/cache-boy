// Package expiry runs active expiration. Expired keys are also removed lazily
// when touched (store.DB.Get), but keys nobody reads would stay in memory
// forever without this background sweep.
package expiry

import (
	"time"

	"cache-boy/internal/config"
	"cache-boy/internal/store"
)

// budgetPerShard caps the wheel entries checked per shard per sweep, so a
// mass expiry is spread over several sweeps instead of pausing the server.
// At the default 10 Hz and 64 shards that is 1.28M keys per second, and up
// to 10× that while Run is catching up.
const budgetPerShard = 2000

// Run sweeps HZ times per second until stop is closed. While a sweep leaves
// due keys behind it sweeps again 10× sooner, which still leaves the shards
// unlocked most of the time but frees a mass expiry within a few ticks.
func Run(db *store.DB, cfg *config.Store, stop <-chan struct{}) {
	more := false
	for {
		hz := cfg.Get().HZ
		if hz <= 0 {
			hz = 10
		}
		wait := time.Second / time.Duration(hz)
		if more {
			wait /= 10
		}
		select {
		case <-stop:
			return
		case <-time.After(wait):
			_, more = db.ActiveExpireCycle(budgetPerShard)
		}
	}
}
