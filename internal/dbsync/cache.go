package dbsync

import (
	"context"
	"log"
	"sync"

	"github.com/shamim3696/cache-boy/pkg/client"
)

// CacheInvalidator sends TAG.INVALIDATE and DEL to a cache-boy server. It
// connects lazily, so the sync can start before the cache is up.
type CacheInvalidator struct {
	cfg CacheConfig
	mu  sync.Mutex
	c   *client.Client
}

func NewCacheInvalidator(cfg CacheConfig) *CacheInvalidator { return &CacheInvalidator{cfg: cfg} }

func (ci *CacheInvalidator) conn() (*client.Client, error) {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if ci.c != nil {
		return ci.c, nil
	}
	var opts []client.Option
	if ci.cfg.Password != "" {
		opts = append(opts, client.Password(ci.cfg.Password))
	}
	if ci.cfg.Username != "" {
		opts = append(opts, client.Username(ci.cfg.Username))
	}
	if ci.cfg.Namespace != "" {
		opts = append(opts, client.Namespace(ci.cfg.Namespace))
	}
	c, err := client.Dial(ci.cfg.Addr, opts...)
	if err != nil {
		return nil, err
	}
	ci.c = c
	return c, nil
}

func (ci *CacheInvalidator) Invalidate(ctx context.Context, tags, keys []string) error {
	c, err := ci.conn()
	if err != nil {
		return err
	}
	if len(tags) > 0 {
		if _, err := c.TagInvalidate(ctx, tags...); err != nil {
			return err
		}
	}
	if len(keys) > 0 {
		if _, err := c.Del(ctx, keys...); err != nil {
			return err
		}
	}
	return nil
}

func (ci *CacheInvalidator) Close() error {
	ci.mu.Lock()
	defer ci.mu.Unlock()
	if ci.c == nil {
		return nil
	}
	return ci.c.Close()
}

// LogInvalidator only logs what would be invalidated (--dry-run).
type LogInvalidator struct{ Log *log.Logger }

func (l LogInvalidator) Invalidate(_ context.Context, tags, keys []string) error {
	l.Log.Printf("dry run: would invalidate tags %v and delete keys %v", tags, keys)
	return nil
}
