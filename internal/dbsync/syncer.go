package dbsync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Invalidator removes cache entries. It must be safe to call again with the
// same input: after a crash, the last changes are replayed.
type Invalidator interface {
	Invalidate(ctx context.Context, tags, keys []string) error
}

// source is one database change stream. session connects, streams until an
// error or ctx is done, and returns; the Syncer reconnects.
type source interface {
	name() string
	session(ctx context.Context, sink *sink) error
}

// ErrDisabled is returned by New when the config has auto_invalidate off.
var ErrDisabled = errors.New("auto invalidation is off: set auto_invalidate: true in the db-sync config to enable it; until then invalidate the cache from your application")

// Options tune a Syncer.
type Options struct {
	Log      *log.Logger
	Verbose  bool   // log every invalidation
	DryRun   bool   // log instead of invalidating (the Invalidator is not called)
	StateDir string // where MySQL/MongoDB state files go when state_file is relative or unset
}

// Syncer runs every configured source and invalidates the cache.
//
// Delivery is at least once: a change is acknowledged to its database (or its
// position saved) only after the invalidation succeeded, so when cache-boy is
// down the stream waits and nothing is skipped.
type Syncer struct {
	cfg     *Config
	sources []source
	sink    *sink
	log     *log.Logger
}

func New(cfg *Config, inv Invalidator, opts Options) (*Syncer, error) {
	if !cfg.AutoInvalidate {
		return nil, ErrDisabled
	}
	if opts.Log == nil {
		opts.Log = log.Default()
	}
	s := &Syncer{cfg: cfg, log: opts.Log}
	if opts.DryRun {
		inv = LogInvalidator{Log: opts.Log}
	}
	s.sink = &sink{inv: inv, log: opts.Log, verbose: opts.Verbose}
	for i := range cfg.Sources {
		sc := &cfg.Sources[i]
		if sc.StateFile == "" {
			sc.StateFile = sc.Name + ".cbsync-state"
		}
		if !filepath.IsAbs(sc.StateFile) && opts.StateDir != "" {
			sc.StateFile = filepath.Join(opts.StateDir, sc.StateFile)
		}
		src, err := newSource(sc, opts.Log)
		if err != nil {
			return nil, err
		}
		s.sources = append(s.sources, src)
	}
	return s, nil
}

func newSource(sc *SourceConfig, logger *log.Logger) (source, error) {
	switch sc.Type {
	case TypePostgres:
		return newPostgres(sc, logger)
	case TypeMySQL:
		return newMySQL(sc, logger)
	case TypeMongoDB:
		return newMongo(sc, logger)
	}
	return nil, fmt.Errorf("unknown source type %q", sc.Type)
}

const maxBackoff = 30 * time.Second

// Run streams every source until ctx is cancelled, reconnecting each one
// after errors.
func (s *Syncer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for _, src := range s.sources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.runSource(ctx, src)
		}()
	}
	go s.report(ctx)
	wg.Wait()
	return nil
}

func (s *Syncer) runSource(ctx context.Context, src source) {
	backoff := time.Second
	for {
		started := time.Now()
		err := src.session(ctx, s.sink)
		if ctx.Err() != nil {
			return
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		s.log.Printf("%s: %v; reconnecting in %s", src.name(), err, backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (s *Syncer) report(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	var last int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n := s.sink.batches.Load()
			if n != last {
				s.log.Printf("so far: %d batches, %d tags and %d keys invalidated", n, s.sink.tags.Load(), s.sink.keys.Load())
				last = n
			}
		}
	}
}

// sink sends batches to the Invalidator with retries. It is shared by all
// sources.
type sink struct {
	inv     Invalidator
	log     *log.Logger
	verbose bool

	batches, tags, keys atomic.Int64
}

const chunkSize = 500 // tags or keys per command

// flush invalidates b, retrying until it succeeds or ctx is done. keepalive,
// if set, is called at least every 10 s while waiting, so the database
// connection doesn't time out.
func (k *sink) flush(ctx context.Context, src string, b *batch, keepalive func() error) error {
	if len(b.tags)+len(b.keys) == 0 {
		return nil
	}
	sort.Strings(b.tags)
	sort.Strings(b.keys)
	backoff := 500 * time.Millisecond
	for {
		err := k.send(ctx, b)
		if err == nil {
			k.batches.Add(1)
			k.tags.Add(int64(len(b.tags)))
			k.keys.Add(int64(len(b.keys)))
			if k.verbose {
				k.log.Printf("%s: %d changes → tags %v keys %v", src, b.changes, b.tags, b.keys)
			}
			return nil
		}
		k.log.Printf("%s: cache: %v; retrying in %s (the database keeps the changes until then)", src, err, backoff)
		deadline := time.Now().Add(backoff)
		for time.Now().Before(deadline) {
			if keepalive != nil {
				if err := keepalive(); err != nil {
					return err
				}
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(min(time.Until(deadline), 10*time.Second)):
			}
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

func (k *sink) send(ctx context.Context, b *batch) error {
	for i := 0; i < len(b.tags) || i < len(b.keys); i += chunkSize {
		var tags, keys []string
		if i < len(b.tags) {
			tags = b.tags[i:min(i+chunkSize, len(b.tags))]
		}
		if i < len(b.keys) {
			keys = b.keys[i:min(i+chunkSize, len(b.keys))]
		}
		if err := k.inv.Invalidate(ctx, tags, keys); err != nil {
			return err
		}
	}
	return nil
}

// Uninstall removes what sources created in their databases (the PostgreSQL
// slot and publication) and deletes MySQL/MongoDB state files. A PostgreSQL
// slot that is no longer read makes Postgres keep WAL forever.
func Uninstall(ctx context.Context, cfg *Config, opts Options) error {
	if opts.Log == nil {
		opts.Log = log.Default()
	}
	for i := range cfg.Sources {
		sc := &cfg.Sources[i]
		if sc.StateFile == "" {
			sc.StateFile = sc.Name + ".cbsync-state"
		}
		if !filepath.IsAbs(sc.StateFile) && opts.StateDir != "" {
			sc.StateFile = filepath.Join(opts.StateDir, sc.StateFile)
		}
		var err error
		switch sc.Type {
		case TypePostgres:
			err = uninstallPostgres(ctx, sc, opts.Log)
		default:
			err = removeState(sc.StateFile, opts.Log)
		}
		if err != nil {
			return fmt.Errorf("%s: %w", sc.Name, err)
		}
	}
	return nil
}
