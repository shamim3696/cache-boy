// Command cache-boy-sync keeps cache-boy in step with your databases
// (PostgreSQL, MySQL, MongoDB): when a row changes, the cache tags and keys
// built from it are invalidated, so the application never has to delete
// cache entries by hand.
//
//	cache-boy-sync -config cache-boy-sync.yaml
//
// The same config can instead be loaded by the server itself with
// `db-sync <file>` in cache-boy.conf; then this command isn't needed.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/dbsync"
)

func main() {
	fs := flag.NewFlagSet("cache-boy-sync", flag.ExitOnError)
	configPath := fs.String("config", "cache-boy-sync.yaml", "config file")
	verbose := fs.Bool("v", false, "log every invalidation")
	dryRun := fs.Bool("dry-run", false, "log what would be invalidated without touching the cache")
	uninstall := fs.Bool("uninstall", false, "drop the PostgreSQL slot and publication and the state files, then exit")
	stateDir := fs.String("state-dir", "", "directory for MySQL/MongoDB state files (default: current directory)")
	version := fs.Bool("version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: cache-boy-sync [-config file] [-v] [-dry-run] [-uninstall]

Streams changes from PostgreSQL, MySQL or MongoDB and invalidates cache-boy
tags and keys for every changed row. Off unless the config has
auto_invalidate: true. See docs/db-sync.md.

`)
		fs.PrintDefaults()
	}
	fs.Parse(os.Args[1:])

	if *version {
		fmt.Printf("cache-boy-sync %s\n", command.Version)
		return
	}
	logger := log.New(os.Stderr, "cache-boy-sync: ", log.LstdFlags)

	cfg, err := dbsync.Load(*configPath)
	if err != nil {
		logger.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	opts := dbsync.Options{Log: logger, Verbose: *verbose, DryRun: *dryRun, StateDir: *stateDir}
	if *uninstall {
		if err := dbsync.Uninstall(ctx, cfg, opts); err != nil {
			logger.Fatal(err)
		}
		return
	}

	ci := dbsync.NewCacheInvalidator(cfg.Cache)
	defer ci.Close()
	s, err := dbsync.New(cfg, ci, opts)
	if errors.Is(err, dbsync.ErrDisabled) {
		logger.Print(err)
		return
	}
	if err != nil {
		logger.Fatal(err)
	}
	logger.Printf("cache-boy-sync %s: %d source(s) → cache-boy at %s", command.Version, len(cfg.Sources), cfg.Cache.Addr)
	if err := s.Run(ctx); err != nil {
		logger.Fatal(err)
	}
	logger.Print("stopped")
}
