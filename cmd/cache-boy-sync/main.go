// Command cache-boy-sync keeps cache-boy in step with PostgreSQL: when a row
// changes, the cache tags and keys built from it are invalidated, so the
// application never has to delete cache entries by hand.
//
//	cache-boy-sync -config cache-boy-sync.yaml
package main

import (
	"context"
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
	uninstall := fs.Bool("uninstall", false, "drop the replication slot and publication, then exit")
	version := fs.Bool("version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprint(os.Stderr, `Usage: cache-boy-sync [-config file] [-v] [-dry-run] [-uninstall]

Streams changes from PostgreSQL (logical replication) and invalidates
cache-boy tags and keys for every changed row. See docs/db-sync.md.

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

	if *uninstall {
		if err := dbsync.Uninstall(ctx, cfg, logger); err != nil {
			logger.Fatal(err)
		}
		return
	}

	var inv dbsync.Invalidator
	if *dryRun {
		inv = dbsync.LogInvalidator{Log: logger}
	} else {
		ci := dbsync.NewCacheInvalidator(cfg.Cache)
		defer ci.Close()
		inv = ci
	}
	s, err := dbsync.New(cfg, inv, logger, *verbose)
	if err != nil {
		logger.Fatal(err)
	}
	logger.Printf("cache-boy-sync %s: postgres → cache-boy at %s", command.Version, cfg.Cache.Addr)
	if err := s.Run(ctx); err != nil {
		logger.Fatal(err)
	}
	logger.Print("stopped")
}
