// Command cache-boy starts the server.
//
//	cache-boy [config-file] [--option value ...]
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/shamim3696/cache-boy/internal/api/httpapi"
	"github.com/shamim3696/cache-boy/internal/cluster"
	"github.com/shamim3696/cache-boy/internal/command"
	"github.com/shamim3696/cache-boy/internal/config"
	"github.com/shamim3696/cache-boy/internal/dbsync"
	"github.com/shamim3696/cache-boy/internal/persistence"
	"github.com/shamim3696/cache-boy/internal/replication"
	"github.com/shamim3696/cache-boy/internal/server"
)

func main() {
	cfg := config.Default()
	args := os.Args[1:]
	if len(args) == 1 {
		switch args[0] {
		case "-v", "--version", "version":
			fmt.Printf("cache-boy %s (redis protocol %s)\n", command.Version, command.RedisVersion)
			return
		case "-h", "--help", "help":
			fmt.Print(usage)
			return
		}
	}
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		if err := cfg.Load(args[0]); err != nil {
			log.Fatalf("config: %v", err)
		}
		args = args[1:]
	}
	if err := cfg.ApplyArgs(args); err != nil {
		log.Fatalf("config: %v", err)
	}
	cs := config.NewStore(cfg)

	e := command.New(cs)

	// Order matters: persistence loads data first, then replication may
	// start following a leader. Everything is set up before serving.
	pm, err := persistence.Setup(e)
	if err != nil {
		log.Fatalf("persistence: %v", err)
	}
	if _, err := cluster.Setup(e); err != nil {
		log.Fatalf("cluster: %v", err)
	}
	rm := replication.Setup(e)
	rn, err := replication.SetupRaft(e, rm, replication.RaftOptions{})
	if err != nil {
		log.Fatalf("raft: %v", err)
	}
	e.Start()

	srv := server.New(e)
	addr := net.JoinHostPort(cfg.Bind, strconv.Itoa(cfg.Port))
	if err := srv.Listen(addr); err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	var hs *http.Server
	if cfg.HTTPPort > 0 {
		httpAddr := net.JoinHostPort(cfg.Bind, strconv.Itoa(cfg.HTTPPort))
		ln, err := net.Listen("tcp", httpAddr)
		if err != nil {
			log.Fatalf("listen http %s: %v", httpAddr, err)
		}
		hs = &http.Server{Handler: httpapi.New(e), ReadHeaderTimeout: 10 * time.Second}
		go func() {
			if err := hs.Serve(ln); err != nil && err != http.ErrServerClosed {
				log.Printf("http: %v", err)
			}
		}()
		fmt.Printf("HTTP API and dashboard on http://%s/\n", httpAddr)
	}

	stopSync := startDBSync(e, cfg)

	var once sync.Once
	shutdown := func(save bool) error {
		once.Do(func() {
			stopSync()
			if hs != nil {
				hs.Close() // Close, not Shutdown: ends WebSockets and blocked requests now
			}
			srv.Close() // stop taking commands first
			if rn != nil {
				rn.Stop()
			}
			rm.Close()
			if err := pm.Close(save); err != nil {
				log.Printf("shutdown: %v", err)
			}
			e.Stop()
			os.Exit(0)
		})
		return nil
	}
	command.OnShutdown = shutdown

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		log.Printf("shutting down...")
		shutdown(true)
	}()

	fmt.Printf("cache-boy %s listening on %s\n", command.Version, addr)
	if err := srv.Serve(); err != nil {
		log.Fatal(err)
	}
}

const usage = `Usage: cache-boy [config-file] [--option value ...]

  cache-boy                                  listen on 127.0.0.1:6380, dashboard on :6381
  cache-boy cache-boy.conf                   load a config file
  cache-boy --port 7000 --maxmemory 256mb    any config file option works as a flag
  cache-boy --version

Every option is documented in configs/cache-boy.conf.
`

// startDBSync runs automatic invalidation from databases when cache-boy.conf
// has `db-sync <file>` and that file says auto_invalidate: true. It returns a
// function that stops it.
func startDBSync(e *command.Engine, cfg *config.Config) (stop func()) {
	stop = func() {}
	if cfg.DBSync == "" {
		return stop
	}
	logger := log.New(os.Stderr, "db-sync: ", log.LstdFlags)
	sc, err := dbsync.Load(cfg.DBSync)
	if err != nil {
		log.Fatalf("db-sync: %v", err)
	}
	if cfg.ReplicaOf != "" {
		logger.Print("this server is a replica; db-sync runs on the leader and replicas receive its invalidations")
		return stop
	}
	s, err := dbsync.New(sc, &engineInvalidator{e: e, ns: sc.Cache.Namespace}, dbsync.Options{Log: logger, StateDir: cfg.Dir})
	if errors.Is(err, dbsync.ErrDisabled) {
		logger.Print(err)
		return stop
	}
	if err != nil {
		log.Fatalf("db-sync: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.Run(ctx)
	}()
	logger.Printf("automatic cache invalidation on for %d source(s)", len(sc.Sources))
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
		}
	}
}

// engineInvalidator runs TAG.INVALIDATE and DEL inside the server, through
// the normal command path (so they replicate and reach the AOF).
type engineInvalidator struct {
	e  *command.Engine
	ns string

	mu sync.Mutex
	c  *command.Client
}

func (ei *engineInvalidator) Invalidate(_ context.Context, tags, keys []string) error {
	ei.mu.Lock()
	defer ei.mu.Unlock()
	if ei.c == nil {
		ei.c = ei.e.InternalClient("db-sync", false)
		if ei.ns != "" {
			if v := ei.e.Execute(ei.c, []string{"NAMESPACE", "USE", ei.ns}); v.IsError() {
				ei.c = nil
				return errors.New(v.Str)
			}
		}
	}
	for _, cmd := range [][]string{append([]string{"TAG.INVALIDATE"}, tags...), append([]string{"DEL"}, keys...)} {
		if len(cmd) == 1 {
			continue
		}
		if v := ei.e.Execute(ei.c, cmd); v.IsError() {
			return errors.New(v.Str)
		}
	}
	return nil
}
