// Command cache-boy starts the server.
//
//	cache-boy [config-file] [--option value ...]
package main

import (
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

	"cache-boy/internal/api/httpapi"
	"cache-boy/internal/cluster"
	"cache-boy/internal/command"
	"cache-boy/internal/config"
	"cache-boy/internal/persistence"
	"cache-boy/internal/replication"
	"cache-boy/internal/server"
)

func main() {
	cfg := config.Default()
	args := os.Args[1:]
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

	var once sync.Once
	shutdown := func(save bool) error {
		once.Do(func() {
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
