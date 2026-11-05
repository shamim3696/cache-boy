// Command cache-boy starts the server.
//
//	cache-boy [config-file] [--option value ...]
package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

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

	var once sync.Once
	shutdown := func(save bool) error {
		once.Do(func() {
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
