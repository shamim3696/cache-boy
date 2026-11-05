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
	"syscall"

	"cache-boy/internal/command"
	"cache-boy/internal/config"
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
	e.Start()

	srv := server.New(e)
	addr := net.JoinHostPort(cfg.Bind, strconv.Itoa(cfg.Port))
	if err := srv.Listen(addr); err != nil {
		log.Fatalf("listen %s: %v", addr, err)
	}

	shutdown := func(save bool) error {
		srv.Close()
		e.Stop()
		os.Exit(0)
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
