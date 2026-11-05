// Package server accepts TCP connections and runs one goroutine per client.
// Commands are parsed and executed on that goroutine; a second goroutine per
// client writes replies and push messages (see conn.go).
package server

import (
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"cache-boy/internal/command"
)

type Server struct {
	e  *command.Engine
	ln net.Listener
	wg sync.WaitGroup

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

func New(e *command.Engine) *Server {
	return &Server{e: e, conns: map[net.Conn]struct{}{}}
}

// Listen binds the address; call Serve afterwards.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.ln = ln
	return nil
}

// Addr returns the bound address (useful when listening on port 0 in tests).
func (s *Server) Addr() net.Addr { return s.ln.Addr() }

// Serve accepts connections until Close is called.
func (s *Server) Serve() error {
	var backoff time.Duration
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			if s.isClosed() {
				return nil
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				backoff = min(max(backoff*2, 5*time.Millisecond), time.Second)
				time.Sleep(backoff)
				continue
			}
			return err
		}
		backoff = 0
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
			tc.SetKeepAlive(true)
		}
		if !s.track(conn) {
			conn.Close()
			return nil
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			s.handle(conn)
		}()
	}
}

// Close stops accepting and disconnects every client.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	if s.ln != nil {
		s.ln.Close()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		log.Printf("server: some connections did not close in time")
	}
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) track(c net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[c] = struct{}{}
	return true
}

func (s *Server) untrack(c net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

func (s *Server) numConns() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}
