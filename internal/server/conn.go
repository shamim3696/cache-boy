package server

import (
	"errors"
	"io"
	"net"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/protocol/resp"
)

// handle runs one client connection until it closes.
func (s *Server) handle(conn net.Conn) {
	cfg := s.e.Cfg.Get()
	if cfg.MaxClients > 0 && s.numConns() > cfg.MaxClients {
		s.e.Metrics.RejectedConns.Add(1)
		conn.Write([]byte("-ERR max number of clients reached\r\n"))
		conn.Close()
		return
	}

	c := s.e.NewClient("tcp", conn.RemoteAddr().String())
	c.SetCloser(func() { conn.Close() })
	defer s.e.RemoveClient(c)

	cw := &countingConn{Conn: conn, s: s}
	stopWriter := make(chan struct{})
	writerDone := make(chan struct{})
	go s.writeLoop(c, cw, stopWriter, writerDone)

	// finishWriting flushes everything queued so far and stops the writer.
	finishWriting := func() {
		close(stopWriter)
		<-writerDone
	}

	r := resp.NewReader(cw)
	for {
		if t := s.e.Cfg.Get().Timeout; t > 0 && s.e.PubSub.Count(c) == 0 {
			conn.SetReadDeadline(time.Now().Add(time.Duration(t) * time.Second))
		}
		args, err := r.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				c.Reply(resp.Err("ERR " + pe.Error()))
				finishWriting()
			}
			return
		}
		c.Reply(s.e.Execute(c, args))

		switch {
		case c.CloseAfterReply:
			finishWriting()
			return
		case c.Hijack != nil:
			finishWriting()
			conn.SetReadDeadline(time.Time{})
			c.Hijack(conn, r)
			return
		case c.Closed():
			return
		}
	}
}

// writeLoop writes replies and pushes. Several queued values (pipelining,
// bursts of pub/sub messages) are written with a single flush.
func (s *Server) writeLoop(c *command.Client, conn io.Writer, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	w := resp.NewWriter(conn, c.Proto())
	out := c.Out()
	write := func(v resp.Value) bool {
		w.Proto = c.Proto()
		return w.Write(v) == nil
	}
	for {
		select {
		case v := <-out:
			if !write(v) {
				c.Close()
				return
			}
			for len(out) > 0 {
				if !write(<-out) {
					c.Close()
					return
				}
			}
			if w.Flush() != nil {
				c.Close()
				return
			}
		case <-stop:
			for len(out) > 0 {
				write(<-out)
			}
			w.Flush()
			return
		case <-c.Done():
			return
		}
	}
}

// countingConn tracks network bytes for INFO stats.
type countingConn struct {
	net.Conn
	s *Server
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.s.e.Metrics.NetInputBytes.Add(int64(n))
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.s.e.Metrics.NetOutputBytes.Add(int64(n))
	return n, err
}
