// Package aof implements the append-only file: every write command is
// appended in RESP form (the same bytes a client sends), and replaying the
// file through the dispatcher rebuilds the dataset.
//
// # Write path
//
// AOF is a command.Propagator. Propagate runs under the shard locks of the
// command, so it only appends to an in-memory buffer (cheap) and wakes a
// background flusher goroutine, which writes whole batches to the file (group
// commit). The fsync policy decides when data reaches the disk:
//
//	always   Propagate itself writes and fsyncs before returning, so the client
//	         only gets its reply once the command is durable. Slowest, safest.
//	everysec the flusher fsyncs once per second: at most ~1s of writes lost on
//	         power failure (a process crash loses nothing already written).
//	no       never fsync; the OS flushes when it wants to.
//
// # File format
//
// A sequence of RESP arrays. After a rewrite the file starts with a binary
// snapshot (see package snapshot; detected by its "CACHEBOY" magic) followed
// by the commands that ran while the rewrite was in progress and after it,
// like Redis' aof-use-rdb-preamble.
//
// # Loading
//
// A crash can leave a half-written command at the end. Load replays every
// complete command, logs a warning and truncates the file at the end of the
// last complete one. Damage elsewhere (a protocol error in the middle) stops
// loading with an error that names the byte offset.
package aof

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/persistence/snapshot"
	"cache-boy/internal/protocol/resp"
)

// Fsync policies.
const (
	FsyncAlways   = "always"
	FsyncEverySec = "everysec"
	FsyncNo       = "no"
)

// AOF is an open append-only file.
type AOF struct {
	path  string
	fsync atomic.Pointer[string]

	// mu guards the pending buffer and the rewrite capture buffer. Held only
	// for appends and buffer swaps, never during I/O (except the final step
	// of a rewrite).
	mu        sync.Mutex
	buf       []byte
	capturing bool
	capture   []byte
	closed    bool

	// fileMu serialises I/O on f and the file swap at the end of a rewrite.
	fileMu   sync.Mutex
	f        *os.File
	needSync bool // data written since the last fsync

	size      atomic.Int64 // current file size (bytes written so far)
	baseSize  atomic.Int64 // size after the last rewrite / at open
	writeErr  atomic.Pointer[error]
	rewriting atomic.Bool
	rewriteOK atomic.Bool
	lastFsync atomic.Int64 // unix ms

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// Open opens (or creates) the AOF at path for appending and starts the
// background flusher. fsync is "always", "everysec" or "no".
func Open(path, fsync string) (*AOF, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	a := &AOF{
		path: path,
		f:    f,
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	a.SetFsync(fsync)
	a.size.Store(st.Size())
	a.baseSize.Store(st.Size())
	a.rewriteOK.Store(true)
	go a.flusher()
	return a, nil
}

// SetFsync changes the fsync policy (CONFIG SET appendfsync).
func (a *AOF) SetFsync(policy string) {
	switch policy {
	case FsyncAlways, FsyncNo:
	default:
		policy = FsyncEverySec
	}
	a.fsync.Store(&policy)
}

func (a *AOF) policy() string { return *a.fsync.Load() }

// Path returns the file name.
func (a *AOF) Path() string { return a.path }

// Size is the current file size in bytes (including data still buffered).
func (a *AOF) Size() int64 { return a.size.Load() + int64(a.pending()) }

// BaseSize is the size right after the last rewrite (or at startup); auto
// rewrite compares Size against it.
func (a *AOF) BaseSize() int64 { return a.baseSize.Load() }

// Rewriting reports whether a rewrite is running.
func (a *AOF) Rewriting() bool { return a.rewriting.Load() }

// LastRewriteOK reports whether the last rewrite succeeded.
func (a *AOF) LastRewriteOK() bool { return a.rewriteOK.Load() }

// LastWriteErr returns the error of the last failed write/fsync, or nil if
// the last write succeeded.
func (a *AOF) LastWriteErr() error {
	if p := a.writeErr.Load(); p != nil {
		return *p
	}
	return nil
}

func (a *AOF) pending() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.buf)
}

// Propagate appends a command. It implements command.Propagator.
func (a *AOF) Propagate(args []string) {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return
	}
	start := len(a.buf)
	a.buf = resp.AppendCommand(a.buf, args)
	if a.capturing {
		a.capture = append(a.capture, a.buf[start:]...)
	}
	a.mu.Unlock()

	if a.policy() == FsyncAlways {
		if err := a.Flush(true); err != nil {
			log.Printf("aof: write failed: %v", err)
		}
		return
	}
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// Flush writes buffered commands to the file, and fsyncs if sync is true.
func (a *AOF) Flush(sync bool) error {
	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	return a.flushLocked(sync)
}

// flushLocked requires fileMu.
func (a *AOF) flushLocked(sync bool) error {
	a.mu.Lock()
	b := a.buf
	a.buf = nil
	a.mu.Unlock()

	if len(b) > 0 {
		n, err := a.f.Write(b)
		a.size.Add(int64(n))
		if err != nil {
			// Keep what wasn't written; it is retried on the next flush.
			a.mu.Lock()
			a.buf = append(b[n:len(b):len(b)], a.buf...)
			a.mu.Unlock()
			a.setErr(err)
			return err
		}
		a.needSync = true
	}
	if sync && a.needSync {
		if err := a.f.Sync(); err != nil {
			a.setErr(err)
			return err
		}
		a.needSync = false
		a.lastFsync.Store(time.Now().UnixMilli())
	}
	a.writeErr.Store(nil)
	return nil
}

func (a *AOF) setErr(err error) { a.writeErr.Store(&err) }

// flusher writes batches as soon as they are queued and applies the
// everysec policy.
func (a *AOF) flusher() {
	defer close(a.done)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-a.wake:
			if err := a.Flush(false); err != nil {
				log.Printf("aof: write failed: %v", err)
			}
		case <-tick.C:
			if err := a.Flush(a.policy() == FsyncEverySec); err != nil {
				log.Printf("aof: write failed: %v", err)
			}
		case <-a.stop:
			return
		}
	}
}

// Close flushes, fsyncs and closes the file. Commands propagated afterwards
// are dropped.
func (a *AOF) Close() error {
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return nil
	}
	a.closed = true
	a.mu.Unlock()
	close(a.stop)
	<-a.done

	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	err := a.flushLocked(true)
	if cerr := a.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// --- loading ---

// countingReader counts bytes read from the file, so the loader knows the
// offset of the last complete command (bytes read minus bytes still buffered).
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// LoadResult describes what Load did.
type LoadResult struct {
	Preamble  bool  // the file started with a snapshot
	Keys      int   // keys loaded from the preamble
	Commands  int   // commands replayed
	Errors    int   // commands that returned an error
	Truncated int64 // bytes cut from the end (0 = file was intact)
}

// Load replays the AOF at path into e. The caller should wrap it in
// e.SetLoading(true/false). A truncated last command is logged and cut off;
// any other damage returns an error.
func Load(path string, e *command.Engine) (LoadResult, error) {
	var res LoadResult
	f, err := os.Open(path)
	if err != nil {
		return res, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return res, err
	}
	size := st.Size()
	cr := &countingReader{r: f}
	// br must be smaller than resp.Reader's own 32KB buffer: bufio.NewReaderSize
	// returns its argument unchanged when that is already a big enough
	// *bufio.Reader, and pos() below assumes two distinct buffers.
	br := bufio.NewReaderSize(cr, 16<<10)

	if head, _ := br.Peek(len(snapshot.Magic)); string(head) == snapshot.Magic {
		data, err := snapshot.Decode(br, int(e.DB.CompressThreshold.Load()))
		if err != nil {
			f.Close()
			return res, fmt.Errorf("aof %s: snapshot preamble: %w", path, err)
		}
		res.Preamble, res.Keys = true, len(data.Keys)
		if err := data.Apply(e, false); err != nil {
			log.Printf("aof: %v", err)
		}
	}

	rr := resp.NewReader(br)
	pos := func() int64 { return cr.n - int64(br.Buffered()) - int64(rr.Buffered()) }
	c := e.InternalClient("aof", true)
	good := pos()           // end of the last complete command
	multiStart := int64(-1) // start of an open MULTI block
	cut := int64(-1)
	var scratch []byte
	for {
		args, err := rr.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			switch {
			case errors.As(err, &pe):
				f.Close()
				return res, fmt.Errorf("aof %s: corrupt command at byte %d: %v (remove or repair the damaged part)", path, good, err)
			case errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF):
				if good != size {
					cut = good
				}
			default:
				f.Close()
				return res, fmt.Errorf("aof %s: %w", path, err)
			}
			break
		}
		// We only ever write RESP arrays, so a command whose size differs from
		// its array encoding was parsed from something else (e.g. text that
		// happened to look like an inline command): the file is damaged.
		scratch = resp.AppendCommand(scratch[:0], args)
		if pos()-good != int64(len(scratch)) {
			f.Close()
			return res, fmt.Errorf("aof %s: corrupt data at byte %d (not a RESP array)", path, good)
		}
		name := strings.ToLower(args[0])
		if name == "multi" {
			multiStart = good
		}
		v := e.Execute(c, args)
		if name == "exec" || name == "discard" {
			multiStart = -1
		}
		res.Commands++
		if v.IsError() {
			res.Errors++
			if res.Errors <= 5 {
				log.Printf("aof: command %q at byte %d failed: %s", args[0], good, v.Str)
			}
		}
		good = pos()
	}
	if multiStart >= 0 { // a transaction never got its EXEC: drop it entirely
		c.Multi = nil
		cut = multiStart
	}
	f.Close()

	if cut >= 0 {
		res.Truncated = size - cut
		log.Printf("aof: WARNING %s ends with an incomplete command; truncating %d bytes (from byte %d)", path, res.Truncated, cut)
		if err := os.Truncate(path, cut); err != nil {
			return res, fmt.Errorf("aof %s: truncating damaged tail: %w", path, err)
		}
	}
	return res, nil
}
