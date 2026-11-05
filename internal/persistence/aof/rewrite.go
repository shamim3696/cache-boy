package aof

import (
	"errors"
	"fmt"
	"os"

	"cache-boy/internal/command"
	"cache-boy/internal/persistence/snapshot"
)

// ErrRewriteInProgress is returned when a rewrite is already running.
var ErrRewriteInProgress = errors.New("ERR Background append only file rewriting already in progress")

// Rewrite compacts the AOF (BGREWRITEAOF). It blocks until done; callers
// that want it in the background run it in a goroutine.
//
// The steps, and the lock held during each:
//
//  1. DB.LockAll: encode a snapshot of the dataset into memory and start
//     capturing every newly propagated command into a rewrite buffer. Holding
//     every shard lock guarantees no write is half-way between the two.
//  2. No lock: write the snapshot to "<aof>.rewrite.tmp" and fsync it. Clients
//     keep writing; their commands go to the old file AND the rewrite buffer.
//  3. AOF mutex (writes pause briefly): append the rewrite buffer to the temp
//     file, fsync, close the old file, rename the temp file over it, reopen
//     it and keep appending there.
//
// If anything fails before the rename, the old file is untouched and keeps
// receiving commands.
func (a *AOF) Rewrite(e *command.Engine) error {
	if !a.rewriting.CompareAndSwap(false, true) {
		return ErrRewriteInProgress
	}
	defer a.rewriting.Store(false)
	err := a.rewrite(e)
	a.rewriteOK.Store(err == nil)
	return err
}

func (a *AOF) rewrite(e *command.Engine) error {
	// 1. point-in-time snapshot + start capturing
	unlock := e.DB.LockAll()
	snap, err := snapshot.AppendDB(nil, e.DB, e.DumpModules)
	if err == nil {
		a.mu.Lock()
		if a.closed {
			err = errors.New("aof is closed")
		} else {
			a.capturing, a.capture = true, nil
		}
		a.mu.Unlock()
	}
	unlock()
	if err != nil {
		return err
	}
	stopCapture := func() {
		a.mu.Lock()
		a.capturing, a.capture = false, nil
		a.mu.Unlock()
	}

	// 2. write the preamble without blocking anyone
	tmpPath := a.path + ".rewrite.tmp"
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		stopCapture()
		return err
	}
	fail := func(err error) error {
		tmp.Close()
		os.Remove(tmpPath)
		stopCapture()
		return fmt.Errorf("aof rewrite: %w", err)
	}
	if _, err := tmp.Write(snap); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	snapLen := int64(len(snap))
	snap = nil

	// 3. append what was captured meanwhile and swap the files. Holding
	// fileMu stops the flusher; holding mu stops Propagate, so nothing can
	// be appended to the old file after its last captured command.
	a.fileMu.Lock()
	defer a.fileMu.Unlock()
	a.mu.Lock()
	defer a.mu.Unlock()
	failLocked := func(err error) error { // like fail, but mu is already held
		tmp.Close()
		os.Remove(tmpPath)
		a.capturing, a.capture = false, nil
		return fmt.Errorf("aof rewrite: %w", err)
	}
	if a.closed {
		return failLocked(errors.New("aof is closed"))
	}
	if _, err := tmp.Write(a.capture); err != nil {
		return failLocked(err)
	}
	if err := tmp.Sync(); err != nil {
		return failLocked(err)
	}
	newSize := snapLen + int64(len(a.capture))
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		a.capturing, a.capture = false, nil
		return fmt.Errorf("aof rewrite: %w", err)
	}

	// Windows can't rename over a file that is open, so close the old one
	// first; if the rename fails, reopen it and carry on as before.
	a.f.Close()
	renameErr := os.Rename(tmpPath, a.path)
	f, err := os.OpenFile(a.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		// Can't even reopen: keep commands buffered, report the error.
		a.setErr(err)
		a.capturing, a.capture = false, nil
		return fmt.Errorf("aof rewrite: reopening %s: %w", a.path, err)
	}
	a.f = f
	if renameErr != nil {
		os.Remove(tmpPath)
		a.capturing, a.capture = false, nil
		return fmt.Errorf("aof rewrite: %w", renameErr)
	}
	snapshot.SyncDir(dirOf(a.path))

	// Everything still pending for the old file is already in the new one.
	a.buf = nil
	a.capturing, a.capture = false, nil
	a.needSync = false
	a.size.Store(newSize)
	a.baseSize.Store(newSize)
	a.writeErr.Store(nil)
	return nil
}

func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if os.IsPathSeparator(path[i]) {
			return path[:i+1]
		}
	}
	return "."
}

// NeedsRewrite applies the auto-aof-rewrite rule: the file is bigger than
// minSize and has grown by at least percent% since the last rewrite.
// percent <= 0 disables automatic rewrites.
func (a *AOF) NeedsRewrite(percent int, minSize int64) bool {
	if percent <= 0 || a.Rewriting() {
		return false
	}
	size, base := a.Size(), a.BaseSize()
	if size < minSize {
		return false
	}
	if base == 0 {
		return true
	}
	return (size-base)*100/base >= int64(percent)
}
