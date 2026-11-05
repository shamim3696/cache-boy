// Package persistence ties snapshots and the AOF to the engine: it loads data
// at startup, applies the "save" rules, runs BGSAVE / BGREWRITEAOF and
// flushes everything at shutdown. It implements command.Persister.
package persistence

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"cache-boy/internal/command"
	"cache-boy/internal/persistence/aof"
	"cache-boy/internal/persistence/snapshot"
)

// ErrBgSaveInProgress is returned by Save/BgSave while a background save runs.
var ErrBgSaveInProgress = errors.New("ERR Background save already in progress")

// Manager implements command.Persister.
type Manager struct {
	e   *command.Engine
	aof *aof.AOF // nil when appendonly is off

	saving       atomic.Bool  // a SAVE or BGSAVE is running
	bgSaving     atomic.Bool  // ... and it is a BGSAVE
	lastSave     atomic.Int64 // unix seconds of the last successful save
	lastSaveOK   atomic.Bool
	lastSaveSecs atomic.Int64 // duration of the last save, seconds
	lastSaveTry  atomic.Int64 // unix seconds of the last attempt (success or not)

	wg       sync.WaitGroup // background saves/rewrites
	stop     chan struct{}
	stopOnce sync.Once
	cronDone chan struct{}

	// CronInterval is how often save rules and auto AOF rewrite are checked.
	CronInterval time.Duration
}

// Setup loads the dataset (the AOF when appendonly is on and the file exists,
// otherwise the snapshot), opens the AOF and registers it as a propagator,
// sets e.Persist and starts the save-rules goroutine. Call it after
// command.New and before serving clients.
func Setup(e *command.Engine) (*Manager, error) {
	cfg := e.Cfg.Get()
	m := &Manager{
		e:            e,
		stop:         make(chan struct{}),
		cronDone:     make(chan struct{}),
		CronInterval: time.Second,
	}
	m.lastSave.Store(time.Now().Unix())
	m.lastSaveOK.Store(true)

	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	aofPath := m.AOFPath()
	aofExists := fileExists(aofPath)

	e.SetLoading(true)
	start := time.Now()
	var err error
	loadedSnapshot := false
	switch {
	case cfg.AppendOnly && aofExists:
		var res aof.LoadResult
		res, err = aof.Load(aofPath, e)
		if err == nil {
			log.Printf("persistence: loaded %s (%d keys from preamble, %d commands) in %v",
				aofPath, res.Keys, res.Commands, time.Since(start).Round(time.Millisecond))
		}
	default:
		err = snapshot.LoadFile(m.SnapshotPath(), e)
		switch {
		case err == nil:
			loadedSnapshot = true
			if st, serr := os.Stat(m.SnapshotPath()); serr == nil {
				m.lastSave.Store(st.ModTime().Unix())
			}
			log.Printf("persistence: loaded %s (%d keys) in %v", m.SnapshotPath(), e.DB.Len(), time.Since(start).Round(time.Millisecond))
		case errors.Is(err, fs.ErrNotExist):
			err = nil
		}
	}
	e.SetLoading(false)
	if err != nil {
		return nil, err
	}
	e.DB.ResetDirty(e.DB.Dirty()) // loaded data is not "changes since last save"

	if cfg.AppendOnly {
		a, err := aof.Open(aofPath, cfg.AppendFsync)
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w", aofPath, err)
		}
		m.aof = a
		e.AddPropagator(a)
		// AOF just turned on with existing data: write it to the new AOF so
		// the next restart (which reads only the AOF) doesn't lose it.
		if !aofExists && (loadedSnapshot || e.DB.Len() > 0) {
			if err := a.Rewrite(e); err != nil {
				return nil, fmt.Errorf("initial AOF rewrite: %w", err)
			}
		}
	}

	e.Persist = m
	go m.cron()
	return m, nil
}

// SnapshotPath is dir/dbfilename.
func (m *Manager) SnapshotPath() string {
	cfg := m.e.Cfg.Get()
	return filepath.Join(cfg.Dir, cfg.DBFilename)
}

// AOFPath is dir/appendfilename.
func (m *Manager) AOFPath() string {
	cfg := m.e.Cfg.Get()
	return filepath.Join(cfg.Dir, cfg.AppendFilename)
}

// AOF returns the open AOF, or nil when appendonly is off.
func (m *Manager) AOF() *aof.AOF { return m.aof }

// Save writes a snapshot synchronously (SAVE). It takes DB.LockAll itself,
// so it must not be called while holding shard locks.
func (m *Manager) Save() error {
	if !m.saving.CompareAndSwap(false, true) {
		return ErrBgSaveInProgress
	}
	defer m.saving.Store(false)
	return m.save()
}

func (m *Manager) save() error {
	start := time.Now()
	m.lastSaveTry.Store(start.Unix())
	snap, dirty, err := snapshot.Bytes(m.e)
	if err == nil {
		err = snapshot.WriteFileAtomic(m.SnapshotPath(), snap)
	}
	m.lastSaveSecs.Store(int64(time.Since(start).Seconds()))
	m.lastSaveOK.Store(err == nil)
	if err != nil {
		log.Printf("persistence: saving %s failed: %v", m.SnapshotPath(), err)
		return err
	}
	m.e.DB.ResetDirty(dirty)
	m.lastSave.Store(time.Now().Unix())
	return nil
}

// BgSave starts a snapshot in the background (BGSAVE). The keyspace is only
// locked while it is serialised into memory; the disk write runs unlocked.
func (m *Manager) BgSave() error {
	if !m.saving.CompareAndSwap(false, true) {
		return ErrBgSaveInProgress
	}
	m.bgSaving.Store(true)
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer m.saving.Store(false)
		defer m.bgSaving.Store(false)
		m.save()
	}()
	return nil
}

// BgRewriteAOF compacts the AOF in the background (BGREWRITEAOF).
func (m *Manager) BgRewriteAOF() error {
	if m.aof == nil {
		return errors.New("ERR AOF is disabled (set appendonly yes at startup)")
	}
	if m.aof.Rewriting() {
		return aof.ErrRewriteInProgress
	}
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		start := time.Now()
		if err := m.aof.Rewrite(m.e); err != nil {
			if !errors.Is(err, aof.ErrRewriteInProgress) {
				log.Printf("persistence: AOF rewrite failed: %v", err)
			}
			return
		}
		log.Printf("persistence: AOF rewritten (%d bytes) in %v", m.aof.Size(), time.Since(start).Round(time.Millisecond))
	}()
	return nil
}

// LastSave is the time of the last successful snapshot.
func (m *Manager) LastSave() time.Time { return time.Unix(m.lastSave.Load(), 0) }

// Info returns the persistence section of INFO.
func (m *Manager) Info() [][2]string {
	status := func(ok bool) string {
		if ok {
			return "ok"
		}
		return "err"
	}
	b := func(v bool) string {
		if v {
			return "1"
		}
		return "0"
	}
	i64 := func(n int64) string { return strconv.FormatInt(n, 10) }
	out := [][2]string{
		{"rdb_bgsave_in_progress", b(m.bgSaving.Load())},
		{"rdb_last_save_time", i64(m.lastSave.Load())},
		{"rdb_last_bgsave_status", status(m.lastSaveOK.Load())},
		{"rdb_last_bgsave_time_sec", i64(m.lastSaveSecs.Load())},
		{"rdb_filename", m.SnapshotPath()},
		{"aof_enabled", b(m.aof != nil)},
	}
	if m.aof == nil {
		return append(out,
			[2]string{"aof_rewrite_in_progress", "0"},
			[2]string{"aof_last_bgrewrite_status", "ok"},
			[2]string{"aof_last_write_status", "ok"},
		)
	}
	return append(out,
		[2]string{"aof_rewrite_in_progress", b(m.aof.Rewriting())},
		[2]string{"aof_last_bgrewrite_status", status(m.aof.LastRewriteOK())},
		[2]string{"aof_last_write_status", status(m.aof.LastWriteErr() == nil)},
		[2]string{"aof_current_size", i64(m.aof.Size())},
		[2]string{"aof_base_size", i64(m.aof.BaseSize())},
		[2]string{"aof_fsync", m.e.Cfg.Get().AppendFsync},
	)
}

// cron applies the save rules (save <seconds> <changes>) and the automatic
// AOF rewrite rule, and picks up CONFIG SET appendfsync.
func (m *Manager) cron() {
	defer close(m.cronDone)
	t := time.NewTicker(m.CronInterval)
	defer t.Stop()
	for {
		select {
		case <-m.stop:
			return
		case <-m.e.Stopping():
			return
		case <-t.C:
		}
		cfg := m.e.Cfg.Get()
		if m.aof != nil {
			m.aof.SetFsync(cfg.AppendFsync)
			if m.aof.NeedsRewrite(cfg.AutoAOFRewritePercentage, cfg.AutoAOFRewriteMinSize) {
				log.Printf("persistence: starting automatic AOF rewrite (size %d, base %d)", m.aof.Size(), m.aof.BaseSize())
				m.BgRewriteAOF()
			}
		}
		if m.saving.Load() {
			continue
		}
		now := time.Now().Unix()
		// After a failed save, wait a bit before retrying (as Redis does).
		if !m.lastSaveOK.Load() && now-m.lastSaveTry.Load() < 5 {
			continue
		}
		dirty := m.e.DB.Dirty()
		elapsed := now - m.lastSave.Load()
		for _, r := range cfg.Save {
			if dirty >= int64(r.Changes) && dirty > 0 && elapsed >= int64(r.Seconds) {
				log.Printf("persistence: %d changes in %d seconds, saving", dirty, r.Seconds)
				m.BgSave()
				break
			}
		}
	}
}

// Close stops background work and makes everything durable: waits for
// running saves/rewrites, writes a final snapshot if save is true, and
// flushes, fsyncs and closes the AOF. Call it at shutdown after the server
// stopped accepting commands.
func (m *Manager) Close(save bool) error {
	m.stopOnce.Do(func() { close(m.stop) })
	<-m.cronDone
	m.wg.Wait()
	var errs []error
	if save {
		if err := m.Save(); err != nil {
			errs = append(errs, fmt.Errorf("final save: %w", err))
		}
	}
	if m.aof != nil {
		m.e.RemovePropagator(m.aof)
		if err := m.aof.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing AOF: %w", err))
		}
	}
	return errors.Join(errs...)
}

func fileExists(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}
