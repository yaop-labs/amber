package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// The catalog log stores REGISTER and EVICT records beside periodic snapshots.
// Recovery replays catalog.snapshot, catalog.log.old, and catalog.log in that
// order. Rotation writes catalog.snapshot.tmp, renames it into place, moves the
// old log to catalog.log.old, opens a fresh catalog.log, fsyncs the directory,
// and removes catalog.log.old after the swap.
//
// Replay is idempotent: REGISTER is keyed by series id and EVICT of a missing
// series is a no-op. The recovery path also removes orphaned snapshot temp files
// and stale catalog.log.old files after a successful replay.

const (
	catalogSnapshotFileName    = "catalog.snapshot"
	catalogSnapshotTmpFileName = "catalog.snapshot.tmp"
	catalogLogFileName         = "catalog.log"
	catalogLogOldFileName      = "catalog.log.old"
)

// catalogLog appends records to the live catalog.log file.
type catalogLog struct {
	dir string

	mu sync.Mutex
	f  *os.File

	// committer batches fsyncs for concurrent appends. A nil committer uses
	// per-operation fsyncs during boot-time seeding and selected tests.
	committer *catalogCommitter

	// rotationStop is a test-only crash-injection point for rotation steps.
	rotationStop string
}

// openCatalogLog opens the live catalog.log file for append.
// The returned log starts without a committer so boot-time seeding can use the
// synchronous path.
func openCatalogLog(dir string) (*catalogLog, error) {
	f, err := os.OpenFile(filepath.Join(dir, catalogLogFileName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("catalog log: open append: %w", err)
	}
	return &catalogLog{dir: dir, f: f}, nil
}

// startCommitter enables group commit for catalog appends.
func (c *catalogLog) startCommitter(flushInterval time.Duration) {
	if c.committer != nil {
		return
	}
	c.committer = newCatalogCommitter(c, flushInterval)
}

// AppendRegister writes and syncs a REGISTER record.
func (c *catalogLog) AppendRegister(seriesID uint64, labels model.LabelSet) error {
	if c.committer != nil {
		return c.committer.Append(encodeRegister(seriesID, labels))
	}
	return c.appendAndSync(encodeRegister(seriesID, labels))
}

// AppendEvict writes and syncs an EVICT record.
func (c *catalogLog) AppendEvict(seriesID uint64, ts int64) error {
	if c.committer != nil {
		return c.committer.Append(encodeEvict(seriesID, ts))
	}
	return c.appendAndSync(encodeEvict(seriesID, ts))
}

// appendAndSync writes through the synchronous path used before the committer
// starts.
func (c *catalogLog) appendAndSync(rec []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.f.Write(rec); err != nil {
		return fmt.Errorf("catalog log: write: %w", err)
	}
	if err := c.f.Sync(); err != nil {
		return fmt.Errorf("catalog log: fsync: %w", err)
	}
	return nil
}

// writeUnsynced appends bytes to the live log without fsync.
func (c *catalogLog) writeUnsynced(rec []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		return fmt.Errorf("catalog log: closed")
	}
	if _, err := c.f.Write(rec); err != nil {
		return fmt.Errorf("catalog log: write: %w", err)
	}
	return nil
}

// sync fsyncs the live log.
func (c *catalogLog) sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		return fmt.Errorf("catalog log: closed")
	}
	return c.f.Sync()
}

// Close drains the committer and closes the live log.
func (c *catalogLog) Close() error {
	if c.committer != nil {
		c.committer.flushAndStop()
		c.committer = nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		return nil
	}
	err := c.f.Close()
	c.f = nil
	return err
}

// liveSeries is one catalog snapshot entry.
type liveSeries struct {
	ID     uint64
	Labels model.LabelSet
}

// writeSnapshot writes catalog.snapshot.tmp and fsyncs the file and directory.
func writeSnapshot(dir string, series []liveSeries) error {
	path := filepath.Join(dir, catalogSnapshotTmpFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("catalog snapshot: open tmp: %w", err)
	}
	for _, s := range series {
		if _, err := f.Write(encodeRegister(s.ID, s.Labels)); err != nil {
			f.Close()
			return fmt.Errorf("catalog snapshot: write: %w", err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("catalog snapshot: fsync tmp: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("catalog snapshot: close tmp: %w", err)
	}
	return syncDir(dir)
}

var errSimulatedCrash = errors.New("simulated crash for rotation test")

// rotateLog swaps catalog.snapshot.tmp and catalog.log into their rotated state.
// The caller must hold the registry write lock and must have already written
// the snapshot temp file.
func (c *catalogLog) rotateLog() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapTmp := filepath.Join(c.dir, catalogSnapshotTmpFileName)
	snap := filepath.Join(c.dir, catalogSnapshotFileName)
	logPath := filepath.Join(c.dir, catalogLogFileName)
	logOld := filepath.Join(c.dir, catalogLogOldFileName)

	if err := os.Rename(snapTmp, snap); err != nil {
		return fmt.Errorf("catalog log: rename snapshot: %w", err)
	}
	if c.rotationStop == "post-3a" {
		return errSimulatedCrash
	}
	if err := os.Rename(logPath, logOld); err != nil {
		return fmt.Errorf("catalog log: rename log: %w", err)
	}
	if c.rotationStop == "post-3b" {
		return errSimulatedCrash
	}
	newF, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("catalog log: create new log: %w", err)
	}
	if c.rotationStop == "post-3c" {
		newF.Close()
		return errSimulatedCrash
	}
	if err := syncDir(c.dir); err != nil {
		newF.Close()
		return fmt.Errorf("catalog log: fsync dir: %w", err)
	}
	old := c.f
	c.f = newF
	if old != nil {
		_ = old.Close()
	}

	if c.rotationStop == "post-3d" {
		return errSimulatedCrash
	}

	if err := os.Remove(logOld); err != nil && !os.IsNotExist(err) {
		_ = err
	}
	return nil
}

// loadCatalogLogState rebuilds the live catalog from snapshot and log files.
// It removes recovery temp files only after successful replay.
func loadCatalogLogState(dir string) (live map[uint64]model.LabelSet, highestID uint64, err error) {
	live = make(map[uint64]model.LabelSet)

	tmpPath := filepath.Join(dir, catalogSnapshotTmpFileName)
	if _, statErr := os.Stat(tmpPath); statErr == nil {
		if removeErr := os.Remove(tmpPath); removeErr != nil {
			return nil, 0, fmt.Errorf("catalog recovery: remove orphan tmp: %w", removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, 0, fmt.Errorf("catalog recovery: stat tmp: %w", statErr)
	}

	if err := replayCatalogFile(filepath.Join(dir, catalogSnapshotFileName), live, &highestID); err != nil {
		return nil, 0, fmt.Errorf("catalog recovery: snapshot: %w", err)
	}

	logOldPath := filepath.Join(dir, catalogLogOldFileName)
	if err := replayCatalogFile(logOldPath, live, &highestID); err != nil {
		return nil, 0, fmt.Errorf("catalog recovery: log.old: %w", err)
	}
	if err := replayCatalogFile(filepath.Join(dir, catalogLogFileName), live, &highestID); err != nil {
		return nil, 0, fmt.Errorf("catalog recovery: log: %w", err)
	}

	if _, statErr := os.Stat(logOldPath); statErr == nil {
		if removeErr := os.Remove(logOldPath); removeErr != nil {
			return nil, 0, fmt.Errorf("catalog recovery: remove log.old: %w", removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, 0, fmt.Errorf("catalog recovery: stat log.old: %w", statErr)
	}

	return live, highestID, nil
}

// replayCatalogFile applies a catalog file to live.
// Missing files are ignored. A torn EOF is truncated to the last complete
// record boundary.
func replayCatalogFile(path string, live map[uint64]model.LabelSet, highestID *uint64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()

	var goodOffset int64
	for {
		rec, recErr := readRecord(f)
		if recErr == io.EOF {
			break
		}
		if errors.Is(recErr, ErrCatalogLogTorn) {
			if err := f.Truncate(goodOffset); err != nil {
				return fmt.Errorf("truncate torn tail at %d: %w", goodOffset, err)
			}
			if err := f.Sync(); err != nil {
				return fmt.Errorf("sync after truncate: %w", err)
			}
			break
		}
		if recErr != nil {
			return fmt.Errorf("at offset %d: %w", goodOffset, recErr)
		}
		switch rec.typ {
		case catalogRecordRegister:
			live[rec.seriesID] = rec.labels
		case catalogRecordEvict:
			delete(live, rec.seriesID)
		}
		if rec.seriesID > *highestID {
			*highestID = rec.seriesID
		}
		off, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek after record: %w", err)
		}
		goodOffset = off
	}
	return nil
}
