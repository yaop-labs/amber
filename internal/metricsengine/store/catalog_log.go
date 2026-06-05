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

// catalog_log.go — append-only catalog log + atomic snapshot+rotation.
// INDEX_EVICTION_SPEC_v0.md §2; codec lives in catalog_log_codec.go.
//
// Files on disk (in the store dir alongside catalog.json):
//
//	catalog.snapshot     — durable point-in-time snapshot of live series.
//	                       One REGISTER record per series. Loaded first
//	                       during recovery.
//	catalog.snapshot.tmp — in-flight snapshot being written; if found alone
//	                       on disk (no catalog.snapshot present), ignore it
//	                       and use prior state. Cleaned up on next boot.
//	catalog.log          — the live append-only log of REGISTER and EVICT
//	                       events since the last snapshot. Read after the
//	                       snapshot during recovery.
//	catalog.log.old      — the previous log, kept across a rotation so a
//	                       crash between rename(log->log.old) and create
//	                       (new log) does NOT lose any records that landed
//	                       just before the snapshot walk. Recovery replays
//	                       it after the snapshot and before the live log.
//
// Lifecycle (rotation; the only mutating compaction path):
//
//	1. (under registry RLock)
//	     walk live series, accumulate REGISTER bytes into a buffer.
//	2. (no lock)
//	     write buffer to catalog.snapshot.tmp + fsync(file) + fsync(dir).
//	     This is the long bit; it does NOT block ingest because the buffer
//	     is already a consistent snapshot taken under the RLock.
//	3. (under registry WRITE lock — microseconds)
//	     atomic rotate:
//	       a) rename(catalog.snapshot.tmp -> catalog.snapshot)
//	       b) rename(catalog.log -> catalog.log.old)
//	       c) create + truncate fresh catalog.log
//	       d) fsync(dir)        // makes a-c durable
//	     swap the writer's file descriptor to the new log. Now ingest may
//	     resume appending.
//	4. (no lock) delete catalog.log.old — best-effort cleanup. If we crash
//	     before this, the next boot's recovery will replay log.old as
//	     no-ops (REGISTERs by id are idempotent against the snapshot) and
//	     delete it on the way out.
//
// Recovery order (read-only at boot):
//
//	1. if catalog.snapshot.tmp exists AND catalog.snapshot does NOT
//	     => orphaned mid-write snapshot; remove the .tmp, proceed.
//	   if catalog.snapshot.tmp exists AND catalog.snapshot exists
//	     => the rename(.tmp->snapshot) succeeded but we crashed before
//	        the directory fsync flushed the unlink-of-.tmp. Both files
//	        on disk; the live one is catalog.snapshot. Remove the .tmp.
//	2. if catalog.snapshot exists: replay it (REGISTER records).
//	3. if catalog.log.old exists: replay it on top.
//	4. if catalog.log exists: replay it on top.
//	5. delete catalog.log.old if it was present (cleanup of a crashed
//	   compaction).
//
// All replay is idempotent: REGISTER keyed by series id (a duplicate is
// dropped by Registry.Import); EVICT against a not-present id is a no-op.
// Crash at any step of the lifecycle leaves the on-disk state recoverable
// to the same logical catalog — see the test matrix in catalog_log_test.go.

const (
	catalogSnapshotFileName    = "catalog.snapshot"
	catalogSnapshotTmpFileName = "catalog.snapshot.tmp"
	catalogLogFileName         = "catalog.log"
	catalogLogOldFileName      = "catalog.log.old"
)

// catalogLog is the live appender. Concurrent appends serialise on mu;
// at our scale (≤ a few hundred new series/sec under churn) this is not a
// hot lock — the registry catalog mutex (the one we're un-blocking) was
// O(N²) per add; this is one short syscall per add.
type catalogLog struct {
	dir string

	mu sync.Mutex
	f  *os.File

	// committer batches fsyncs across concurrent AppendRegister /
	// AppendEvict callers (Postgres-style group commit). When nil, the
	// log falls back to per-op fsync (used during boot-time seeding
	// before the committer is started, and in tests that don't need
	// the goroutine). See catalog_log_committer.go.
	committer *catalogCommitter

	// rotationStop is a test-only crash-injection hook scoped to this
	// catalogLog instance. Production callers leave it empty. When the
	// background compaction goroutine lands in step 3, a package-global
	// hook would race with concurrent rotations from real production
	// code in another test; the per-instance field keeps each test's
	// crash injection isolated to its own log.
	//
	// Valid values match the lifecycle step boundaries: "post-3a",
	// "post-3b", "post-3c", "post-3d". See rotateLog for what each one
	// halts after.
	rotationStop string
}

// openCatalogLog opens (or creates) the live catalog.log file for append.
// Called by Store after recovery — at this point all the recovery files
// have been merged and (if needed) cleaned up.
//
// The returned log has NO committer attached. Callers that want
// group-commit on the ingest hot path must call startCommitter after
// any boot-time seeding (which uses the synchronous Append path so the
// seed is durable before normal ingest starts).
func openCatalogLog(dir string) (*catalogLog, error) {
	f, err := os.OpenFile(filepath.Join(dir, catalogLogFileName),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("catalog log: open append: %w", err)
	}
	return &catalogLog{dir: dir, f: f}, nil
}

// startCommitter switches AppendRegister/AppendEvict from per-op fsync
// to group-commit through a background goroutine. Idempotent: a second
// call is a no-op.
//
// flushInterval is the maximum time an AppendRegister waits for its
// fsync. 5ms is the engine's WAL default; catalog log writes are far
// rarer than WAL writes (registrations, not samples) so the same value
// is fine.
func (c *catalogLog) startCommitter(flushInterval time.Duration) {
	if c.committer != nil {
		return
	}
	c.committer = newCatalogCommitter(c, flushInterval)
}

// AppendRegister writes a REGISTER record and fsyncs. Routed through the
// group-commit committer so concurrent appenders share fsyncs (Postgres-
// style) — see catalog_log_committer.go. Without group commit the per-
// REGISTER fsync was 22% off the ingest knee (see loadtest_v0 3a control).
//
// Durability: AppendRegister returns only AFTER the record's bytes are
// fsync'd. A crash before return MAY leave the record absent on disk;
// the caller treats it as ingest failure. A successful return means the
// REGISTER is durable.
//
// Group-commit safety w.r.t. crash: a batched fsync window <= flush
// interval. If a crash drops a REGISTER from the catalog log but the
// series' samples already landed in the WAL/blocks (which they will if
// the engine's WAL group-commit fsync'd them — independent path),
// reconcileLastTouchFromBlocks on next boot re-registers the series from
// the block's directory entry. So a lost REGISTER is NOT a lost series.
// The coupling holds because the block path's durability is independent
// of the catalog log's. See TestCatalogLogLostRegisterRecoveredFromBlocks.
func (c *catalogLog) AppendRegister(seriesID uint64, labels model.LabelSet) error {
	if c.committer != nil {
		return c.committer.Append(encodeRegister(seriesID, labels))
	}
	return c.appendAndSync(encodeRegister(seriesID, labels))
}

// AppendEvict writes an EVICT record and fsyncs. Routed through the
// group-commit committer (same path as AppendRegister).
//
// Crash safety: a lost EVICT means the series stays in the catalog on
// next boot until the next sweep re-evicts it — idempotent, no data
// loss. Sweep at that point re-detects it as cold (lastTouch unchanged
// from the previous run, reconcile-from-blocks gives the same value).
func (c *catalogLog) AppendEvict(seriesID uint64, ts int64) error {
	if c.committer != nil {
		return c.committer.Append(encodeEvict(seriesID, ts))
	}
	return c.appendAndSync(encodeEvict(seriesID, ts))
}

// appendAndSync is the non-grouped path. Used when the committer isn't
// running (boot-time seeding from rebuilt manifest happens before the
// committer starts; flushing the snapshot file uses its own helper).
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

// writeUnsynced writes the bytes to the live log without fsync. Called
// by the committer; the committer's tick() does the fsync.
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

// sync calls fsync on the live log. Used by the committer's tick().
func (c *catalogLog) sync() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.f == nil {
		return fmt.Errorf("catalog log: closed")
	}
	return c.f.Sync()
}

// Close drains the committer (if running), then flushes and closes the
// live log. Idempotent for repeated Close calls.
//
// Order matters: the committer's flushAndStop does a final tick which
// calls c.sync(); that acquires c.mu. So we must NOT hold c.mu while
// draining the committer. After flushAndStop returns, the goroutine has
// exited and won't touch c.f anymore.
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

// liveSeries is the snapshot input — the caller (Store) gathers this
// under the registry RLock during compaction step 1 and hands it to
// writeSnapshot for the unlocked write phase.
type liveSeries struct {
	ID     uint64
	Labels model.LabelSet
}

// writeSnapshot builds catalog.snapshot.tmp on disk. Caller is responsible
// for taking the registry RLock around its iteration; THIS function does
// no locking — it writes the already-consistent snapshot to disk and
// fsyncs both the file and the directory. The actual swap-into-place
// happens later in rotateLog under the registry write lock.
func writeSnapshot(dir string, series []liveSeries) error {
	path := filepath.Join(dir, catalogSnapshotTmpFileName)
	// O_TRUNC so a leftover .tmp from a prior crashed snapshot is
	// overwritten. The orphan-cleanup at boot also handles this, but
	// being explicit here makes the function self-healing.
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
	// The .tmp's create-and-data are now durable in the file; the
	// directory entry itself may still be in the page cache. fsync the
	// dir so a crash here lands the .tmp on disk (a no-op for recovery,
	// which will clean it up).
	return syncDir(dir)
}

var errSimulatedCrash = errors.New("simulated crash for rotation test")

// rotateLog performs the atomic snapshot-and-log swap (lifecycle step 3).
// Caller MUST hold the registry write lock for the duration of this call
// AND MUST have already written catalog.snapshot.tmp (via writeSnapshot)
// AND fsynced it durably. This function only does the renames + create +
// directory-fsync + descriptor swap.
//
// On success the catalogLog's internal file descriptor points at the new
// (empty) catalog.log; the previous descriptor is closed.
//
// On error the on-disk state is recoverable — the boot path's recovery
// matrix handles each partial-rotation transition (see file header).
func (c *catalogLog) rotateLog() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	snapTmp := filepath.Join(c.dir, catalogSnapshotTmpFileName)
	snap := filepath.Join(c.dir, catalogSnapshotFileName)
	logPath := filepath.Join(c.dir, catalogLogFileName)
	logOld := filepath.Join(c.dir, catalogLogOldFileName)

	// 3a: snapshot.tmp -> snapshot. Atomic on POSIX: a crash here leaves
	// EITHER (snapshot.tmp + old snapshot) OR (new snapshot + log.old
	// not yet renamed). Recovery handles both.
	if err := os.Rename(snapTmp, snap); err != nil {
		return fmt.Errorf("catalog log: rename snapshot: %w", err)
	}
	if c.rotationStop == "post-3a" {
		return errSimulatedCrash
	}
	// 3b: log -> log.old. Crash here: snapshot is new, log.old exists,
	// no live log. Recovery: snapshot + log.old. Will recreate log on
	// boot.
	if err := os.Rename(logPath, logOld); err != nil {
		return fmt.Errorf("catalog log: rename log: %w", err)
	}
	if c.rotationStop == "post-3b" {
		return errSimulatedCrash
	}
	// 3c: create new empty log + open for append.
	newF, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("catalog log: create new log: %w", err)
	}
	if c.rotationStop == "post-3c" {
		newF.Close()
		return errSimulatedCrash
	}
	// 3d: fsync the directory so the four operations (rename, rename,
	// create, and the unlink-of-.tmp implied by the first rename) are
	// all durable. Without this a crash could leave the directory in a
	// state where (e.g.) snapshot.tmp is the only present file because
	// the rename hasn't been flushed.
	if err := syncDir(c.dir); err != nil {
		newF.Close()
		// At this point the in-cache state has the new log open, but
		// disk durability is unknown. Best to surface the error and
		// let the boot path's recovery resolve.
		return fmt.Errorf("catalog log: fsync dir: %w", err)
	}
	// Swap descriptor. Close the old one outside the critical section
	// of the new-write-path so any pending fsync there can't deadlock —
	// we hold c.mu, so any AppendRegister/AppendEvict is blocked
	// regardless until we return.
	old := c.f
	c.f = newF
	if old != nil {
		_ = old.Close()
	}

	if c.rotationStop == "post-3d" {
		return errSimulatedCrash
	}

	// 3e (best-effort cleanup): delete log.old. Not critical — recovery
	// handles it being present if we crash before this point.
	if err := os.Remove(logOld); err != nil && !os.IsNotExist(err) {
		// Non-fatal: next boot's recovery cleans up.
		_ = err
	}
	return nil
}

// loadCatalogLogState is the recovery entry point. It returns the
// reconstructed live set (id -> labels) and the highest series id ever
// observed (so the registry's id allocator picks up after it).
//
// On success the returned map is the merge of (snapshot ∪ log.old ∪ log)
// minus any series whose last record was an EVICT. Recovery is read-only
// for the .snapshot and the two logs; it DOES delete catalog.log.old (if
// present, as cleanup of a crashed rotation) and orphaned .tmp files. The
// live catalog.log is preserved — openCatalogLog will reopen it for
// append.
//
// An ErrCatalogLogCorrupt anywhere in the replay is fatal: returned to
// the caller, which should refuse to start unless the operator has
// explicitly enabled a corruption-override flag (not provided by this
// function; the policy lives at the Store boot path).
func loadCatalogLogState(dir string) (live map[uint64]model.LabelSet, highestID uint64, err error) {
	live = make(map[uint64]model.LabelSet)

	// 1. Orphaned .tmp cleanup. Two cases:
	//    a) .tmp exists, snapshot does NOT: prior snapshot write crashed
	//       mid-flight. Drop the .tmp; prior state (= what's in log) wins.
	//    b) .tmp exists, snapshot ALSO exists: the rename succeeded but
	//       the dir-fsync that removes the .tmp's directory entry didn't
	//       finish before crash. Either file's contents could be the
	//       "live" one — but the rename target (snapshot) is the
	//       intended-live, since the rename happens BEFORE the .tmp is
	//       conceptually retired. Drop the .tmp.
	tmpPath := filepath.Join(dir, catalogSnapshotTmpFileName)
	if _, statErr := os.Stat(tmpPath); statErr == nil {
		if removeErr := os.Remove(tmpPath); removeErr != nil {
			return nil, 0, fmt.Errorf("catalog recovery: remove orphan tmp: %w", removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, 0, fmt.Errorf("catalog recovery: stat tmp: %w", statErr)
	}

	// 2. Snapshot.
	if err := replayCatalogFile(filepath.Join(dir, catalogSnapshotFileName), live, &highestID); err != nil {
		return nil, 0, fmt.Errorf("catalog recovery: snapshot: %w", err)
	}

	// 3. log.old (if a rotation crashed before cleanup).
	logOldPath := filepath.Join(dir, catalogLogOldFileName)
	if err := replayCatalogFile(logOldPath, live, &highestID); err != nil {
		return nil, 0, fmt.Errorf("catalog recovery: log.old: %w", err)
	}
	// 4. live log.
	if err := replayCatalogFile(filepath.Join(dir, catalogLogFileName), live, &highestID); err != nil {
		return nil, 0, fmt.Errorf("catalog recovery: log: %w", err)
	}

	// 5. Cleanup of log.old (only after a successful merge — if we
	// crashed during the merge the next boot will redo it).
	if _, statErr := os.Stat(logOldPath); statErr == nil {
		if removeErr := os.Remove(logOldPath); removeErr != nil {
			return nil, 0, fmt.Errorf("catalog recovery: remove log.old: %w", removeErr)
		}
	} else if !os.IsNotExist(statErr) {
		return nil, 0, fmt.Errorf("catalog recovery: stat log.old: %w", statErr)
	}

	return live, highestID, nil
}

// replayCatalogFile reads a catalog file and applies its records to live.
// Missing file is not an error (a fresh store has no snapshot or log).
// Torn write at EOF is handled by truncating the file to the last
// consistent record boundary.
//
// Invariant on goodOffset: it tracks the file position AFTER the last
// successfully-decoded record. We compute it via Seek(0, SeekCurrent)
// rather than summing readRecord's `n` return because on a torn-tail
// case readRecord may have advanced the cursor by a partial header (no
// `n` returned can describe "what was cleanly consumed before the
// tear" — only the file's actual current-position can). Truncate(goodOffset)
// therefore drops exactly the torn bytes and nothing more; the file
// after recovery contains only complete, CRC-verified records.
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
			// Truncate at the last good boundary and stop. Tolerated.
			// After truncate, file size == goodOffset and every record
			// before that position is intact + CRC-verified — a property
			// the next boot's recovery relies on (otherwise a second
			// torn-tail would compound).
			if err := f.Truncate(goodOffset); err != nil {
				return fmt.Errorf("truncate torn tail at %d: %w", goodOffset, err)
			}
			if err := f.Sync(); err != nil {
				return fmt.Errorf("sync after truncate: %w", err)
			}
			break
		}
		if recErr != nil {
			// ErrCatalogLogCorrupt or other. Refuse to advance.
			return fmt.Errorf("at offset %d: %w", goodOffset, recErr)
		}
		switch rec.typ {
		case catalogRecordRegister:
			// Idempotent: a duplicate id is fine (snapshot built from
			// live set, log.old may re-register the same id, that's OK
			// because the replay is into a map keyed by id).
			live[rec.seriesID] = rec.labels
		case catalogRecordEvict:
			delete(live, rec.seriesID)
		}
		if rec.seriesID > *highestID {
			*highestID = rec.seriesID
		}
		// Advance the good-offset checkpoint.
		off, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("seek after record: %w", err)
		}
		goodOffset = off
	}
	return nil
}
