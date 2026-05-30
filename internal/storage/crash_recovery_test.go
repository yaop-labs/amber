package storage

import (
	"encoding/binary"
	"testing"
	"time"
)

// These two tests pin durability/correctness defects in the crash-recovery
// path that the existing crash_test.go misses: it exercises only the single
// Write() path and verifies record presence, not the WriteBatch() path nor the
// rebuilt time range.

// TestSegmentManager_WriteBatch_TailSurvivesCrash proves that records a
// successful WriteBatch acknowledged are not lost on a crash.
//
// WriteBatch fsyncs every item to the WAL first, then writes them into the
// active segment. checkpoint() runs when a block flushes mid-batch and ends
// with wal.Truncate(), which zeroes the WHOLE WAL — including the seqs of the
// trailing batch items that landed only in the in-memory blockBuf and never
// reached a durable on-disk block. After a crash those items exist nowhere.
//
// The "crash" is modeled in-process by abandoning the manager (no Close, which
// would seal+flush the tail) and reopening the data dir: only bytes that
// actually reached the fd survive, exactly as after SIGKILL. blockBuf lives in
// a bytes.Buffer that is never written to the fd until a block flush, so the
// reopen sees precisely the post-crash on-disk state — deterministically.
func TestSegmentManager_WriteBatch_TailSurvivesCrash(t *testing.T) {
	dir := t.TempDir()

	// No rotation: rotate() would seal the active segment and flush the tail to
	// disk, masking the bug. We want the tail stranded in blockBuf.
	policy := RotationPolicy{MaxRecords: 0, MaxBytes: 0}

	sm, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	// Intentionally NOT closed: Close() seals the active segment and would make
	// the tail durable, which is the very thing we are testing the absence of.

	// 1500 records of ~5 KiB cross the 4 MiB block boundary exactly once: the
	// first ~839 records flush as block 0, the remaining ~660 sit in blockBuf.
	const (
		n       = 1500
		recSize = 5000
	)
	base := time.Now().UnixNano()
	items := make([]BatchItem, n)
	for i := range items {
		data := make([]byte, recSize)
		binary.LittleEndian.PutUint32(data[:4], uint32(i))
		items[i] = BatchItem{Data: data, TS: base + int64(i)}
	}

	if err := sm.WriteBatch(items); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Precondition: a checkpoint must have happened mid-batch, otherwise the WAL
	// still holds everything and the bug cannot manifest. If this fails, the
	// batch was too small to flush a block — raise n or recSize.
	meta, ok := sm.ActiveSegmentMeta()
	if !ok || meta.LastSyncedSeq == 0 {
		t.Fatalf("test precondition not met: no mid-batch checkpoint (LastSyncedSeq=%d) — increase batch size", meta.LastSyncedSeq)
	}

	// Crash + restart: reopen the same directory. Only durable bytes survive.
	sm2, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer sm2.Close()

	// Seal the recovered active segment so every record is scannable.
	if err := sm2.Rotate(); err != nil {
		t.Fatalf("rotate after recovery: %v", err)
	}

	seen := make(map[uint32]bool, n)
	for _, seg := range sm2.Segments() {
		if seg.RecordCount == 0 {
			continue
		}
		sr, err := OpenSegmentReader(sm2.SegmentPath(seg), nil)
		if err != nil {
			t.Fatalf("open reader %s: %v", seg.FileName, err)
		}
		scanErr := sr.Scan(func(data []byte) error {
			if len(data) >= 4 {
				seen[binary.LittleEndian.Uint32(data[:4])] = true
			}
			return nil
		})
		_ = sr.Close()
		if scanErr != nil {
			t.Fatalf("scan %s: %v", seg.FileName, scanErr)
		}
	}

	if len(seen) != n {
		t.Errorf("WriteBatch acknowledged %d records but only %d survived the crash: "+
			"checkpoint truncated the WAL past the durable block, dropping the in-memory tail",
			n, len(seen))
	}
}

// TestSegmentManager_CrashRecovery_PreservesTimeRange proves that a segment
// recovered and sealed after a crash reports its true time range.
//
// The per-record event timestamp is NOT stored in the segment block (only the
// opaque record bytes are) and the WAL records for already-flushed blocks are
// skipped on replay, so the range of a crash-recovered active segment is
// recoverable only from the durable watermark checkpoint() persists into meta.
// Records carry an explicit ts that appears nowhere in their bytes, so a
// rebuild that tries to parse it from the block would get garbage — the seal
// footer must instead reflect the meta-seeded range.
//
// The crash is modeled in-process by abandoning the manager (no Close) and
// reopening the dir; see the sibling WriteBatch test for why that is faithful.
func TestSegmentManager_CrashRecovery_PreservesTimeRange(t *testing.T) {
	dir := t.TempDir()
	policy := RotationPolicy{MaxRecords: 0, MaxBytes: 0} // no rotation

	sm, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	// Intentionally NOT closed — Close() would seal cleanly and bypass recovery.

	// Cross the 4 MiB block boundary so a checkpoint runs (persisting the range
	// into meta) and a footerless block survives on disk for recovery to rebuild.
	const (
		n       = 1000
		recSize = 5000
	)
	base := time.Date(2026, 5, 29, 12, 0, 0, 0, time.UTC).UnixNano()
	items := make([]BatchItem, n)
	for i := range items {
		data := make([]byte, recSize) // opaque: the true ts is not in here
		items[i] = BatchItem{Data: data, TS: base + int64(i)}
	}
	wantMin, wantMax := base, base+int64(n-1)

	if err := sm.WriteBatch(items); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	meta, ok := sm.ActiveSegmentMeta()
	if !ok || meta.LastSyncedSeq == 0 || meta.MinTS == 0 {
		t.Fatalf("test precondition not met: no checkpoint persisted a time range "+
			"(LastSyncedSeq=%d MinTS=%d) — increase batch size", meta.LastSyncedSeq, meta.MinTS)
	}

	// Crash + restart.
	sm2, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer sm2.Close()
	if err := sm2.Rotate(); err != nil {
		t.Fatalf("rotate after recovery: %v", err)
	}

	sealed := sm2.Segments()
	if len(sealed) == 0 {
		t.Fatal("no sealed segment after recovery")
	}
	seg := sealed[0]
	if seg.RecordCount != n {
		t.Errorf("recovered RecordCount = %d, want %d", seg.RecordCount, n)
	}
	if seg.MinTS != wantMin || seg.MaxTS != wantMax {
		t.Errorf("recovered segment time range = [%d,%d], want [%d,%d]: "+
			"crash recovery lost the true time range, so this segment is mis-pruned",
			seg.MinTS, seg.MaxTS, wantMin, wantMax)
	}
}
