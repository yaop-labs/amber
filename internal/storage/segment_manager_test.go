package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTestManager(t *testing.T) (*SegmentManager, string) {
	t.Helper()
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	t.Cleanup(func() { sm.Close() })
	return sm, dir
}

func writeN(t *testing.T, sm *SegmentManager, n int) {
	t.Helper()
	base := time.Now().UnixNano()
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("record-%d", i))
		ts := base + int64(i)*int64(time.Millisecond)
		if err := sm.Write(data, ts); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}
}

func TestSegmentManager_Open_CreatesStructure(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	defer sm.Close()

	if !fileExists(filepath.Join(dir, metaFileName)) {
		t.Error("meta.json not created")
	}
	if !fileExists(filepath.Join(dir, walFileName)) {
		t.Error("amber.wal not created")
	}
	if _, ok := sm.ActiveSegmentMeta(); !ok {
		t.Error("no active segment after open")
	}
}

func TestSegmentManager_Open_Idempotent(t *testing.T) {
	dir := t.TempDir()
	sm1, _ := OpenSegmentManager(dir, DefaultRotationPolicy)
	writeN(t, sm1, 10)
	sm1.Close()

	sm2, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sm2.Close()

	if _, ok := sm2.ActiveSegmentMeta(); !ok {
		t.Error("no active segment after reopen")
	}
}

func TestSegmentManager_Write_Single(t *testing.T) {
	sm, _ := newTestManager(t)
	if err := sm.Write([]byte("hello"), time.Now().UnixNano()); err != nil {
		t.Fatalf("Write: %v", err)
	}
}

func TestSegmentManager_Write_Many(t *testing.T) {
	sm, _ := newTestManager(t)
	writeN(t, sm, 100)
}

func TestSegmentManager_Rotation_ByRecordCount(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 10})
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	defer sm.Close()

	writeN(t, sm, 25)

	if sealed := sm.Segments(); len(sealed) < 2 {
		t.Errorf("expected >=2 sealed segments, got %d", len(sealed))
	}
}

func TestSegmentManager_Rotation_ByBytes(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{MaxBytes: 200})
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	defer sm.Close()

	base := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		data := []byte(fmt.Sprintf("record-with-some-content-%d-padding", i))
		sm.Write(data, base+int64(i))
	}

	if sealed := sm.Segments(); len(sealed) == 0 {
		t.Error("expected at least 1 sealed segment after byte limit rotation")
	}
}

func TestSegmentManager_Rotate_Manual(t *testing.T) {
	sm, _ := newTestManager(t)
	writeN(t, sm, 5)
	before := len(sm.Segments())

	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	if after := len(sm.Segments()); after != before+1 {
		t.Errorf("expected %d sealed segments, got %d", before+1, after)
	}
}

func TestSegmentManager_Rotate_EmptySegment_NoOp(t *testing.T) {
	sm, _ := newTestManager(t)
	before := len(sm.Segments())
	sm.Rotate()
	if after := len(sm.Segments()); after != before {
		t.Errorf("rotating empty segment should be no-op: %d -> %d", before, after)
	}
}

func TestSegmentManager_Meta_Persisted(t *testing.T) {
	dir := t.TempDir()
	sm1, _ := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 5})
	writeN(t, sm1, 10)
	sm1.Close()

	sm2, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sm2.Close()

	if sealed := sm2.Segments(); len(sealed) == 0 {
		t.Error("sealed segments not persisted in meta.json")
	}
}

func TestSegmentManager_Meta_SealedHasTimestamps(t *testing.T) {
	dir := t.TempDir()
	sm, _ := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 3})
	defer sm.Close()

	sm.Write([]byte("a"), int64(2_000_000))
	sm.Write([]byte("b"), int64(1_000_000))
	sm.Write([]byte("c"), int64(3_000_000))
	sm.Write([]byte("d"), int64(1_000_000))

	sealed := sm.Segments()
	if len(sealed) == 0 {
		t.Fatal("expected sealed segment")
	}
	s := sealed[0]
	if s.MinTS == 0 || s.MaxTS == 0 {
		t.Errorf("sealed segment has zero timestamps: min=%d max=%d", s.MinTS, s.MaxTS)
	}
	if s.MinTS > s.MaxTS {
		t.Errorf("minTS > maxTS: %d > %d", s.MinTS, s.MaxTS)
	}
}

func TestSegmentManager_WALRecovery(t *testing.T) {
	dir := t.TempDir()
	sm1, _ := OpenSegmentManager(dir, DefaultRotationPolicy)
	writeN(t, sm1, 5)
	sm1.Close()

	sm2, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sm2.Close()

	total := uint64(0)
	for _, seg := range sm2.Segments() {
		total += seg.RecordCount
	}
	if total == 0 {
		t.Error("all records lost after reopen")
	}
}

// Verifies the WAL-checkpoint contract: writes are durable across a
// simulated crash (no Close), no records lost, no duplicates surface after
// reopen and seal. Mixes records that trigger flushBlock+checkpoint with
// records that ride along in the WAL only.
func TestSegmentManager_Checkpoint_NoLossNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	policy := RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 0}

	sm1, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Tiny block size so flushBlock fires repeatedly within the workload.
	sm1.active.blockSize = 256

	const n = 300
	for i := 0; i < n; i++ {
		data := []byte(fmt.Sprintf("rec-%05d", i))
		ts := int64(i + 1)
		if err := sm1.Write(data, ts); err != nil {
			t.Fatalf("Write[%d]: %v", i, err)
		}
	}
	// Simulate crash: drop sm1 without Close (no footer, possibly some
	// records still only in WAL).

	sm2, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := sm2.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := sm2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Scan all sealed segments and collect payloads.
	sm3, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	defer sm3.Close()

	seen := make(map[string]int)
	for _, seg := range sm3.Segments() {
		path := filepath.Join(dir, seg.FileName)
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			t.Fatalf("reader %s: %v", path, err)
		}
		err = sr.Scan(func(data []byte) error {
			seen[string(data)]++
			return nil
		})
		_ = sr.Close()
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
	}

	if len(seen) != n {
		t.Errorf("unique records: want %d, got %d", n, len(seen))
	}
	dupes := 0
	for k, c := range seen {
		if c != 1 {
			dupes++
			t.Logf("duplicated %q × %d", k, c)
		}
	}
	if dupes > 0 {
		t.Errorf("%d records duplicated after recovery", dupes)
	}
	for i := 0; i < n; i++ {
		if seen[fmt.Sprintf("rec-%05d", i)] == 0 {
			t.Errorf("record %d lost", i)
		}
	}
}

// Regression test for the bug where appendSegmentWriter created a fresh
// writer with empty blockOffsets, so a rotate after restart wrote a footer
// pointing only at the post-restart blocks and orphaned everything written
// before the crash.
func TestSegmentManager_AppendRecovery_PreservesPreCrashBlocks(t *testing.T) {
	dir := t.TempDir()
	policy := RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 0}

	sm1, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// Force tiny blocks so flushBlock fires repeatedly within the test.
	sm1.active.blockSize = 64

	const preCrash = 20
	writeN(t, sm1, preCrash)
	if err := sm1.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	// Simulate crash: drop sm1 without Close so the footer is never written
	// and meta is not rewritten. The OS still holds the active segment file
	// open via sm1.active.file; that's fine for the reopen below on Linux.

	sm2, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := sm2.active.recordCount; got != preCrash {
		t.Fatalf("recovered recordCount: want %d, got %d", preCrash, got)
	}
	if len(sm2.active.blockOffsets) == 0 {
		t.Fatalf("recovered blockOffsets is empty; pre-crash blocks would be orphaned on rotate")
	}

	const postCrash = 5
	writeN(t, sm2, postCrash)
	if err := sm2.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	if err := sm2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	sm3, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("reopen post-rotate: %v", err)
	}
	defer sm3.Close()

	var total uint64
	for _, seg := range sm3.Segments() {
		total += seg.RecordCount
	}
	if want := uint64(preCrash + postCrash); total != want {
		t.Errorf("sealed total: want %d, got %d", want, total)
	}
}

func TestSegmentManager_SegmentPath(t *testing.T) {
	sm, dir := newTestManager(t)
	writeN(t, sm, 5)
	sm.Rotate()

	for _, seg := range sm.Segments() {
		path := sm.SegmentPath(seg)
		if !fileExists(path) {
			t.Errorf("segment file not found: %s", path)
		}
		if want := filepath.Join(dir, seg.FileName); path != want {
			t.Errorf("path mismatch: got %s, want %s", path, want)
		}
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}
