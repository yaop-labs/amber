package storage

import (
	"errors"
	"fmt"
	"io"
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
	for i := range n {
		data := fmt.Appendf(nil, "record-%d", i)
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

func TestActiveSegmentMetaReflectsUncheckpointedWriterState(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	defer sm.Close()

	items := []BatchItem{
		{Data: []byte("one"), TS: 300},
		{Data: []byte("two"), TS: 100},
		{Data: []byte("three"), TS: 200},
	}
	if err := sm.WriteBatch(items); err != nil {
		t.Fatal(err)
	}
	meta, ok := sm.ActiveSegmentMeta()
	if !ok {
		t.Fatal("active segment metadata missing")
	}
	if meta.RecordCount != 3 || meta.MinTS != 100 || meta.MaxTS != 300 {
		t.Fatalf("active metadata = count:%d range:[%d,%d], want count:3 range:[100,300]", meta.RecordCount, meta.MinTS, meta.MaxTS)
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

func TestSegmentManagerTransitionFailureIsSticky(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Write([]byte("before failure"), 1); err != nil {
		t.Fatal(err)
	}

	// Inject a deterministic seal transition failure.
	if err := sm.active.file.Close(); err != nil {
		t.Fatal(err)
	}
	first := sm.Rotate()
	if first == nil {
		t.Fatal("Rotate succeeded with closed active segment file")
	}
	second := sm.Write([]byte("must not append"), 2)
	if second == nil || second.Error() != first.Error() {
		t.Fatalf("write after transition failure = %v, want sticky %v", second, first)
	}
	if err := sm.Close(); err == nil {
		t.Fatal("Close hid the manager's terminal failure")
	}
}

func TestSegmentManagerClosePropagatesWALTruncateError(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Write([]byte("durable in wal"), 1); err != nil {
		t.Fatal(err)
	}
	// Closing the descriptor behind WAL makes the close-time truncate fail.
	if err := sm.wal.file.Close(); err != nil {
		t.Fatal(err)
	}
	first := sm.Close()
	if first == nil {
		t.Fatal("Close returned nil after WAL truncate/close failure")
	}
	if second := sm.Close(); second == nil || second.Error() != first.Error() {
		t.Fatalf("second Close = %v, want stable %v", second, first)
	}
}

func TestSegmentManagerClosePreservesWALWhenActivePathIsMissing(t *testing.T) {
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if err := sm.Write([]byte("wal is the recovery copy"), 1); err != nil {
		t.Fatal(err)
	}
	active, ok := sm.ActiveSegmentMeta()
	if !ok {
		t.Fatal("active metadata missing")
	}
	if err := os.Remove(sm.SegmentPath(active)); err != nil {
		t.Fatal(err)
	}
	if err := sm.Close(); err == nil {
		t.Fatal("Close succeeded after active path disappeared")
	}

	walPath := filepath.Join(dir, walFileName)
	before, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if before.Size() == 0 {
		t.Fatal("Close truncated the only recovery WAL copy")
	}
	if reopened, err := OpenSegmentManager(dir, RotationPolicy{}); err == nil {
		_ = reopened.Close()
		t.Fatal("Open guessed recovery state despite missing active segment")
	}
	after, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("failed reopen changed recovery WAL size from %d to %d", before.Size(), after.Size())
	}
}

func TestSegmentManagerRotateFaultMatrixRecoversExactlyOnce(t *testing.T) {
	points := []string{
		"rotate:meta:before_tmp_open",
		"rotate:meta:after_tmp_write",
		"rotate:meta:after_tmp_sync",
		"rotate:meta:before_rename",
		"rotate:meta:after_rename",
		"rotate:meta:before_dir_sync",
		"rotate:before_wal_truncate",
		"create:before_segment_file",
		"create:meta:after_tmp_write",
		"create:meta:before_rename",
		"create:meta:after_rename",
		"create:meta:before_dir_sync",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			sm, err := OpenSegmentManager(dir, RotationPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			if err := sm.Write([]byte("fault-matrix-record"), 100); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("simulated transition fault")
			sm.fault = func(got string) error {
				if got == point {
					return injected
				}
				return nil
			}
			first := sm.Rotate()
			if !errors.Is(first, injected) {
				t.Fatalf("Rotate error = %v, want injected fault at %s", first, point)
			}
			if second := sm.Write([]byte("must-not-append"), 200); second == nil || second.Error() != first.Error() {
				t.Fatalf("Write after %s = %v, want sticky %v", point, second, first)
			}
			if err := sm.Close(); err == nil {
				t.Fatalf("Close hid terminal fault at %s", point)
			}
			assertRecoveredPayloadOnce(t, dir, "fault-matrix-record")
		})
	}
}

func TestSegmentManagerCloseFaultMatrixRecoversExactlyOnce(t *testing.T) {
	points := []string{
		"close:meta:before_tmp_open",
		"close:meta:after_tmp_write",
		"close:meta:after_tmp_sync",
		"close:meta:before_rename",
		"close:meta:after_rename",
		"close:meta:before_dir_sync",
		"close:before_wal_truncate",
	}
	for _, point := range points {
		t.Run(point, func(t *testing.T) {
			dir := t.TempDir()
			sm, err := OpenSegmentManager(dir, RotationPolicy{})
			if err != nil {
				t.Fatal(err)
			}
			if err := sm.Write([]byte("close-fault-record"), 100); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("simulated close fault")
			sm.fault = func(got string) error {
				if got == point {
					return injected
				}
				return nil
			}
			first := sm.Close()
			if !errors.Is(first, injected) {
				t.Fatalf("Close error = %v, want injected fault at %s", first, point)
			}
			if second := sm.Close(); second == nil || second.Error() != first.Error() {
				t.Fatalf("second Close = %v, want stable %v", second, first)
			}
			assertRecoveredPayloadOnce(t, dir, "close-fault-record")
		})
	}
}

func assertRecoveredPayloadOnce(t *testing.T, dir, want string) {
	t.Helper()
	sm, err := OpenSegmentManager(dir, RotationPolicy{})
	if err != nil {
		t.Fatalf("reopen after injected fault: %v", err)
	}
	defer sm.Close()
	if sm.ActiveRecordCount() > 0 {
		if err := sm.Rotate(); err != nil {
			t.Fatalf("seal recovered active: %v", err)
		}
	}
	count := 0
	for _, seg := range sm.Segments() {
		sr, err := OpenSegmentReader(sm.SegmentPath(seg), nil)
		if err != nil {
			t.Fatalf("open recovered segment %s: %v", seg.FileName, err)
		}
		err = sr.Scan(func(data []byte) error {
			if string(data) == want {
				count++
			}
			if string(data) == "must-not-append" {
				t.Errorf("sticky manager appended a rejected record")
			}
			return nil
		})
		_ = sr.Close()
		if err != nil {
			t.Fatalf("scan recovered segment %s: %v", seg.FileName, err)
		}
	}
	if count != 1 {
		t.Fatalf("recovered payload %q count = %d, want 1", want, count)
	}
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
	for i := range 20 {
		data := fmt.Appendf(nil, "record-with-some-content-%d-padding", i)
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

// Regression test for the rotate() ordering bug: the WAL must be truncated
// BEFORE the next segment is created. The old order (createNewSegment, then
// wal.Truncate) had a crash window where meta already contained a fresh
// unsealed segment with LastSyncedSeq=0, so replayWAL re-applied the entire
// WAL into it - duplicating every record of the just-sealed segment.
//
// With the fixed order the only reachable crash state is "sealed segment in
// meta, no unsealed segment, WAL not yet truncated". This test constructs
// exactly that state (by restoring a pre-rotate WAL snapshot) and verifies
// reopen drops the orphan WAL records instead of replaying them.
func TestSegmentManager_Rotate_CrashBeforeWALTruncate_NoDuplicates(t *testing.T) {
	dir := t.TempDir()
	policy := RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 0}

	sm1, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	const n = 100
	base := time.Now().UnixNano()
	items := make([]BatchItem, 0, n)
	for i := range n {
		items = append(items, BatchItem{
			Data: fmt.Appendf(nil, "rot-rec-%05d", i),
			TS:   base + int64(i),
		})
	}
	if err := sm1.WriteBatch(items); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	// Snapshot the WAL while it still holds all n records.
	walPath := filepath.Join(dir, walFileName)
	walCopy, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	if len(walCopy) == 0 {
		t.Fatal("wal unexpectedly empty before rotate")
	}

	if err := sm1.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	if err := sm1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate the crash state: seal is durable in meta, but the WAL was not
	// truncated yet.
	if err := os.WriteFile(walPath, walCopy, 0o600); err != nil {
		t.Fatalf("restore wal: %v", err)
	}

	sm2, err := OpenSegmentManager(dir, policy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sm2.Close()

	// Orphan WAL records must be dropped, not replayed into a fresh segment.
	if size, err := sm2.wal.Size(); err != nil {
		t.Fatalf("wal size: %v", err)
	} else if size != 0 {
		t.Errorf("wal not truncated after orphan-drop recovery: %d bytes", size)
	}
	if got := sm2.ActiveRecordCount(); got != 0 {
		t.Errorf("active segment has %d records, want 0 (WAL replayed into it?)", got)
	}

	seen := make(map[string]int, n)
	for _, seg := range sm2.Segments() {
		if seg.RecordCount == 0 {
			continue
		}
		sr, err := OpenSegmentReader(filepath.Join(dir, seg.FileName), nil)
		if err != nil {
			t.Fatalf("reader %s: %v", seg.FileName, err)
		}
		scanErr := sr.Scan(func(data []byte) error {
			seen[string(data)]++
			return nil
		})
		_ = sr.Close()
		if scanErr != nil {
			t.Fatalf("scan %s: %v", seg.FileName, scanErr)
		}
	}

	for i := range n {
		key := fmt.Sprintf("rot-rec-%05d", i)
		switch seen[key] {
		case 0:
			t.Errorf("record %d lost", i)
		case 1:
			// OK
		default:
			t.Errorf("record %d duplicated ×%d", i, seen[key])
		}
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

// recordingStore captures Delete and DeleteLocal calls without touching disk
// or the network.
type recordingStore struct {
	deleted      []string
	deletedLocal []string
}

func (r *recordingStore) Put(_ string, _ io.Reader) error { return nil }
func (r *recordingStore) Get(name string) (io.ReadCloser, error) {
	return nil, fmt.Errorf("not implemented: %s", name)
}
func (r *recordingStore) Delete(name string) error {
	r.deleted = append(r.deleted, name)
	return nil
}
func (r *recordingStore) DeleteLocal(name string) error {
	r.deletedLocal = append(r.deletedLocal, name)
	return nil
}
func (r *recordingStore) List() ([]string, error) { return nil, nil }

func TestDeleteSegmentFiles_CoversAllSidecars(t *testing.T) {
	sm, _ := newTestManager(t)
	rec := &recordingStore{}
	sm.SetStore(rec)

	meta := SegmentMeta{FileName: "seg_00000001.alog"}
	if err := sm.DeleteSegmentFiles(meta); err != nil {
		t.Fatalf("DeleteSegmentFiles: %v", err)
	}

	if len(rec.deleted) != len(SegmentSidecarExts) {
		t.Fatalf("delete call count: got %d, want %d (%v)", len(rec.deleted), len(SegmentSidecarExts), rec.deleted)
	}

	got := make(map[string]bool, len(rec.deleted))
	for _, name := range rec.deleted {
		got[name] = true
	}
	for _, ext := range SegmentSidecarExts {
		want := meta.FileName + ext
		if !got[want] {
			t.Errorf("missing delete for %q", want)
		}
	}

	// Regression guard: .pidx (posting index) was historically omitted, leaving
	// orphaned files in S3 after retention. Assert it explicitly so a future
	// refactor of SegmentSidecarExts can't silently drop it.
	if !got[meta.FileName+".pidx"] {
		t.Errorf(".pidx not deleted; SegmentSidecarExts regressed")
	}
}

func TestUploadState_PendingAndMarkUploaded(t *testing.T) {
	sm, _ := newTestManager(t)
	writeN(t, sm, 3)
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	writeN(t, sm, 3)
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	pending := sm.PendingUploads()
	if len(pending) != 2 {
		t.Fatalf("pending after two rotations: got %d, want 2", len(pending))
	}

	if err := sm.MarkUploaded(pending[0].ID); err != nil {
		t.Fatalf("MarkUploaded: %v", err)
	}

	pending2 := sm.PendingUploads()
	if len(pending2) != 1 {
		t.Fatalf("pending after one upload: got %d, want 1", len(pending2))
	}
	if pending2[0].ID == pending[0].ID {
		t.Errorf("MarkUploaded did not transition segment %d", pending[0].ID)
	}

	// Idempotency.
	if err := sm.MarkUploaded(pending[0].ID); err != nil {
		t.Errorf("MarkUploaded second call: %v", err)
	}
}

func TestUploadState_RecordFailurePersists(t *testing.T) {
	sm, dir := newTestManager(t)
	writeN(t, sm, 3)
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	pending := sm.PendingUploads()
	if len(pending) != 1 {
		t.Fatalf("pending: got %d, want 1", len(pending))
	}
	id := pending[0].ID

	if err := sm.RecordUploadFailure(id, "s3: timeout"); err != nil {
		t.Fatalf("RecordUploadFailure: %v", err)
	}
	if err := sm.RecordUploadFailure(id, "s3: 503"); err != nil {
		t.Fatalf("RecordUploadFailure: %v", err)
	}

	// Reopen from disk and check both fields survived.
	if err := sm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sm2, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("Reopen: %v", err)
	}
	defer sm2.Close()

	// Close seals the active segment, so reopen may surface more than one
	// pending entry. Find the one we recorded failures against and assert
	// counters survived the round-trip.
	var found SegmentMeta
	var ok bool
	for _, seg := range sm2.PendingUploads() {
		if seg.ID == id {
			found = seg
			ok = true
			break
		}
	}
	if !ok {
		t.Fatalf("segment %d missing from pending after reopen", id)
	}
	if found.UploadAttempts != 2 {
		t.Errorf("UploadAttempts: got %d, want 2", found.UploadAttempts)
	}
	if found.LastUploadErr != "s3: 503" {
		t.Errorf("LastUploadErr: got %q, want %q", found.LastUploadErr, "s3: 503")
	}
}
