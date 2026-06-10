package retention

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/storage"
)

type failingDeleteStore struct {
	base storage.SegmentStore
	fail bool
}

func (s *failingDeleteStore) Put(name string, r io.Reader) error { return s.base.Put(name, r) }
func (s *failingDeleteStore) Get(name string) (io.ReadCloser, error) {
	return s.base.Get(name)
}
func (s *failingDeleteStore) Delete(name string) error {
	if s.fail {
		return errors.New("delete failed")
	}
	return s.base.Delete(name)
}
func (s *failingDeleteStore) DeleteLocal(name string) error { return s.base.DeleteLocal(name) }
func (s *failingDeleteStore) List() ([]string, error)       { return s.base.List() }

func setupTestCleaner(t *testing.T, policy Policy, numSegments int) (*Cleaner, *storage.SegmentManager, string) {
	t.Helper()
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	manager, err := storage.OpenSegmentManager(dir, storage.RotationPolicy{
		MaxRecords: 100,
		MaxBytes:   1 << 30,
	})
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}

	sparse := index.NewSparseIndex()

	for seg := 0; seg < numSegments; seg++ {
		ts := time.Now().Add(-time.Duration(numSegments-seg) * 24 * time.Hour).UnixNano()
		for i := 0; i < 10; i++ {
			data := []byte("test record for retention testing padding")
			if err := manager.Write(data, ts+int64(i)); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		if err := manager.Rotate(); err != nil {
			t.Fatalf("Rotate: %v", err)
		}
	}

	cleaner := NewCleaner(manager, sparse, policy, dir, "logs", log)
	return cleaner, manager, dir
}

func TestCleaner_NoPolicy(t *testing.T) {
	cleaner, manager, _ := setupTestCleaner(t, Policy{}, 3)
	defer manager.Close()

	deleted, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deletions with empty policy, got %d", deleted)
	}
}

func TestCleaner_MaxSegments(t *testing.T) {
	cleaner, manager, dir := setupTestCleaner(t, Policy{MaxSegments: 2}, 5)
	defer manager.Close()

	before := len(manager.Segments())
	if before != 5 {
		t.Fatalf("expected 5 segments before cleanup, got %d", before)
	}

	deleted, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if deleted != 3 {
		t.Errorf("expected 3 deletions, got %d", deleted)
	}

	entries, _ := os.ReadDir(dir)
	var alogCount int
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > 5 && e.Name()[len(e.Name())-5:] == ".alog" {
			alogCount++
		}
	}
	if alogCount > 3 {
		t.Errorf("expected at most 3 .alog files on disk, got %d", alogCount)
	}
	if after := len(manager.Segments()); after != 2 {
		t.Fatalf("expected manager metadata to keep 2 sealed segments after cleanup, got %d", after)
	}
}

func TestCleaner_DeletePendingRetriesAfterFileDeleteFailure(t *testing.T) {
	cleaner, manager, dir := setupTestCleaner(t, Policy{}, 1)
	defer manager.Close()

	seg := manager.Segments()[0]
	store := &failingDeleteStore{base: storage.NewLocalStore(dir), fail: true}
	manager.SetStore(store)

	if err := cleaner.deleteSegment(seg); err == nil {
		t.Fatal("deleteSegment returned nil, want delete failure")
	}
	if got := manager.Segments(); len(got) != 0 {
		t.Fatalf("queryable segments after pending delete = %d, want 0", len(got))
	}
	pending := manager.SegmentsForRetention()
	if len(pending) != 1 || !pending[0].DeletePending {
		t.Fatalf("retention segments = %+v, want one DeletePending segment", pending)
	}

	store.fail = false
	n, err := cleaner.Run()
	if err != nil {
		t.Fatalf("retry Run: %v", err)
	}
	if n != 1 {
		t.Fatalf("retry deleted %d segments, want 1", n)
	}
	if got := manager.SegmentsForRetention(); len(got) != 0 {
		t.Fatalf("segments after retry = %d, want 0", len(got))
	}
}

func TestCleaner_MaxAge(t *testing.T) {
	cleaner, manager, _ := setupTestCleaner(t, Policy{MaxAge: 48 * time.Hour}, 5)
	defer manager.Close()

	deleted, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if deleted < 2 {
		t.Errorf("expected at least 2 deletions for 48h max_age with 5 segments spanning 5 days, got %d", deleted)
	}
}

func TestCleaner_EmptyStorage(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))

	manager, err := storage.OpenSegmentManager(dir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()

	sparse := index.NewSparseIndex()
	cleaner := NewCleaner(manager, sparse, Policy{MaxSegments: 5}, dir, "logs", log)

	deleted, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deletions on empty storage, got %d", deleted)
	}
}

func TestSelectForDeletion_MaxTotalBytes(t *testing.T) {
	cleaner, manager, _ := setupTestCleaner(t, Policy{MaxTotalBytes: 1}, 3)
	defer manager.Close()

	deleted, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if deleted < 2 {
		t.Errorf("expected at least 2 deletions with 1-byte limit, got %d", deleted)
	}
}

func TestCleaner_RequireUploadedSkipsLocalOnly(t *testing.T) {
	cleaner, manager, _ := setupTestCleaner(t, Policy{MaxSegments: 1}, 3)
	defer manager.Close()
	cleaner.RequireUploaded(true)

	deleted, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if deleted != 0 {
		t.Errorf("expected 0 deletions when no segments are Uploaded, got %d", deleted)
	}

	for _, seg := range manager.Segments() {
		if err := manager.MarkUploaded(seg.ID); err != nil {
			t.Fatalf("MarkUploaded: %v", err)
		}
	}
	deleted, err = cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if deleted != 2 {
		t.Errorf("expected 2 deletions after marking Uploaded, got %d", deleted)
	}
}

func TestSelectForDeletion_ReasonLabels(t *testing.T) {
	old := time.Now().Add(-72 * time.Hour).UnixNano()
	now := time.Now().UnixNano()
	cleaner, manager, _ := setupTestCleaner(t, Policy{MaxAge: 48 * time.Hour, MaxSegments: 1}, 0)
	defer manager.Close()

	segs := []storage.SegmentMeta{
		{ID: 1, FileName: "seg_1.alog", MaxTS: old},
		{ID: 2, FileName: "seg_2.alog", MaxTS: old},
		{ID: 3, FileName: "seg_3.alog", MaxTS: now, SizeBytes: 1024},
	}

	got := cleaner.selectForDeletion(segs)

	byID := map[uint32]string{}
	for _, c := range got {
		byID[c.seg.ID] = c.reason
	}
	if byID[1] != "max_age" {
		t.Errorf("seg 1: reason=%q, want max_age", byID[1])
	}
	if byID[2] != "max_age" {
		t.Errorf("seg 2: reason=%q, want max_age", byID[2])
	}
	if byID[3] != "" {
		t.Errorf("seg 3 should not be selected (MaxSegments leaves it as the youngest): reason=%q", byID[3])
	}
}

func TestFilterOut(t *testing.T) {
	all := []storage.SegmentMeta{
		{ID: 1, FileName: "seg_1"},
		{ID: 2, FileName: "seg_2"},
		{ID: 3, FileName: "seg_3"},
	}
	exclude := []storage.SegmentMeta{
		{ID: 2, FileName: "seg_2"},
	}

	result := filterOut(all, exclude)
	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result[0].ID != 1 || result[1].ID != 3 {
		t.Errorf("wrong result: %v", result)
	}
}

func TestCleaner_LocalEvictionRequiresUploaded(t *testing.T) {
	cleaner, manager, dir := setupTestCleaner(t, Policy{LocalMaxAge: time.Hour}, 3)

	n, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 evictions for non-uploaded segments, got %d", n)
	}

	for _, s := range manager.Segments() {
		path := filepath.Join(dir, s.FileName)
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("segment file %s should still exist: %v", s.FileName, err)
		}
	}
}

func TestCleaner_LocalEvictionByMaxAge(t *testing.T) {
	cleaner, manager, dir := setupTestCleaner(t, Policy{LocalMaxAge: 36 * time.Hour}, 3)

	for _, s := range manager.Segments() {
		if err := manager.MarkUploaded(s.ID); err != nil {
			t.Fatalf("MarkUploaded(%d): %v", s.ID, err)
		}
	}

	n, err := cleaner.Run()
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n != 2 {
		t.Fatalf("expected 2 local evictions, got %d", n)
	}

	segs := manager.Segments()
	if len(segs) != 3 {
		t.Fatalf("expected manifest to retain all 3 segments, got %d", len(segs))
	}

	var presentCount, absentCount int
	for _, s := range segs {
		path := filepath.Join(dir, s.FileName)
		_, statErr := os.Stat(path)
		if s.HasLocalCopy() {
			presentCount++
			if statErr != nil {
				t.Errorf("segment %s marked present but file missing: %v", s.FileName, statErr)
			}
		} else {
			absentCount++
			if !os.IsNotExist(statErr) {
				t.Errorf("segment %s marked absent but file still on disk", s.FileName)
			}
		}
	}
	if presentCount != 1 || absentCount != 2 {
		t.Errorf("expected 1 present + 2 absent, got %d present + %d absent", presentCount, absentCount)
	}
}

// TestCleaner_LocalEvictionSkipsAlreadyEvicted confirms idempotency: running
// twice doesn't double-count or error.
func TestCleaner_LocalEvictionIdempotent(t *testing.T) {
	cleaner, manager, _ := setupTestCleaner(t, Policy{LocalMaxAge: 36 * time.Hour}, 3)
	for _, s := range manager.Segments() {
		if err := manager.MarkUploaded(s.ID); err != nil {
			t.Fatalf("MarkUploaded: %v", err)
		}
	}

	first, err := cleaner.Run()
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}
	second, err := cleaner.Run()
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if first != 2 || second != 0 {
		t.Errorf("first=%d second=%d; expected 2 then 0", first, second)
	}
}
