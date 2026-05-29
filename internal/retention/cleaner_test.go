package retention

import (
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/storage"
)

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

	cleaner := NewCleaner(manager, sparse, policy, dir, log)
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
	cleaner := NewCleaner(manager, sparse, Policy{MaxSegments: 5}, dir, log)

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
	// MaxSegments=1 with 3 segments would normally evict 2. With
	// RequireUploaded enabled and none marked Uploaded, nothing should be
	// deleted — protecting segments still in flight to S3.
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

	// Mark all three as Uploaded; retention should now evict the surplus.
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
