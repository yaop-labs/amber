package query

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// makeMonotonicID builds an EntryID whose first 6 bytes encode `ms` and
// whose suffix is a deterministic function of `seq`. Used by cursor tests
// to keep entry-id ordering aligned with synthetic timestamps — the
// production MustNewEntryID() uses wall-clock + randomness, so 50 IDs
// generated in <1ms share the ms-prefix and randomize their ordering,
// which breaks heap.thresholdID-based top-K guarantees in fixture data.
func makeMonotonicID(ms uint64, seq uint64) model.EntryID {
	var id model.EntryID
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	binary.BigEndian.PutUint64(id[6:14], seq)
	binary.BigEndian.PutUint16(id[14:16], 0)
	return id
}

// buildCursorDataset writes n records spread across one hour into a single
// sealed segment, registers its bitmap index, and returns a ready executor.
// Records use 5 services round-robin'd so a service filter cuts the volume.
func buildCursorDataset(t *testing.T, n int) (*Executor, func()) {
	t.Helper()
	dir := t.TempDir()
	logDir := dir + "/logs"
	spanDir := dir + "/spans"

	policy := storage.RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 1 << 30}
	mgr, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager logs: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		t.Fatalf("OpenSegmentManager spans: %v", err)
	}

	sparse := index.NewSparseIndex()
	spanSparse := index.NewSparseIndex()

	services := []string{"api-gateway", "auth-service", "payment", "worker", "scheduler"}
	base := time.Now().Add(-time.Hour).UnixNano()
	step := int64(time.Hour) / int64(n)
	if step == 0 {
		step = 1
	}

	buf := &bytes.Buffer{}
	batch := make([]storage.BatchItem, 0, n)
	for i := 0; i < n; i++ {
		ts := base + int64(i)*step
		entry := model.LogEntry{
			ID:        makeMonotonicID(uint64(ts/int64(time.Millisecond)), uint64(i)),
			Timestamp: time.Unix(0, ts),
			Level:     model.LevelInfo,
			Service:   services[i%len(services)],
			Host:      fmt.Sprintf("host-%02d", i%5),
			Body:      "test body",
		}
		buf.Reset()
		entry.WriteTo(buf)
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		batch = append(batch, storage.BatchItem{Data: data, TS: entry.Timestamp.UnixNano()})
	}
	if err := mgr.WriteBatch(batch); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if active, ok := mgr.ActiveSegmentMeta(); ok {
		sparse.TouchRange(active.ID, active.FileName, base, base+int64(time.Hour)-1)
	}
	if err := mgr.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	exec := NewExecutor(mgr, spanMgr, sparse, spanSparse)
	for _, seg := range mgr.Segments() {
		segPath := logDir + "/" + seg.FileName
		if idx, err := index.BuildLogBitmapIndex(segPath, log); err == nil {
			exec.RegisterBitmapIndex(seg.FileName, idx)
		}
	}

	cleanup := func() { mgr.Close(); spanMgr.Close() }
	return exec, cleanup
}

// TestExecutor_CursorPagination_NoOverlapNoGap walks the dataset page by
// page and asserts: (1) consecutive pages don't repeat any entry ID, (2)
// the union of all pages equals the full dataset, (3) each page (except
// possibly the last) is exactly Limit big and yields a NextCursor.
func TestExecutor_CursorPagination_NoOverlapNoGap(t *testing.T) {
	const total = 50
	const limit = 7

	exec, cleanup := buildCursorDataset(t, total)
	defer cleanup()

	seen := make(map[model.EntryID]struct{}, total)
	cursor := ""
	pageCount := 0

	for {
		pageCount++
		if pageCount > total {
			t.Fatalf("pagination did not terminate after %d pages", pageCount)
		}
		result, err := exec.ExecLog(context.Background(), &LogQuery{
			Limit:  limit,
			Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("page %d: %v", pageCount, err)
		}

		if len(result.Entries) == 0 {
			break
		}

		for _, e := range result.Entries {
			if _, dup := seen[e.ID]; dup {
				t.Fatalf("duplicate entry id on page %d: %v", pageCount, e.ID)
			}
			seen[e.ID] = struct{}{}
		}

		// Pages strictly newest-first within and across pages.
		for i := 1; i < len(result.Entries); i++ {
			if result.Entries[i].Timestamp.After(result.Entries[i-1].Timestamp) {
				t.Errorf("page %d not sorted newest-first at %d", pageCount, i)
			}
		}

		if result.NextCursor == "" {
			// Last page: must be <= limit and may be empty.
			if len(result.Entries) > limit {
				t.Errorf("last page exceeds limit: %d > %d", len(result.Entries), limit)
			}
			break
		}
		if len(result.Entries) != limit {
			t.Errorf("page %d returned %d entries with NextCursor set; want %d",
				pageCount, len(result.Entries), limit)
		}
		cursor = result.NextCursor
	}

	if len(seen) != total {
		t.Errorf("union of pages: got %d entries, want %d", len(seen), total)
	}
}

// TestExecutor_CursorPagination_AcrossSegments ensures the segment-MinTS
// skip path doesn't accidentally drop records on segment boundaries.
func TestExecutor_CursorPagination_AcrossSegments(t *testing.T) {
	dir := t.TempDir()
	logDir := dir + "/logs"
	spanDir := dir + "/spans"

	policy := storage.RotationPolicy{MaxRecords: 1_000_000, MaxBytes: 1 << 30}
	mgr, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		t.Fatalf("open mgr: %v", err)
	}
	spanMgr, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		t.Fatalf("open span mgr: %v", err)
	}
	defer mgr.Close()
	defer spanMgr.Close()

	sparse := index.NewSparseIndex()

	// Write three segments, each with a distinct hour window. Segments are
	// rotated between writes so each ends up sealed and registered with the
	// sparse index.
	base := time.Now().Add(-3 * time.Hour).UnixNano()
	const perSeg = 20
	for seg := 0; seg < 3; seg++ {
		segStart := base + int64(seg)*int64(time.Hour)
		buf := &bytes.Buffer{}
		batch := make([]storage.BatchItem, 0, perSeg)
		for i := 0; i < perSeg; i++ {
			ts := segStart + int64(i)*int64(time.Minute)
			entry := model.LogEntry{
				ID:        makeMonotonicID(uint64(ts/int64(time.Millisecond)), uint64(seg*perSeg+i)),
				Timestamp: time.Unix(0, ts),
				Level:     model.LevelInfo,
				Service:   "svc",
				Body:      "x",
			}
			buf.Reset()
			entry.WriteTo(buf)
			data := make([]byte, buf.Len())
			copy(data, buf.Bytes())
			batch = append(batch, storage.BatchItem{Data: data, TS: ts})
		}
		if err := mgr.WriteBatch(batch); err != nil {
			t.Fatalf("WriteBatch seg %d: %v", seg, err)
		}
		if active, ok := mgr.ActiveSegmentMeta(); ok {
			sparse.TouchRange(active.ID, active.FileName, segStart, segStart+int64(time.Hour)-1)
		}
		if err := mgr.Rotate(); err != nil {
			t.Fatalf("Rotate seg %d: %v", seg, err)
		}
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	exec := NewExecutor(mgr, spanMgr, sparse, index.NewSparseIndex())
	for _, seg := range mgr.Segments() {
		segPath := logDir + "/" + seg.FileName
		if idx, err := index.BuildLogBitmapIndex(segPath, log); err == nil {
			exec.RegisterBitmapIndex(seg.FileName, idx)
		}
	}

	// Page across all 60 records with a small limit so cursor crosses segment
	// boundaries multiple times.
	total := 3 * perSeg
	seen := make(map[model.EntryID]struct{}, total)
	cursor := ""
	for {
		result, err := exec.ExecLog(context.Background(), &LogQuery{
			Limit:  9,
			Cursor: cursor,
		})
		if err != nil {
			t.Fatalf("paged exec: %v", err)
		}
		for _, e := range result.Entries {
			if _, dup := seen[e.ID]; dup {
				t.Fatalf("duplicate entry id across segments: %v", e.ID)
			}
			seen[e.ID] = struct{}{}
		}
		if result.NextCursor == "" {
			break
		}
		cursor = result.NextCursor
	}

	if len(seen) != total {
		t.Errorf("multi-segment pagination: got %d, want %d", len(seen), total)
	}
}
