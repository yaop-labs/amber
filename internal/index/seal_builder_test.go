package index

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// TestBuildLogSealIndexes_OnePass pins the one-pass seal build contract:
// every sidecar is written, the FTS index finds the indexed bodies, and the
// FTS ribbon - built from the FTS build state's token keys, not a separate
// tokenize pass - contains every indexed token (a false negative there
// silently prunes segments out of full-text queries).
func TestBuildLogSealIndexes_OnePass(t *testing.T) {
	dir := t.TempDir()
	segPath := filepath.Join(dir, "seg-000001")
	sw, err := storage.OpenSegmentWriter(segPath)
	if err != nil {
		t.Fatalf("OpenSegmentWriter: %v", err)
	}

	bodies := []string{
		"payment timeout upstream provider=stripe",
		"payment authorized",
		"cache warmup finished req_id=4f8a9b2c-dead-beef-cafe-1234567890ab",
	}
	ids := make([]uint64, 0, len(bodies))
	for _, body := range bodies {
		e, err := model.NewLogEntry(model.LevelInfo, "checkout", "host1", body)
		if err != nil {
			t.Fatalf("NewLogEntry: %v", err)
		}
		e.TraceID = model.TraceID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
		ids = append(ids, model.EntryIDToUint64(e.ID))
		var buf bytes.Buffer
		if _, err := e.WriteTo(&buf); err != nil {
			t.Fatalf("WriteTo: %v", err)
		}
		if err := sw.WriteRecord(buf.Bytes(), e.Timestamp.UnixNano()); err != nil {
			t.Fatalf("WriteRecord: %v", err)
		}
	}
	if err := sw.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	built, err := BuildLogSealIndexes(segPath, nil)
	if err != nil {
		t.Fatalf("BuildLogSealIndexes: %v", err)
	}

	for _, ext := range []string{".bidx", ".fidx", ".filt", ".fts.filt", ".pidx"} {
		if _, err := os.Stat(segPath + ext); err != nil {
			t.Errorf("sidecar %s missing: %v", ext, err)
		}
	}

	ctx := context.Background()
	got, err := built.FTS.Search(ctx, "payment", 10)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	// Search returns ascending entry IDs; same-millisecond ULIDs are not
	// insert-ordered, so compare as sets.
	want := []uint64{ids[0], ids[1]}
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("search(payment) = %v, want %v", got, want)
	}
	// df==1 token from the UUID-bearing body goes through the unique section.
	if got, _ := built.FTS.Search(ctx, "warmup", 10); len(got) != 1 || got[0] != ids[2] {
		t.Fatalf("search(warmup) = %v, want [%d]", got, ids[2])
	}

	if built.FTSRibbon == nil {
		t.Fatal("FTSRibbon not built")
	}
	for _, body := range bodies {
		for _, tok := range TokenizeFTS(body) {
			if tok == "" {
				continue
			}
			if !built.FTSRibbon.Contains([]byte(tok)) {
				t.Errorf("fts ribbon misses indexed token %q", tok)
			}
		}
	}

	if built.Ribbon == nil {
		t.Fatal("trace ribbon not built")
	}
	trace := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	if !built.Ribbon.Contains(trace) {
		t.Error("trace ribbon misses indexed trace_id")
	}

	matched := built.Bitmap.Filter(map[string]string{"service": "checkout"})
	if len(matched) != len(ids) {
		t.Fatalf("bitmap filter(service=checkout) = %v, want %d ids", matched, len(ids))
	}
}
