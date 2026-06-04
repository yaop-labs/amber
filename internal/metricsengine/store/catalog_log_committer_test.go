package store

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TestCatalogLogLostRegisterRecoveredFromBlocks proves the load-bearing
// crash-safety coupling for catalog-log group commit: a REGISTER lost in
// the un-fsync'd window is recovered on restart because the series'
// samples have already reached the blocks (via the engine's WAL group
// commit, which is an independent persistence path). Without this
// coupling, batching catalog fsyncs would be unsafe — every lost REGISTER
// would be a lost series.
//
// Setup:
//  1. Build a real block file with series id=42, labels {job=api}.
//  2. Write a manifest that points at it. No catalog.log entries — the
//     series' REGISTER never fsync'd.
//  3. Open the Store. Recovery loads the manifest, finds the block, the
//     catalog log has no entry for id=42.
//  4. Assert: registry knows the labels {job=api} after recovery.
//  5. Assert: subsequent ingest with the same labels uses a registered
//     series (no panic, no error, eventually queryable).
func TestCatalogLogLostRegisterRecoveredFromBlocks(t *testing.T) {
	dir := t.TempDir()

	// Step 1: a real block on disk.
	const blockName = "block-lost-register.meb"
	series := []block.Series{
		{
			ID:         42,
			Type:       model.MetricTypeCounter,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: []int64{1000, 2000},
			Values:     []int64{10, 20},
		},
	}
	if err := block.WriteFile(filepath.Join(dir, blockName), series); err != nil {
		t.Fatal(err)
	}

	// Step 2: a manifest that points at the block. The catalog log
	// stays empty — simulates "REGISTER never fsync'd before crash".
	manifest := Manifest{Blocks: []BlockMeta{{
		Path:        blockName,
		MinTime:     1000,
		MaxTime:     2000,
		SeriesCount: 1,
	}}}
	if err := saveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}

	// Step 3: open the store. Recovery should:
	//  - load empty catalog from log (no entries),
	//  - rebuild from manifest (catalog.Series gets the block's entry),
	//  - the rebuild branch seeds the log, so series 42 lands there too
	//    on this boot (not lost going forward).
	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	// Step 4: registry must know the labels. Active-series count must
	// include the block-only series.
	if got := st.ActiveSeries(); got < 1 {
		t.Fatalf("ActiveSeries=%d, want >= 1 (lost-REGISTER series should be re-registered)", got)
	}

	// Step 5: a fresh ingest with the same labels must succeed.
	id, err := st.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeCounter, 3000, 30)
	if err != nil {
		t.Fatalf("Append after recovery: %v", err)
	}
	if id == 0 {
		t.Fatal("Append returned zero SeriesID")
	}
}

// TestCatalogLogLostRegisterByBlockReconcileAlone covers the narrower
// case: the rebuild-from-manifest branch did NOT trigger (because
// catalog.Series was non-empty from the log), but a block has a series
// the log doesn't. This is the "mid-run crash with mostly-complete
// catalog log" shape: most REGISTERs landed, the few in the un-fsync'd
// tail didn't, AND the rebuild branch's "len(catalog.Series) == 0" guard
// skips the rebuild. Without the reconcile's resurrect-by-labels path,
// the block's series would only be reachable by labels-on-block-directory
// — but the registry wouldn't track it and the active-series gauge would
// undercount.
func TestCatalogLogLostRegisterByBlockReconcileAlone(t *testing.T) {
	dir := t.TempDir()

	// Pre-populate a catalog log with one entry so the rebuild branch
	// does NOT trigger on next open. The synchronous AppendRegister
	// fsyncs, so this entry is durable on disk.
	{
		cl, err := openCatalogLog(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := cl.AppendRegister(1, model.LabelSet{{Name: "job", Value: "preexisting"}}); err != nil {
			t.Fatal(err)
		}
		cl.Close()
	}

	// Write a block with a series id=42 whose REGISTER never landed in
	// the catalog log.
	const blockName = "block-mid-run-loss.meb"
	if err := block.WriteFile(filepath.Join(dir, blockName), []block.Series{
		{
			ID:         42,
			Type:       model.MetricTypeCounter,
			Labels:     model.LabelSet{{Name: "job", Value: "lost"}},
			Timestamps: []int64{1000},
			Values:     []int64{10},
		},
	}); err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Blocks: []BlockMeta{{
		Path: blockName, MinTime: 1000, MaxTime: 1000, SeriesCount: 1,
	}}}
	if err := saveManifest(dir, manifest); err != nil {
		t.Fatal(err)
	}

	st, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()

	// Both series should be tracked:
	//   1: from the catalog log (synchronous fsync from setup).
	//   42's labels: resurrected by reconcileLastTouchFromBlocks's
	//   GetOrCreateAt fallback (the lost-REGISTER coupling fix).
	if got := st.ActiveSeries(); got < 2 {
		t.Fatalf("ActiveSeries=%d, want 2 (preexisting + lost-REGISTER-resurrected)", got)
	}

	// Ingesting the lost-labels must work and not allocate a duplicate.
	idA, err := st.Append(model.LabelSet{{Name: "job", Value: "lost"}}, model.MetricTypeCounter, 5000, 50)
	if err != nil {
		t.Fatal(err)
	}
	idB, err := st.Append(model.LabelSet{{Name: "job", Value: "lost"}}, model.MetricTypeCounter, 6000, 60)
	if err != nil {
		t.Fatal(err)
	}
	if idA != idB {
		t.Fatalf("same labels yielded different ids: %d vs %d", idA, idB)
	}
}

// TestCatalogLogCommitterBatchesFsyncs is a basic smoke test for the
// group-commit committer — proves Append returns successfully and the
// records land durably. Doesn't try to count fsyncs (that's an
// observation, not a contract); the real perf signal is the post-fix
// control_3a run.
func TestCatalogLogCommitterBatchesFsyncs(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	cl.startCommitter(5 * time.Millisecond)

	for i := 0; i < 100; i++ {
		labels := model.LabelSet{{Name: "id", Value: itoaTest(i)}}
		if err := cl.AppendRegister(uint64(i+1), labels); err != nil {
			t.Fatalf("AppendRegister %d: %v", i, err)
		}
	}
	cl.Close()

	// All 100 records should be recoverable.
	live, highest, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 100 {
		t.Fatalf("live=%d, want 100", len(live))
	}
	if highest != 100 {
		t.Fatalf("highest=%d, want 100", highest)
	}
}

// TestCatalogLogCommitterRecoversAcrossReopen catches the regression
// where the committer's pending writes don't get flushed by Close —
// would manifest as the last few REGISTERs being absent after restart.
func TestCatalogLogCommitterRecoversAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	cl.startCommitter(5 * time.Millisecond)
	if err := cl.AppendRegister(1, model.LabelSet{{Name: "job", Value: "api"}}); err != nil {
		t.Fatal(err)
	}
	if err := cl.AppendRegister(2, model.LabelSet{{Name: "job", Value: "worker"}}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}

	live, highest, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 || highest != 2 {
		t.Fatalf("live=%v highest=%d, want 2 entries", live, highest)
	}

	// And the file should be reopen-able without panicking.
	cl2, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	cl2.Close()
}

// silence the unused-test-helper warning across builds.
var _ = os.Getenv

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
