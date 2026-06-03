package store

import (
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// TestReconcileLastTouchFromBlocks proves the boot-time reconcile sets a
// non-zero last-touch on series whose only evidence-of-life is on-disk
// blocks. Without this the sweep would treat them as "never touched"
// (lastTouch=0 sentinel from a bare Import) and never evict them —
// re-introducing the leak the eviction work is closing.
func TestReconcileLastTouchFromBlocks(t *testing.T) {
	dir := t.TempDir()

	// Write a real block on disk with two series, max timestamps
	// 1000 and 2000 respectively.
	const blockPath = "block-0001.amb"
	series := []block.Series{
		{
			ID:         1,
			Type:       model.MetricTypeCounter,
			Labels:     model.LabelSet{{Name: "job", Value: "api"}},
			Timestamps: []int64{500, 1000},
			Values:     []int64{1, 2},
		},
		{
			ID:         2,
			Type:       model.MetricTypeGauge,
			Labels:     model.LabelSet{{Name: "job", Value: "worker"}},
			Timestamps: []int64{1500, 2000},
			Values:     []int64{1, 2},
		},
	}
	if err := block.WriteFile(filepath.Join(dir, blockPath), series); err != nil {
		t.Fatal(err)
	}

	registry := index.NewRegistry()
	registry.Import(1, model.LabelSet{{Name: "job", Value: "api"}})
	registry.Import(2, model.LabelSet{{Name: "job", Value: "worker"}})

	// Both series should start at lastTouch=0 (the Import sentinel).
	if ts, _ := registry.LastTouch(1); ts != 0 {
		t.Fatalf("pre-reconcile id=1 ts=%d want 0", ts)
	}

	manifest := Manifest{Blocks: []BlockMeta{{Path: blockPath}}}
	if err := reconcileLastTouchFromBlocks(dir, manifest, registry); err != nil {
		t.Fatal(err)
	}

	if ts, _ := registry.LastTouch(1); ts != 1000 {
		t.Fatalf("post-reconcile id=1 ts=%d want 1000", ts)
	}
	if ts, _ := registry.LastTouch(2); ts != 2000 {
		t.Fatalf("post-reconcile id=2 ts=%d want 2000", ts)
	}

	// A subsequent reconcile of the SAME block must not regress (idempotent).
	if err := reconcileLastTouchFromBlocks(dir, manifest, registry); err != nil {
		t.Fatal(err)
	}
	if ts, _ := registry.LastTouch(1); ts != 1000 {
		t.Fatalf("after second reconcile id=1 ts=%d want 1000 unchanged", ts)
	}

	// A later UpdateLastTouch with an OLDER ts must not regress either —
	// the monotonic-max contract holds across the reconcile and ingest
	// paths together.
	registry.UpdateLastTouch(1, 750)
	if ts, _ := registry.LastTouch(1); ts != 1000 {
		t.Fatalf("after stale UpdateLastTouch id=1 ts=%d want 1000 unchanged", ts)
	}
}
