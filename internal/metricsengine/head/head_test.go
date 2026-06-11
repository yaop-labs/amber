package head

import (
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestSnapshotSortsSamplesByTimestamp(t *testing.T) {
	h := New(index.NewRegistry())
	labels := model.LabelSet{{Name: "job", Value: "api"}}
	h.Append(labels, model.MetricTypeGauge, 2000, 20)
	h.Append(labels, model.MetricTypeGauge, 1000, 10)

	snapshot := h.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("len(snapshot) = %d, want 1", len(snapshot))
	}
	if snapshot[0].Timestamps[0] != 1000 || snapshot[0].Values[0] != 10 {
		t.Fatalf("snapshot was not sorted: %+v", snapshot[0])
	}

	// A snapshot after an in-order append must stay sorted: the lazy sort
	// rewrote the buffer in place and cleared the dirty flag.
	h.Append(labels, model.MetricTypeGauge, 3000, 30)
	snapshot = h.Snapshot()
	got := snapshot[0].Timestamps
	if got[0] != 1000 || got[1] != 2000 || got[2] != 3000 {
		t.Fatalf("snapshot after in-order append not sorted: %v", got)
	}
}

func TestSnapshotMatchingFiltersSeries(t *testing.T) {
	h := New(index.NewRegistry())
	api := model.LabelSet{{Name: "job", Value: "api"}}
	db := model.LabelSet{{Name: "job", Value: "db"}}
	h.Append(api, model.MetricTypeGauge, 1000, 1)
	h.Append(db, model.MetricTypeGauge, 1000, 2)

	snapshot := h.SnapshotMatching(func(labels model.LabelSet) bool {
		v, _ := labels.Get("job")
		return v == "db"
	})
	if len(snapshot) != 1 {
		t.Fatalf("len(snapshot) = %d, want 1", len(snapshot))
	}
	if v, _ := snapshot[0].Labels.Get("job"); v != "db" {
		t.Fatalf("matched wrong series: %+v", snapshot[0].Labels)
	}

	if all := h.SnapshotMatching(nil); len(all) != 2 {
		t.Fatalf("nil match should return all series, got %d", len(all))
	}
}
