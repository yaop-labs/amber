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
}
