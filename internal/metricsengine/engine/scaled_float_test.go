package engine

import (
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestAppendScaledFloat(t *testing.T) {
	e := New()
	labels := model.LabelSet{{Name: "job", Value: "api"}}
	if _, err := e.AppendScaledFloat(labels, model.MetricTypeGauge, 1000, 12.345, 100); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "scaled.meb")
	if err := e.FlushBlock(path); err != nil {
		t.Fatal(err)
	}
	series, err := block.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := series[0].Values[0]; got != 1235 {
		t.Fatalf("scaled value = %d, want 1235", got)
	}
}

func TestAppendScaledFloatRejectsInvalidScale(t *testing.T) {
	e := New()
	if _, err := e.AppendScaledFloat(nil, model.MetricTypeGauge, 0, 1, 0); err == nil {
		t.Fatal("expected invalid scale error")
	}
}
