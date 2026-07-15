package amber

import (
	"context"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/metricsengine"
)

func TestNativeLogSpanReachReplayJournal(t *testing.T) {
	root := t.TempDir()
	db, err := Open(root, &Options{BatchTimeout: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	logEntry, err := NewLogEntry(LevelWarn, "api", "node", "native", Attr{Key: "k", Value: "v"})
	if err != nil {
		t.Fatal(err)
	}
	traceID := TraceID{1}
	spanID := SpanID{2}
	spanEntry, err := NewSpanEntry(traceID, spanID, model.ZeroSpanID(), "api", "GET /")
	if err != nil {
		t.Fatal(err)
	}
	spanEntry.EndTime = spanEntry.StartTime.Add(time.Millisecond)
	if err := db.Log(context.Background(), logEntry); err != nil {
		t.Fatal(err)
	}
	if err := db.Span(context.Background(), spanEntry); err != nil {
		t.Fatal(err)
	}
	metricLabels := metricsengine.LabelSet{{Name: metricsengine.MetricNameLabel, Value: "native_int"}}
	if _, err := db.MetricStore().AppendBatch([]metricsengine.Sample{{
		Labels: metricLabels, Type: metricsengine.MetricTypeGauge, Timestamp: 10, Value: 7,
	}}); err != nil {
		t.Fatal(err)
	}
	floatLabels := metricsengine.LabelSet{{Name: metricsengine.MetricNameLabel, Value: "native_float"}}
	if _, err := db.MetricStore().AppendScaledFloat(floatLabels, metricsengine.MetricTypeGauge, 11, 1.25, 1000); err != nil {
		t.Fatal(err)
	}
	if _, err := db.MetricStore().AppendSketches([]engine.SketchSample{{
		Labels:    metricsengine.LabelSet{{Name: metricsengine.MetricNameLabel, Value: "native_histogram"}},
		Timestamp: 12, Explicit: histogram.ExplicitFromValues([]float64{1}, []float64{0.5, 2}),
	}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	signals := make(map[otlpv4.Signal]int)
	if err := otlpv4.Replay(context.Background(), root, func(envelope otlpv4.Envelope) error {
		if envelope.Fidelity() != otlpv4.FidelityNormalizedNative {
			t.Fatalf("fidelity = %v", envelope.Fidelity())
		}
		signals[envelope.Signal()]++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(signals) != 3 || signals[otlpv4.SignalLogs] != 1 || signals[otlpv4.SignalTraces] != 1 || signals[otlpv4.SignalMetrics] != 3 {
		t.Fatalf("signals = %v", signals)
	}
}
