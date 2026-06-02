package runtime

import (
	"log/slog"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	"github.com/yaop-labs/amber/internal/selfobs"
)

// runDogfoodScraper periodically snapshots the in-process selfobs registry
// and writes the result into amber's own embedded metric store. This is the
// "amber observes itself" loop: scrape-equivalent telemetry without an
// external Prometheus, so single-node deployments still get rate() over their
// own counters via the same /api/v1/metrics/rate endpoint that external
// callers use.
//
// Scope choices: we bypass the OTLP path and call AppendBatch directly. The
// HTTP loopback alternative would exercise the real ingest pipeline, but at
// the cost of a parser dependency and an extra goroutine per scrape; for a
// purely internal feedback loop, the direct call is cheaper and equally
// observable downstream.
func runDogfoodScraper(interval time.Duration, store *mestore.Store, log *slog.Logger, stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			samples := snapshotToSamples(selfobs.Snapshot(), time.Now().UnixMilli())
			if len(samples) == 0 {
				continue
			}
			if _, err := store.AppendBatch(samples); err != nil {
				log.Warn("dogfood scrape append failed", "err", err, "samples", len(samples))
			}
		}
	}
}

// snapshotToSamples converts a selfobs.Snapshot result into metricsengine
// samples. The "type" field in selfobs ("counter"|"gauge") maps directly onto
// MetricTypeCounter/MetricTypeGauge; everything else (including histogram
// _count/_sum derived series) rides through this translation unchanged.
func snapshotToSamples(snap []selfobs.Sample, tsMillis int64) []model.Sample {
	if len(snap) == 0 {
		return nil
	}
	out := make([]model.Sample, 0, len(snap))
	for _, s := range snap {
		labels := make(model.LabelSet, 0, len(s.Labels)+1)
		labels = append(labels, model.Label{Name: model.MetricNameLabel, Value: s.Name})
		for _, l := range s.Labels {
			labels = append(labels, model.Label{Name: l.Name, Value: l.Value})
		}
		typ := model.MetricTypeGauge
		if s.Type == "counter" {
			typ = model.MetricTypeCounter
		}
		out = append(out, model.Sample{
			Labels:    labels,
			Type:      typ,
			Timestamp: tsMillis,
			Value:     int64(s.Value),
		})
	}
	return out
}
