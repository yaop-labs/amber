package head

import (
	"sort"
	"sync"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

type Head struct {
	mu       sync.RWMutex
	registry *index.Registry
	series   map[index.SeriesID]*bufferedSeries
}

type bufferedSeries struct {
	typ        model.MetricType
	labels     model.LabelSet
	timestamps []int64
	values     []int64
}

func New(registry *index.Registry) *Head {
	return &Head{
		registry: registry,
		series:   make(map[index.SeriesID]*bufferedSeries),
	}
}

func (h *Head) Append(labels model.LabelSet, typ model.MetricType, timestamp int64, value int64) index.SeriesID {
	id := h.registry.GetOrCreateAt(labels, timestamp)

	h.mu.Lock()
	defer h.mu.Unlock()

	buf := h.series[id]
	if buf == nil {
		buf = &bufferedSeries{typ: typ, labels: labels.Canonical()}
		h.series[id] = buf
	}
	buf.timestamps = append(buf.timestamps, timestamp)
	buf.values = append(buf.values, value)
	return id
}

func (h *Head) Snapshot() []block.Series {
	h.mu.RLock()
	defer h.mu.RUnlock()

	ids := make([]int, 0, len(h.series))
	for id := range h.series {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)

	out := make([]block.Series, 0, len(ids))
	for _, rawID := range ids {
		id := index.SeriesID(rawID)
		buf := h.series[id]
		timestamps, values := sortedSamples(buf.timestamps, buf.values)
		out = append(out, block.Series{
			ID:         uint64(id),
			Type:       buf.typ,
			Labels:     append(model.LabelSet(nil), buf.labels...),
			Timestamps: timestamps,
			Values:     values,
		})
	}
	return out
}

func (h *Head) Len() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.series)
}

func (h *Head) SampleCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := 0
	for _, series := range h.series {
		count += len(series.values)
	}
	return count
}

func (h *Head) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.series = make(map[index.SeriesID]*bufferedSeries)
}

type sample struct {
	timestamp int64
	value     int64
}

func sortedSamples(timestamps []int64, values []int64) ([]int64, []int64) {
	samples := make([]sample, len(timestamps))
	for i := range timestamps {
		samples[i] = sample{timestamp: timestamps[i], value: values[i]}
	}
	sort.SliceStable(samples, func(i, j int) bool {
		return samples[i].timestamp < samples[j].timestamp
	})
	sortedTimestamps := make([]int64, len(samples))
	sortedValues := make([]int64, len(samples))
	for i, sample := range samples {
		sortedTimestamps[i] = sample.timestamp
		sortedValues[i] = sample.value
	}
	return sortedTimestamps, sortedValues
}
