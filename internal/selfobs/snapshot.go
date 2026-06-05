package selfobs

import (
	"math"
	"sort"
	"strings"
)

// Sample is a single observation extracted from the in-process selfobs
// registry, in the shape the embedded metricsengine consumes.
//
// Type is "counter" for monotonic counters (including func counters) and
// "gauge" for func gauges. Histograms are flattened to two synthetic series:
// "<name>_count" (counter) and "<name>_sum" (gauge). Per-bucket _bucket series
// are intentionally dropped — metricsengine v0 has no histogram query path,
// so writing 13 extra series per histogram per scrape would just bloat the
// WAL.
type Sample struct {
	Name   string
	Type   string // "counter" | "gauge"
	Labels []SampleLabel
	Value  float64
}

type SampleLabel struct {
	Name, Value string
}

// Snapshot walks every registered metric and returns a flat list of samples.
// Used by the in-process dogfood scraper to feed amber's own observability
// into its embedded metric store. Safe to call concurrently with metric
// updates — counter/gauge reads are atomic, vec children iteration is under
// RLock.
func Snapshot() []Sample {
	regMu.RLock()
	cvs := append([]*CounterVec(nil), counterVecs...)
	hvs := append([]*HistogramVec(nil), histogramVecs...)
	fms := append([]funcMetric(nil), funcMetrics...)
	regMu.RUnlock()

	capacity := len(fms)
	for _, cv := range cvs {
		cv.mu.RLock()
		capacity += len(cv.children)
		cv.mu.RUnlock()
	}
	for _, hv := range hvs {
		hv.mu.RLock()
		capacity += 2 * len(hv.children)
		hv.mu.RUnlock()
	}
	out := make([]Sample, 0, capacity)
	for _, cv := range cvs {
		out = appendCounterVec(out, cv)
	}
	for _, hv := range hvs {
		out = appendHistogramVec(out, hv)
	}
	for _, fm := range fms {
		out = append(out, Sample{
			Name:  fm.name,
			Type:  fm.typ,
			Value: fm.fn(),
		})
	}
	return out
}

func appendCounterVec(out []Sample, cv *CounterVec) []Sample {
	cv.mu.RLock()
	defer cv.mu.RUnlock()
	keys := make([]string, 0, len(cv.children))
	for k := range cv.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		out = append(out, Sample{
			Name:   cv.name,
			Type:   "counter",
			Labels: decodeLabels(cv.labels, k),
			Value:  float64(cv.children[k].Get()),
		})
	}
	return out
}

func appendHistogramVec(out []Sample, hv *HistogramVec) []Sample {
	hv.mu.RLock()
	defer hv.mu.RUnlock()
	keys := make([]string, 0, len(hv.children))
	for k := range hv.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		h := hv.children[k]
		labels := decodeLabels(hv.labels, k)
		out = append(out,
			Sample{
				Name:   hv.name + "_count",
				Type:   "counter",
				Labels: labels,
				Value:  float64(h.count.Load()),
			},
			Sample{
				Name:   hv.name + "_sum",
				Type:   "gauge",
				Labels: labels,
				Value:  math.Float64frombits(h.sumBits.Load()),
			},
		)
	}
	return out
}

func decodeLabels(names []string, key string) []SampleLabel {
	if len(names) == 0 {
		return nil
	}
	values := strings.Split(key, "\x00")
	if len(values) != len(names) {
		return nil
	}
	out := make([]SampleLabel, len(names))
	for i, name := range names {
		out[i] = SampleLabel{Name: name, Value: values[i]}
	}
	return out
}
