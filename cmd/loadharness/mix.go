package main

import (
	"fmt"
	"strconv"
	"strings"
)

// metricKind enumerates the OTLP shapes the harness can emit. Histograms here
// means ExponentialHistogram; explicit-bucket histograms are accepted by
// amber but the exp variant is more interesting for the codec.
type metricKind int

const (
	kindCounter metricKind = iota
	kindGauge
	kindExpHistogram
)

func (k metricKind) String() string {
	switch k {
	case kindCounter:
		return "counter"
	case kindGauge:
		return "gauge"
	case kindExpHistogram:
		return "exphist"
	}
	return "unknown"
}

// mix is a normalised distribution over metric kinds. Pick(u) maps a uniform
// u in [0,1) to a kind.
type mix struct {
	kinds   []metricKind
	cumProb []float64
}

func parseMix(spec string) (*mix, error) {
	parts := strings.Split(spec, ",")
	weights := make(map[metricKind]float64)
	total := 0.0
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		kv := strings.SplitN(p, ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("entry %q must be kind:weight", p)
		}
		w, err := strconv.ParseFloat(strings.TrimSpace(kv[1]), 64)
		if err != nil {
			return nil, fmt.Errorf("entry %q: %w", p, err)
		}
		if w < 0 {
			return nil, fmt.Errorf("entry %q: weight must be >= 0", p)
		}
		var k metricKind
		switch strings.TrimSpace(kv[0]) {
		case "counter":
			k = kindCounter
		case "gauge":
			k = kindGauge
		case "exphist", "exphistogram", "histogram":
			k = kindExpHistogram
		default:
			return nil, fmt.Errorf("unknown kind %q", kv[0])
		}
		weights[k] += w
		total += w
	}
	if total <= 0 {
		return nil, fmt.Errorf("total weight must be > 0")
	}

	m := &mix{}
	cum := 0.0
	// Stable iteration order: counter, gauge, exphist.
	for _, k := range []metricKind{kindCounter, kindGauge, kindExpHistogram} {
		w, ok := weights[k]
		if !ok {
			continue
		}
		cum += w / total
		m.kinds = append(m.kinds, k)
		m.cumProb = append(m.cumProb, cum)
	}
	// Force last to exactly 1 to avoid FP edge case where u==0.9999... falls past.
	if len(m.cumProb) > 0 {
		m.cumProb[len(m.cumProb)-1] = 1.0
	}
	return m, nil
}

func (m *mix) Pick(u float64) metricKind {
	for i, c := range m.cumProb {
		if u < c {
			return m.kinds[i]
		}
	}
	return m.kinds[len(m.kinds)-1]
}

func (m *mix) String() string {
	var b strings.Builder
	prev := 0.0
	for i, k := range m.kinds {
		if i > 0 {
			b.WriteString(", ")
		}
		fmt.Fprintf(&b, "%s=%.0f%%", k, (m.cumProb[i]-prev)*100)
		prev = m.cumProb[i]
	}
	return b.String()
}
