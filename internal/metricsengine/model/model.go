package model

import (
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
)

type MetricType uint8

const MetricNameLabel = "__name__"

const (
	MetricTypeGauge MetricType = iota + 1
	MetricTypeCounter
	MetricTypeHistogram
	MetricTypeExponentialHistogram
)

func (t MetricType) String() string {
	switch t {
	case MetricTypeGauge:
		return "gauge"
	case MetricTypeCounter:
		return "counter"
	case MetricTypeHistogram:
		return "histogram"
	case MetricTypeExponentialHistogram:
		return "exponential_histogram"
	default:
		return "unknown"
	}
}

type Label struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Sample struct {
	Labels    LabelSet   `json:"labels"`
	Type      MetricType `json:"type"`
	Timestamp int64      `json:"timestamp"`
	Value     int64      `json:"value"`
}

type LabelSet []Label

func (ls LabelSet) Canonical() LabelSet {
	out := append(LabelSet(nil), ls...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			return out[i].Value < out[j].Value
		}
		return out[i].Name < out[j].Name
	})
	return out
}

func (ls LabelSet) Equal(other LabelSet) bool {
	a := ls.Canonical()
	b := other.Canonical()
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (ls LabelSet) Fingerprint() uint64 {
	h := fnv.New64a()
	for _, label := range ls.Canonical() {
		h.Write([]byte(label.Name))
		h.Write([]byte{0})
		h.Write([]byte(label.Value))
		h.Write([]byte{0xff})
	}
	return h.Sum64()
}

func (ls LabelSet) Key() string {
	var b strings.Builder
	for i, label := range ls.Canonical() {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.Quote(label.Name))
		b.WriteByte('=')
		b.WriteString(strconv.Quote(label.Value))
	}
	return b.String()
}

func (ls LabelSet) Get(name string) (string, bool) {
	for _, label := range ls {
		if label.Name == name {
			return label.Value, true
		}
	}
	return "", false
}
