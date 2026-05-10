// Package metrics owns Amber's Prometheus registry. Registry is private so
// callers cannot accidentally pollute it; live-state gauges are registered via
// callbacks at wire-up time.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var registry = prometheus.NewRegistry()

var (
	IngestAccepted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "amber_ingest_accepted_total",
		Help: "Entries successfully written to a segment.",
	}, []string{"kind"})

	IngestDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "amber_ingest_dropped_total",
		Help: "Entries dropped before reaching storage.",
	}, []string{"kind", "reason"})

	SealIndexErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "amber_seal_index_errors_total",
		Help: "Index builds that failed during segment seal, after retries.",
	}, []string{"kind", "index"})
)

func init() {
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		IngestAccepted,
		IngestDropped,
		SealIndexErrors,
	)
}

func RegisterGaugeFunc(name, help string, f func() float64) {
	registry.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
		Name: name,
		Help: help,
	}, f))
}

func RegisterCounterFunc(name, help string, f func() float64) {
	registry.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
		Name: name,
		Help: help,
	}, f))
}

func Handler() http.Handler {
	return promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
}
