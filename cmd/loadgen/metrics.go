package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"time"
)

// OTLP JSON metric shapes. The subset amber actually consumes: Sum and Gauge
// (number data points) and ExponentialHistogram (sketch). Explicit-bucket
// Histogram is also supported server-side but the exp variant exercises the
// more interesting sketch codec, so that's what loadgen emits.

type otlpMetricVal struct {
	StringValue string   `json:"stringValue,omitempty"`
	IntValue    string   `json:"intValue,omitempty"`
	DoubleValue *float64 `json:"doubleValue,omitempty"`
}

type otlpMetricAttr struct {
	Key   string        `json:"key"`
	Value otlpMetricVal `json:"value"`
}

type otlpNumberPoint struct {
	Attributes   []otlpMetricAttr `json:"attributes,omitempty"`
	TimeUnixNano string           `json:"timeUnixNano"`
	AsInt        string           `json:"asInt,omitempty"`
	AsDouble     *float64         `json:"asDouble,omitempty"`
}

type otlpSum struct {
	DataPoints             []otlpNumberPoint `json:"dataPoints"`
	AggregationTemporality int               `json:"aggregationTemporality"`
	IsMonotonic            bool              `json:"isMonotonic"`
}

type otlpGauge struct {
	DataPoints []otlpNumberPoint `json:"dataPoints"`
}

type otlpMetric struct {
	Name                 string            `json:"name"`
	Sum                  *otlpSum          `json:"sum,omitempty"`
	Gauge                *otlpGauge        `json:"gauge,omitempty"`
	ExponentialHistogram *otlpExpHistogram `json:"exponentialHistogram,omitempty"`
}

type otlpExpBuckets struct {
	Offset       int32    `json:"offset"`
	BucketCounts []uint64 `json:"bucketCounts"`
}

type otlpExpHistogramPoint struct {
	Attributes   []otlpMetricAttr `json:"attributes,omitempty"`
	TimeUnixNano string           `json:"timeUnixNano"`
	Count        uint64           `json:"count"`
	Sum          *float64         `json:"sum,omitempty"`
	Scale        int32            `json:"scale"`
	ZeroCount    uint64           `json:"zeroCount,omitempty"`
	Positive     otlpExpBuckets   `json:"positive"`
}

type otlpExpHistogram struct {
	DataPoints             []otlpExpHistogramPoint `json:"dataPoints"`
	AggregationTemporality int                     `json:"aggregationTemporality"`
}

type otlpScopeMetrics struct {
	Metrics []otlpMetric `json:"metrics"`
}

type otlpResourceMetrics struct {
	Resource     map[string]any     `json:"resource"`
	ScopeMetrics []otlpScopeMetrics `json:"scopeMetrics"`
}

type otlpMetricsExportReq struct {
	ResourceMetrics []otlpResourceMetrics `json:"resourceMetrics"`
}

// sendOTLPMetrics emits a synthetic mix of counters and gauges across the same
// service/host taxonomy used by traces+logs. We batch all points into a single
// request keyed by service.name to mirror real collector behavior — and to
// stress the per-resource grouping path in the ingest handler.
func sendOTLPMetrics(client *http.Client, addr string, rng *rand.Rand, n int) error {
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statuses := []string{"200", "201", "400", "500"}

	now := time.Now()
	pointsByService := make(map[string][]otlpMetric)

	for i := 0; i < n; i++ {
		svc := services[rng.IntN(len(services))]
		host := hosts[rng.IntN(len(hosts))]
		ts := now.Add(-time.Duration(rng.IntN(300)) * time.Second)
		tsStr := fmt.Sprintf("%d", ts.UnixNano())

		// Cycle through four metric shapes: monotonic sum, two gauge variants,
		// and an exponential histogram. Explicit-bucket histograms are also
		// accepted by the server but the exp variant exercises the
		// interesting sketch codec.
		switch i % 4 {
		case 0:
			pointsByService[svc] = append(pointsByService[svc], otlpMetric{
				Name: "http_requests_total",
				Sum: &otlpSum{
					AggregationTemporality: 2,
					IsMonotonic:            true,
					DataPoints: []otlpNumberPoint{{
						TimeUnixNano: tsStr,
						AsInt:        fmt.Sprintf("%d", 1+rng.IntN(100)),
						Attributes: []otlpMetricAttr{
							{Key: "method", Value: otlpMetricVal{StringValue: methods[rng.IntN(len(methods))]}},
							{Key: "status", Value: otlpMetricVal{StringValue: statuses[rng.IntN(len(statuses))]}},
							{Key: "host", Value: otlpMetricVal{StringValue: host}},
						},
					}},
				},
			})
		case 1:
			latency := float64(10 + rng.IntN(990))
			pointsByService[svc] = append(pointsByService[svc], otlpMetric{
				Name: "http_request_duration_ms",
				Gauge: &otlpGauge{
					DataPoints: []otlpNumberPoint{{
						TimeUnixNano: tsStr,
						AsDouble:     &latency,
						Attributes: []otlpMetricAttr{
							{Key: "host", Value: otlpMetricVal{StringValue: host}},
						},
					}},
				},
			})
		case 2:
			ratio := float64(rng.IntN(100)) / 100.0
			pointsByService[svc] = append(pointsByService[svc], otlpMetric{
				Name: "cpu_usage_ratio",
				Gauge: &otlpGauge{
					DataPoints: []otlpNumberPoint{{
						TimeUnixNano: tsStr,
						AsDouble:     &ratio,
						Attributes: []otlpMetricAttr{
							{Key: "host", Value: otlpMetricVal{StringValue: host}},
						},
					}},
				},
			})
		case 3:
			// Exponential histogram with 8 positive buckets, weighted toward
			// the center — this gives the quantile path something meaningful
			// to estimate. Offset shifts the distribution so different hosts
			// land at slightly different latency centers.
			counts := []uint64{1, 2, 4, 8, 8, 4, 2, 1}
			sum := 0.42 * float64(rng.IntN(10)+1)
			pointsByService[svc] = append(pointsByService[svc], otlpMetric{
				Name: "rpc_latency_seconds",
				ExponentialHistogram: &otlpExpHistogram{
					AggregationTemporality: 2,
					DataPoints: []otlpExpHistogramPoint{{
						TimeUnixNano: tsStr,
						Count:        30,
						Sum:          &sum,
						Scale:        2,
						Positive: otlpExpBuckets{
							Offset:       int32(rng.IntN(4)),
							BucketCounts: counts,
						},
						Attributes: []otlpMetricAttr{
							{Key: "host", Value: otlpMetricVal{StringValue: host}},
						},
					}},
				},
			})
		}
	}

	var rms []otlpResourceMetrics
	for svc, points := range pointsByService {
		rms = append(rms, otlpResourceMetrics{
			Resource: map[string]any{
				"attributes": []otlpMetricAttr{
					{Key: "service.name", Value: otlpMetricVal{StringValue: svc}},
				},
			},
			ScopeMetrics: []otlpScopeMetrics{{Metrics: points}},
		})
	}

	body, err := json.Marshal(otlpMetricsExportReq{ResourceMetrics: rms})
	if err != nil {
		return err
	}
	resp, err := client.Post(addr+"/v1/metrics", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}
	return nil
}
