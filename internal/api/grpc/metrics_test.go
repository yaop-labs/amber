package grpc

import (
	"context"
	"io"
	"log/slog"
	"math"
	"net"
	"testing"
	"time"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/grpcreef"
)

func discardLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func openStore(t *testing.T) *metricsengine.Store {
	t.Helper()
	store, err := metricsengine.OpenStore(t.TempDir() + "/metrics")
	if err != nil {
		t.Fatalf("open metric store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func sumRequest(name string, t0, t1 uint64) *collectormetrics.ExportMetricsServiceRequest {
	return &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
				Key: "service.name", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}},
			}}},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: name,
					Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						IsMonotonic:            true,
						DataPoints: []*metricspb.NumberDataPoint{
							{TimeUnixNano: t0, Attributes: []*commonpb.KeyValue{{Key: "job", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}}}}, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 1}},
							{TimeUnixNano: t1, Attributes: []*commonpb.KeyValue{{Key: "job", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "api"}}}}, Value: &metricspb.NumberDataPoint_AsInt{AsInt: 61}},
						},
					}},
				}},
			}},
		}},
	}
}

// TestMetricsExportWritesToStore drives metricsServer.Export with a real store
// and asserts the points land (queryable) with no partial-success rejections.
func TestMetricsExportWritesToStore(t *testing.T) {
	store := openStore(t)
	s := &metricsServer{store: store, log: discardLog()}

	t0 := uint64(time.Now().Add(-time.Minute).UnixNano())
	t1 := uint64(time.Now().UnixNano())
	resp, err := s.Export(context.Background(), sumRequest("http_requests_total", t0, t1))
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if ps := resp.GetPartialSuccess(); ps.GetRejectedDataPoints() != 0 {
		t.Fatalf("rejected_data_points = %d, want 0", ps.GetRejectedDataPoints())
	}

	rs := metricsengine.RangeSelector{
		Selector: metricsengine.NewSelector(metricsengine.MetricName("http_requests_total")),
		Window:   2 * time.Minute,
	}
	rates, err := store.RateByLabelRange(rs, int64(t1)/1_000_000, "job")
	if err != nil {
		t.Fatalf("RateByLabelRange: %v", err)
	}
	if rates["api"] <= 0 {
		t.Fatalf("rates[api] = %v, want > 0", rates["api"])
	}
}

// TestMetricsExportUnsupportedReportsPartialSuccess asserts a metric with no
// recognized data shape is reported as a rejected data point.
func TestMetricsExportUnsupportedReportsPartialSuccess(t *testing.T) {
	s := &metricsServer{store: openStore(t), log: discardLog()}

	resp, err := s.Export(context.Background(), &collectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{Name: "shapeless"}}, // no Data set
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export() error = %v", err)
	}
	if got := resp.GetPartialSuccess().GetRejectedDataPoints(); got != 1 {
		t.Fatalf("rejected_data_points = %d, want 1", got)
	}
	if resp.GetPartialSuccess().GetErrorMessage() == "" {
		t.Fatal("expected partial success error message")
	}
}

func TestMetricsExportJournalExcludesRejectedAndUnsupportedPoints(t *testing.T) {
	root := t.TempDir()
	journal, err := otlpv4.OpenJournal(root, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	s := &metricsServer{store: openStore(t), journal: journal, log: discardLog()}
	accepted := &metricspb.NumberDataPoint{TimeUnixNano: 1_000_000, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.25}}
	rejected := &metricspb.NumberDataPoint{TimeUnixNano: 2_000_000, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: math.NaN()}}
	req := &collectormetrics.ExportMetricsServiceRequest{ResourceMetrics: []*metricspb.ResourceMetrics{{
		ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
			{Name: "temperature", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{DataPoints: []*metricspb.NumberDataPoint{accepted, rejected}}}},
			{Name: "unsupported", Data: &metricspb.Metric_Summary{Summary: &metricspb.Summary{DataPoints: []*metricspb.SummaryDataPoint{{TimeUnixNano: 3_000_000}}}}},
		}}},
	}}}
	response, err := s.Export(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if response.GetPartialSuccess().GetRejectedDataPoints() != 2 {
		t.Fatalf("partial success = %v", response.GetPartialSuccess())
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}
	want := otlpv4.MetricsSubset(req, map[proto.Message]struct{}{accepted: {}})
	count := 0
	if err := otlpv4.Replay(context.Background(), root, func(envelope otlpv4.Envelope) error {
		count++
		message, err := envelope.Request()
		if err != nil {
			return err
		}
		if !proto.Equal(message, want) {
			t.Fatalf("replayed metrics differ: %v", message)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("journal record count = %d", count)
	}
}

// TestNewServerRegistersMetricsOnlyWithStore validates the registration gating:
// MetricsService is served only when a store is supplied; otherwise a metrics
// client receives Unimplemented.
func TestNewServerRegistersMetricsOnlyWithStore(t *testing.T) {
	t.Run("nil store -> Unimplemented", func(t *testing.T) {
		_, code := dialAndExport(t, NewServer(fakeSender{}, nil, 0, discardLog()))
		if code != codes.Unimplemented {
			t.Fatalf("status code = %v, want Unimplemented", code)
		}
	})
	t.Run("store -> served", func(t *testing.T) {
		_, code := dialAndExport(t, NewServer(fakeSender{}, openStore(t), 0, discardLog()))
		if code != codes.OK {
			t.Fatalf("status code = %v, want OK", code)
		}
	})
}

func TestNewServerReefAuthGatesGRPC(t *testing.T) {
	serverOpts, err := grpcreef.ServerOptions(nil, &bearer.ServerConfig{
		Bearer: []bearer.Key{{Name: "collector", Token: "secret"}},
	})
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}
	srv := NewServer(fakeSender{}, openStore(t), 0, discardLog(), serverOpts...)

	if _, code := dialAndExport(t, srv); code != codes.Unauthenticated {
		t.Fatalf("status code without token = %v, want Unauthenticated", code)
	}
}

func TestNewServerReefAuthAcceptsBearer(t *testing.T) {
	serverOpts, err := grpcreef.ServerOptions(nil, &bearer.ServerConfig{
		Bearer: []bearer.Key{{Name: "collector", Token: "secret"}},
	})
	if err != nil {
		t.Fatalf("ServerOptions: %v", err)
	}
	dialOpts, err := grpcreef.DialOptions(nil, &bearer.ClientConfig{Token: "secret"})
	if err != nil {
		t.Fatalf("DialOptions: %v", err)
	}
	srv := NewServer(fakeSender{}, openStore(t), 0, discardLog(), serverOpts...)

	if _, code := dialAndExport(t, srv, dialOpts...); code != codes.OK {
		t.Fatalf("status code with token = %v, want OK", code)
	}
}

// dialAndExport serves srv on an ephemeral port, sends one empty metrics export,
// and returns the response and gRPC status code.
func dialAndExport(t *testing.T, srv *grpc.Server, dialOpts ...grpc.DialOption) (*collectormetrics.ExportMetricsServiceResponse, codes.Code) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	if len(dialOpts) == 0 {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	dialOpts = append([]grpc.DialOption{
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
	}, dialOpts...)
	conn, err := grpc.NewClient("passthrough:///bufnet", dialOpts...)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := collectormetrics.NewMetricsServiceClient(conn).Export(ctx, &collectormetrics.ExportMetricsServiceRequest{})
	return resp, status.Code(err)
}
