package grpc

import (
	"context"
	"fmt"
	"log/slog"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/yaop-labs/amber/internal/otlpmetrics"
	"github.com/yaop-labs/amber/metricsengine"
)

type metricsServer struct {
	collectormetrics.UnimplementedMetricsServiceServer
	store *metricsengine.Store
	log   *slog.Logger
}

// Export implements the OTLP metrics collector service: it writes received
// points to the embedded metric store via the shared otlpmetrics path,
// reporting points that did not land through OTLP partial success.
func (s *metricsServer) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	res := otlpmetrics.Ingest(s.store, req, s.log)
	// Rejected and unsupported points both failed to land; a gRPC client has
	// only RejectedDataPoints to learn that, so report their sum.
	notStored := int64(res.Rejected + res.Unsupported)
	if notStored == 0 {
		return &collectormetrics.ExportMetricsServiceResponse{}, nil
	}
	return &collectormetrics.ExportMetricsServiceResponse{
		PartialSuccess: &collectormetrics.ExportMetricsPartialSuccess{
			RejectedDataPoints: notStored,
			ErrorMessage:       fmt.Sprintf("rejected %d data point(s)", notStored),
		},
	}, nil
}
