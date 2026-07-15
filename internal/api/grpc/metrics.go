package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yaop-labs/amber/internal/otlpmetrics"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/metricsengine"
)

type metricsServer struct {
	collectormetrics.UnimplementedMetricsServiceServer
	store   *metricsengine.Store
	journal *otlpv4.Journal
	log     *slog.Logger
}

// Export implements the OTLP metrics collector service: it writes received
// points to the embedded metric store via the shared otlpmetrics path,
// reporting points that did not land through OTLP partial success.
func (s *metricsServer) Export(ctx context.Context, req *collectormetrics.ExportMetricsServiceRequest) (*collectormetrics.ExportMetricsServiceResponse, error) {
	res, err := otlpmetrics.Ingest(s.store, req, s.log)
	if s.journal != nil && res.AcceptedRequest != nil {
		if journalErr := s.journal.AppendRequest(otlpv4.SignalMetrics, res.AcceptedRequest, time.Now()); journalErr != nil {
			return nil, status.Error(codes.Unavailable, "metrics replay journal failed; retry request")
		}
	}
	if err != nil {
		return nil, status.Error(codes.Unavailable, "metrics durable ingest failed; retry request")
	}
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
