// Package grpc serves the OTLP gRPC ingestion endpoints.
package grpc

import (
	"context"
	"log/slog"
	"net"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/metricsengine"
)

type sender interface {
	SendLog(model.LogEntry) error
	SendSpan(model.SpanEntry) error
	IsBreakerOpen() bool
	IsLogBreakerOpen() bool
	IsSpanBreakerOpen() bool
	FlushLogs(context.Context) error
	FlushSpans(context.Context) error
}

// NewServer builds a gRPC server exposing the OTLP logs and traces collector
// services, writing received entries through batcher. When metricStore is
// non-nil it also registers the OTLP metrics service, writing points to the
// embedded metric store; a nil store leaves MetricsService unregistered so
// metric clients receive Unimplemented (metrics disabled).
func NewServer(batcher sender, metricStore *metricsengine.Store, maxRecvBytes int, log *slog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	if maxRecvBytes > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(maxRecvBytes))
	}
	s := grpc.NewServer(opts...)
	collectorlogs.RegisterLogsServiceServer(s, &logsServer{batcher: batcher, log: log})
	collectortrace.RegisterTraceServiceServer(s, &tracesServer{batcher: batcher, log: log})
	if metricStore != nil {
		collectormetrics.RegisterMetricsServiceServer(s, &metricsServer{store: metricStore, log: log})
	}
	return s
}

// ListenAndServe binds addr and serves until the server is stopped.
func ListenAndServe(s *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(lis)
}
