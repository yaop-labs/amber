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
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/metricsengine"
)

type sender interface {
	SendLog(model.LogEntry) error
	SendSpan(model.SpanEntry) error
	SendOTLPLog(model.LogEntry) error
	SendOTLPSpan(model.SpanEntry) error
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
	return newServer(batcher, metricStore, nil, nil, maxRecvBytes, log, opts...)
}

// NewServerWithJournal is NewServer with lossless accepted-OTLP recording.
func NewServerWithJournal(batcher sender, metricStore *metricsengine.Store, journal *otlpv4.Journal, maxRecvBytes int, log *slog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	return newServer(batcher, metricStore, journal, nil, maxRecvBytes, log, opts...)
}

// NewServerWithJournalAndAdmission adds a fail-closed resource admission
// decision shared by all three OTLP services.
func NewServerWithJournalAndAdmission(batcher sender, metricStore *metricsengine.Store, journal *otlpv4.Journal, admit func() error, maxRecvBytes int, log *slog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	return newServer(batcher, metricStore, journal, admit, maxRecvBytes, log, opts...)
}

func newServer(batcher sender, metricStore *metricsengine.Store, journal *otlpv4.Journal, admit func() error, maxRecvBytes int, log *slog.Logger, opts ...grpc.ServerOption) *grpc.Server {
	if maxRecvBytes > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(maxRecvBytes))
	}
	s := grpc.NewServer(opts...)
	collectorlogs.RegisterLogsServiceServer(s, &logsServer{batcher: batcher, journal: journal, admit: admit, log: log})
	collectortrace.RegisterTraceServiceServer(s, &tracesServer{batcher: batcher, journal: journal, admit: admit, log: log})
	if metricStore != nil {
		collectormetrics.RegisterMetricsServiceServer(s, &metricsServer{store: metricStore, journal: journal, admit: admit, log: log})
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
