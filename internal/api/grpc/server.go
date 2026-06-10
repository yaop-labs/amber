// Package grpc serves the OTLP gRPC ingestion endpoints.
package grpc

import (
	"log/slog"
	"net"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc"

	"github.com/yaop-labs/amber/internal/model"
)

type sender interface {
	SendLog(model.LogEntry) error
	SendSpan(model.SpanEntry) error
	IsBreakerOpen() bool
	IsLogBreakerOpen() bool
	IsSpanBreakerOpen() bool
}

func NewServer(batcher sender, maxRecvBytes int, log *slog.Logger) *grpc.Server {
	var opts []grpc.ServerOption
	if maxRecvBytes > 0 {
		opts = append(opts, grpc.MaxRecvMsgSize(maxRecvBytes))
	}
	s := grpc.NewServer(opts...)
	collectorlogs.RegisterLogsServiceServer(s, &logsServer{batcher: batcher, log: log})
	collectortrace.RegisterTraceServiceServer(s, &tracesServer{batcher: batcher, log: log})
	return s
}

func ListenAndServe(s *grpc.Server, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	return s.Serve(lis)
}
