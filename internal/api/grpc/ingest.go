package grpc

import (
	"context"
	"fmt"
	"log/slog"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yaop-labs/amber/internal/ingest"
)

type logsServer struct {
	collectorlogs.UnimplementedLogsServiceServer
	batcher sender
	log     *slog.Logger
}

func (s *logsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	if s.batcher.IsBreakerOpen() {
		return nil, status.Error(codes.Unavailable, "ingest temporarily unavailable")
	}
	var rejected int64
	var firstErr error
	for _, rl := range req.ResourceLogs {
		service, host := ingest.ExtractResource(rl.Resource.GetAttributes())
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				entry, err := ingest.OTLPLogToEntry(lr, service, host)
				if err != nil {
					s.log.Debug("grpc: skip log record", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if err := s.batcher.SendLog(entry); err != nil {
					s.log.Debug("grpc: send log failed", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
				}
			}
		}
	}
	return logExportResponse(rejected, firstErr), nil
}

type tracesServer struct {
	collectortrace.UnimplementedTraceServiceServer
	batcher sender
	log     *slog.Logger
}

func (s *tracesServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	if s.batcher.IsBreakerOpen() {
		return nil, status.Error(codes.Unavailable, "ingest temporarily unavailable")
	}
	var rejected int64
	var firstErr error
	for _, rs := range req.ResourceSpans {
		service, _ := ingest.ExtractResource(rs.Resource.GetAttributes())
		for _, ss := range rs.ScopeSpans {
			for _, sp := range ss.Spans {
				entry, err := ingest.OTLPSpanToEntry(sp, service)
				if err != nil {
					s.log.Debug("grpc: skip span", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if err := s.batcher.SendSpan(entry); err != nil {
					s.log.Debug("grpc: send span failed", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
				}
			}
		}
	}
	return traceExportResponse(rejected, firstErr), nil
}

func logExportResponse(rejected int64, err error) *collectorlogs.ExportLogsServiceResponse {
	if rejected == 0 {
		return &collectorlogs.ExportLogsServiceResponse{}
	}
	return &collectorlogs.ExportLogsServiceResponse{
		PartialSuccess: &collectorlogs.ExportLogsPartialSuccess{
			RejectedLogRecords: rejected,
			ErrorMessage:       partialSuccessMessage("log record", rejected, err),
		},
	}
}

func traceExportResponse(rejected int64, err error) *collectortrace.ExportTraceServiceResponse {
	if rejected == 0 {
		return &collectortrace.ExportTraceServiceResponse{}
	}
	return &collectortrace.ExportTraceServiceResponse{
		PartialSuccess: &collectortrace.ExportTracePartialSuccess{
			RejectedSpans: rejected,
			ErrorMessage:  partialSuccessMessage("span", rejected, err),
		},
	}
}

func partialSuccessMessage(kind string, rejected int64, err error) string {
	if err == nil {
		return fmt.Sprintf("rejected %d %s(s)", rejected, kind)
	}
	return fmt.Sprintf("rejected %d %s(s): %v", rejected, kind, err)
}
