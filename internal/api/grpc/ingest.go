package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/otlpv4"
)

type logsServer struct {
	collectorlogs.UnimplementedLogsServiceServer
	batcher sender
	journal *otlpv4.Journal
	log     *slog.Logger
}

// Export implements the OTLP logs collector service: it converts the request's
// records and waits for their lane durability barrier. A successful response
// is a durable handoff; temporary admission/storage failures are retryable.
func (s *logsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	if s.batcher.IsLogBreakerOpen() {
		return nil, status.Error(codes.Unavailable, "ingest temporarily unavailable")
	}
	var rejected int64
	var firstErr error
	var transient error
	accepted := make(map[*logspb.LogRecord]struct{})
	for _, rl := range req.ResourceLogs {
		if rl == nil {
			continue
		}
		service, host := ingest.ExtractResource(rl.Resource.GetAttributes())
		for _, sl := range rl.ScopeLogs {
			if sl == nil {
				continue
			}
			for _, lr := range sl.LogRecords {
				if lr == nil {
					rejected++
					continue
				}
				entry, err := ingest.OTLPLogToEntry(lr, service, host)
				if err != nil {
					s.log.Debug("grpc: skip log record", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if err := s.batcher.SendOTLPLog(entry); err != nil {
					s.log.Debug("grpc: send log failed", "err", err)
					if retryableIngestError(err) {
						transient = err
					} else {
						rejected++
						if firstErr == nil {
							firstErr = err
						}
					}
					continue
				}
				accepted[lr] = struct{}{}
			}
		}
	}
	flushErr := s.batcher.FlushLogs(ctx)
	if flushErr != nil && transient == nil {
		transient = flushErr
	}
	if flushErr == nil && s.journal != nil && len(accepted) != 0 {
		if err := s.journal.AppendRequest(otlpv4.SignalLogs, otlpv4.LogsSubset(req, accepted), time.Now()); err != nil {
			transient = err
		}
	}
	if transient != nil {
		if errors.Is(transient, context.Canceled) || errors.Is(transient, context.DeadlineExceeded) {
			return nil, status.FromContextError(transient).Err()
		}
		return nil, status.Error(codes.Unavailable, "durable ingest handoff failed; retry request")
	}
	return logExportResponse(rejected, firstErr), nil
}

type tracesServer struct {
	collectortrace.UnimplementedTraceServiceServer
	batcher sender
	journal *otlpv4.Journal
	log     *slog.Logger
}

// Export implements the OTLP traces collector service, mirroring logsServer.Export.
func (s *tracesServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	if s.batcher.IsSpanBreakerOpen() {
		return nil, status.Error(codes.Unavailable, "ingest temporarily unavailable")
	}
	var rejected int64
	var firstErr error
	var transient error
	accepted := make(map[*tracepb.Span]struct{})
	for _, rs := range req.ResourceSpans {
		if rs == nil {
			continue
		}
		service, _ := ingest.ExtractResource(rs.Resource.GetAttributes())
		for _, ss := range rs.ScopeSpans {
			if ss == nil {
				continue
			}
			for _, sp := range ss.Spans {
				if sp == nil {
					rejected++
					continue
				}
				entry, err := ingest.OTLPSpanToEntry(sp, service)
				if err != nil {
					s.log.Debug("grpc: skip span", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if err := s.batcher.SendOTLPSpan(entry); err != nil {
					s.log.Debug("grpc: send span failed", "err", err)
					if retryableIngestError(err) {
						transient = err
					} else {
						rejected++
						if firstErr == nil {
							firstErr = err
						}
					}
					continue
				}
				accepted[sp] = struct{}{}
			}
		}
	}
	flushErr := s.batcher.FlushSpans(ctx)
	if flushErr != nil && transient == nil {
		transient = flushErr
	}
	if flushErr == nil && s.journal != nil && len(accepted) != 0 {
		if err := s.journal.AppendRequest(otlpv4.SignalTraces, otlpv4.TracesSubset(req, accepted), time.Now()); err != nil {
			transient = err
		}
	}
	if transient != nil {
		if errors.Is(transient, context.Canceled) || errors.Is(transient, context.DeadlineExceeded) {
			return nil, status.FromContextError(transient).Err()
		}
		return nil, status.Error(codes.Unavailable, "durable ingest handoff failed; retry request")
	}
	return traceExportResponse(rejected, firstErr), nil
}

func retryableIngestError(err error) bool {
	return errors.Is(err, ingest.ErrQueueFull) || errors.Is(err, ingest.ErrBreakerOpen) || errors.Is(err, ingest.ErrClosed)
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
