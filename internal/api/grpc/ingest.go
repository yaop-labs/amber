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
	admit   func() error
	log     *slog.Logger
}

// Export implements the OTLP logs collector service: it converts the request's
// records and waits for their lane durability barrier. A successful response
// is a durable handoff; temporary admission/storage failures are retryable.
func (s *logsServer) Export(ctx context.Context, req *collectorlogs.ExportLogsServiceRequest) (*collectorlogs.ExportLogsServiceResponse, error) {
	if s.admit != nil {
		if err := s.admit(); err != nil {
			return nil, status.Error(codes.Unavailable, "ingest stopped by resource admission policy; retry request")
		}
	}
	if s.batcher.IsLogBreakerOpen() {
		return nil, status.Error(codes.Unavailable, "ingest temporarily unavailable")
	}
	var rejected int64
	var firstErr error
	var transient error
	var excluded map[*logspb.LogRecord]struct{}
	acceptedCount := 0
	allAccepted := true
	for _, rl := range req.ResourceLogs {
		if rl == nil {
			allAccepted = false
			continue
		}
		service, host := ingest.ExtractResource(rl.Resource.GetAttributes())
		for _, sl := range rl.ScopeLogs {
			if sl == nil {
				allAccepted = false
				continue
			}
			for _, lr := range sl.LogRecords {
				if lr == nil {
					allAccepted = false
					rejected++
					continue
				}
				entry, err := ingest.OTLPLogToEntry(lr, service, host)
				if err != nil {
					allAccepted = false
					if excluded == nil {
						excluded = make(map[*logspb.LogRecord]struct{})
					}
					excluded[lr] = struct{}{}
					s.log.Debug("grpc: skip log record", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if err := s.batcher.SendOTLPLog(entry); err != nil {
					allAccepted = false
					if excluded == nil {
						excluded = make(map[*logspb.LogRecord]struct{})
					}
					excluded[lr] = struct{}{}
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
				acceptedCount++
			}
		}
	}
	flushErr := s.batcher.FlushLogs(ctx)
	if flushErr != nil && transient == nil {
		transient = flushErr
	}
	if flushErr == nil && s.journal != nil && acceptedCount != 0 {
		journalRequest := req
		if !allAccepted {
			journalRequest = otlpv4.LogsSubsetExcluding(req, excluded)
		}
		if err := s.journal.AppendRequest(otlpv4.SignalLogs, journalRequest, time.Now()); err != nil {
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
	admit   func() error
	log     *slog.Logger
}

// Export implements the OTLP traces collector service, mirroring logsServer.Export.
func (s *tracesServer) Export(ctx context.Context, req *collectortrace.ExportTraceServiceRequest) (*collectortrace.ExportTraceServiceResponse, error) {
	if s.admit != nil {
		if err := s.admit(); err != nil {
			return nil, status.Error(codes.Unavailable, "ingest stopped by resource admission policy; retry request")
		}
	}
	if s.batcher.IsSpanBreakerOpen() {
		return nil, status.Error(codes.Unavailable, "ingest temporarily unavailable")
	}
	var rejected int64
	var firstErr error
	var transient error
	var excluded map[*tracepb.Span]struct{}
	acceptedCount := 0
	allAccepted := true
	for _, rs := range req.ResourceSpans {
		if rs == nil {
			allAccepted = false
			continue
		}
		service, _ := ingest.ExtractResource(rs.Resource.GetAttributes())
		for _, ss := range rs.ScopeSpans {
			if ss == nil {
				allAccepted = false
				continue
			}
			for _, sp := range ss.Spans {
				if sp == nil {
					allAccepted = false
					rejected++
					continue
				}
				entry, err := ingest.OTLPSpanToEntry(sp, service)
				if err != nil {
					allAccepted = false
					if excluded == nil {
						excluded = make(map[*tracepb.Span]struct{})
					}
					excluded[sp] = struct{}{}
					s.log.Debug("grpc: skip span", "err", err)
					rejected++
					if firstErr == nil {
						firstErr = err
					}
					continue
				}
				if err := s.batcher.SendOTLPSpan(entry); err != nil {
					allAccepted = false
					if excluded == nil {
						excluded = make(map[*tracepb.Span]struct{})
					}
					excluded[sp] = struct{}{}
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
				acceptedCount++
			}
		}
	}
	flushErr := s.batcher.FlushSpans(ctx)
	if flushErr != nil && transient == nil {
		transient = flushErr
	}
	if flushErr == nil && s.journal != nil && acceptedCount != 0 {
		journalRequest := req
		if !allAccepted {
			journalRequest = otlpv4.TracesSubsetExcluding(req, excluded)
		}
		if err := s.journal.AppendRequest(otlpv4.SignalTraces, journalRequest, time.Now()); err != nil {
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
