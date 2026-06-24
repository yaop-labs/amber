package obsbench

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	amberhttp "github.com/yaop-labs/amber/internal/api/http"
	"github.com/yaop-labs/amber/internal/runtime"
)

// startAmber boots a real amber stack behind an httptest server, so the
// builders are exercised against the actual ingest handlers - payload-format
// drift fails here instead of silently producing zero-record benchmarks.
func startAmber(t *testing.T) (*runtime.Stack, *httptest.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	stack, err := runtime.New(ctx, runtime.Options{
		DataDir: t.TempDir(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatalf("runtime.New: %v", err)
	}
	mux := http.NewServeMux()
	amberhttp.RegisterRoutes(mux, amberhttp.RoutesDeps{
		Batcher:     stack.Batcher,
		Executor:    stack.Executor,
		LogManager:  stack.LogManager,
		LogSparse:   stack.LogSparse,
		MetricStore: stack.MetricStore,
		IsReady:     stack.IsReady,
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, amberhttp.RoutesConfig{MaxRequestBytes: 64 << 20})
	srv := httptest.NewServer(mux)
	t.Cleanup(func() {
		srv.Close()
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		_ = stack.Close(closeCtx)
	})
	return stack, srv
}

func ingestIntoAmber(t *testing.T, protocol string) {
	t.Helper()
	const n = 2000
	path := genDataset(t, n, 11)
	stack, srv := startAmber(t)

	res, err := Ingest(context.Background(), "amber", TargetConfig{
		Kind: "amber", BaseURL: srv.URL, Protocol: protocol,
	}, path, IngestOptions{Workers: 4, BatchSize: 250})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.AckedRecords != n || res.FailedBatches != 0 {
		t.Fatalf("acked=%d failed=%d errors=%v, want %d/0", res.AckedRecords, res.FailedBatches, res.Errors, n)
	}

	// Durability barrier, then count durable records the way the verify
	// subcommand does (segment metadata, exact). TotalHits is unsuitable: it
	// is a lower bound by design - heap-threshold skips drop segments and
	// blocks that cannot reach the top-k result.
	if err := stack.Batcher.Flush(context.Background()); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	var total uint64
	for _, seg := range stack.LogManager.Segments() {
		total += seg.RecordCount
	}
	total += stack.LogManager.ActiveRecordCount()
	if total != n {
		t.Fatalf("queryable = %d, want %d (silent ingest loss)", total, n)
	}
}

func TestE2E_AmberNativeIngest(t *testing.T) {
	ingestIntoAmber(t, "native")
}

func TestE2E_AmberOTLPIngest(t *testing.T) {
	ingestIntoAmber(t, "otlp")
}
