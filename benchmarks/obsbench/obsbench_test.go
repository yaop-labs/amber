package obsbench

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func genDataset(t *testing.T, count uint64, seed uint64) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ds.ndjson.zst")
	if err := Generate(path, GenConfig{Count: count, Seed: seed, RareTokenEvery: 100}, nil); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	return path
}

func TestDatasetRoundTripAndDeterminism(t *testing.T) {
	const n = 1000
	pathA := genDataset(t, n, 42)
	pathB := genDataset(t, n, 42)

	readAll := func(path string) []Record {
		r, err := OpenDataset(path)
		if err != nil {
			t.Fatalf("OpenDataset: %v", err)
		}
		defer r.Close()
		var out []Record
		for {
			rec, err := r.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			out = append(out, rec)
		}
		return out
	}

	a, b := readAll(pathA), readAll(pathB)
	if len(a) != n || len(b) != n {
		t.Fatalf("got %d/%d records, want %d", len(a), len(b), n)
	}
	rare := 0
	for i := range a {
		if a[i].Body != b[i].Body || a[i].Service != b[i].Service || a[i].Level != b[i].Level {
			t.Fatalf("record %d differs across same-seed runs", i)
		}
		if a[i].Seq != uint64(i+1) {
			t.Fatalf("seq not dense: record %d has seq %d", i, a[i].Seq)
		}
		if strings.Contains(a[i].Body, RareToken(42)) {
			rare++
		}
	}
	if rare != n/100 {
		t.Fatalf("rare token count = %d, want %d", rare, n/100)
	}
}

func TestIngestLossAccounting(t *testing.T) {
	const n = 1000
	path := genDataset(t, n, 7)

	// Server fails every 4th batch with 500: acked must reflect only 2xx
	// batches and the error map must name the status.
	var batchN atomic.Uint64
	var received atomic.Uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []map[string]any
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Errorf("decode batch: %v", err)
		}
		if batchN.Add(1)%4 == 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		received.Add(uint64(len(batch)))
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	res, err := Ingest(context.Background(), "test", TargetConfig{
		Kind: "amber", BaseURL: srv.URL, Protocol: "native",
	}, path, IngestOptions{Workers: 4, BatchSize: 100})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}

	if res.SentRecords != n {
		t.Fatalf("sent = %d, want %d", res.SentRecords, n)
	}
	if res.AckedRecords != received.Load() {
		t.Fatalf("acked = %d, server received %d", res.AckedRecords, received.Load())
	}
	if res.AckedRecords == n {
		t.Fatal("expected some failed batches")
	}
	if res.FailedBatches == 0 || res.Errors["status 500"] != res.FailedBatches {
		t.Fatalf("error accounting wrong: %+v", res)
	}
	if res.LatencyP50 <= 0 || res.LatencyP99 < res.LatencyP50 {
		t.Fatalf("latency percentiles wrong: p50=%v p99=%v", res.LatencyP50, res.LatencyP99)
	}
}

func TestIngestRateCap(t *testing.T) {
	const n = 500
	path := genDataset(t, n, 9)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	// 500 records at 1000 rec/s should take ~0.5s; assert >= 0.35s so the
	// limiter is provably engaged without making the test timing-fragile.
	start := time.Now()
	res, err := Ingest(context.Background(), "test", TargetConfig{
		Kind: "amber", BaseURL: srv.URL, Protocol: "native",
	}, path, IngestOptions{Workers: 4, BatchSize: 50, Rate: 1000})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if res.AckedRecords != n {
		t.Fatalf("acked = %d, want %d", res.AckedRecords, n)
	}
	if elapsed := time.Since(start); elapsed < 350*time.Millisecond {
		t.Fatalf("rate cap not engaged: %d records in %v at rate 1000", n, elapsed)
	}
}

func TestBuildersProduceParseablePayloads(t *testing.T) {
	g := NewGenerator(GenConfig{Seed: 3})
	batch := make([]Record, 10)
	for i := range batch {
		batch[i] = g.Next()
	}
	now := time.Now()

	t.Run("loki", func(t *testing.T) {
		_, ct, body, err := (lokiBuilder{}).Build(batch, now)
		if err != nil || ct != "application/json" {
			t.Fatalf("build: ct=%s err=%v", ct, err)
		}
		var payload struct {
			Streams []struct {
				Stream map[string]string `json:"stream"`
				Values [][2]string       `json:"values"`
			} `json:"streams"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		total := 0
		for _, s := range payload.Streams {
			total += len(s.Values)
		}
		if total != len(batch) {
			t.Fatalf("loki payload has %d values, want %d", total, len(batch))
		}
	})

	t.Run("victorialogs", func(t *testing.T) {
		path, _, body, err := (victoriaLogsBuilder{}).Build(batch, now)
		if err != nil {
			t.Fatalf("build: %v", err)
		}
		if !strings.Contains(path, "_stream_fields") {
			t.Fatalf("jsonline path missing stream fields: %s", path)
		}
		lines := strings.Count(strings.TrimSpace(string(body)), "\n") + 1
		if lines != len(batch) {
			t.Fatalf("jsonline has %d lines, want %d", lines, len(batch))
		}
	})

	t.Run("otlp", func(t *testing.T) {
		_, ct, body, err := (&otlpBuilder{path: "/v1/logs"}).Build(batch, now)
		if err != nil || ct != "application/x-protobuf" {
			t.Fatalf("build: ct=%s err=%v", ct, err)
		}
		req := &collectorlogs.ExportLogsServiceRequest{}
		if err := proto.Unmarshal(body, req); err != nil {
			t.Fatalf("proto unmarshal: %v", err)
		}
		total := 0
		for _, rl := range req.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				total += len(sl.LogRecords)
			}
		}
		if total != len(batch) {
			t.Fatalf("otlp payload has %d records, want %d", total, len(batch))
		}
	})
}
