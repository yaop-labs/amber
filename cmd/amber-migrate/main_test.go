package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	amber "github.com/yaop-labs/amber"
	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/metricsengine"
)

func TestRunRejectsInvalidArguments(t *testing.T) {
	if err := run(context.Background(), []string{"otlp-v4"}, &bytes.Buffer{}); err == nil {
		t.Fatal("run() error = nil")
	}
}

func TestRunMigratesEmptyLegacyRoot(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatal(err)
	}
	identity, err := dbmeta.LoadOrCreate(source)
	if err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "target")
	var output bytes.Buffer
	if err := run(context.Background(), []string{"otlp-v4", source, target}, &output); err != nil {
		t.Fatal(err)
	}
	var result otlpv4.MigrationResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.DatabaseID != identity.ID || result.TargetRoot != target || result.RecordCount != 0 || len(result.Digest) != 64 {
		t.Fatalf("result = %+v", result)
	}
	if err := otlpv4.Replay(context.Background(), target, func(otlpv4.Envelope) error { return nil }); err != nil {
		t.Fatalf("replay migrated target: %v", err)
	}
}

func TestRunMigratesNonEmptyV3FixtureAndPreservesRollbackRoot(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	db, err := amber.Open(source, &amber.Options{SegmentMaxRecords: 1})
	if err != nil {
		t.Fatal(err)
	}
	traceID := amber.TraceID{1, 2, 3}
	spanID := amber.SpanID{4, 5, 6}
	logEntry, err := amber.NewLogEntry(amber.LevelWarn, "checkout", "host-a", "payment retry",
		amber.Attr{Key: "region", Value: "eu"})
	if err != nil {
		t.Fatal(err)
	}
	logEntry.TraceID = traceID
	logEntry.SpanID = spanID
	spanEntry, err := amber.NewSpanEntry(traceID, spanID, amber.SpanID{}, "checkout", "POST /pay")
	if err != nil {
		t.Fatal(err)
	}
	spanEntry.EndTime = spanEntry.StartTime.Add(25 * time.Millisecond)
	if err := db.Log(context.Background(), logEntry); err != nil {
		t.Fatal(err)
	}
	if err := db.Span(context.Background(), spanEntry); err != nil {
		t.Fatal(err)
	}
	if err := db.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := db.MetricStore().Append(metricsengine.LabelSet{
		{Name: "__name__", Value: "checkout_requests_total"},
		{Name: "service", Value: "checkout"},
	}, metricsengine.MetricTypeCounter, 1_000, 7); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// This fixture models a clean v0.3 root: it contains non-empty projections
	// for every signal but predates the AOT4 journal.
	if err := os.RemoveAll(filepath.Join(source, otlpv4.DirectoryName)); err != nil {
		t.Fatal(err)
	}
	before, err := treeDigest(source)
	if err != nil {
		t.Fatal(err)
	}

	target := filepath.Join(parent, "target")
	var output bytes.Buffer
	if err := run(context.Background(), []string{"otlp-v4", source, target}, &output); err != nil {
		t.Fatal(err)
	}
	var result otlpv4.MigrationResult
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.RecordCount != 3 || len(result.Digest) != 64 {
		t.Fatalf("migration result = %+v", result)
	}
	after, err := treeDigest(source)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("rollback source changed: before=%x after=%x", before, after)
	}

	migrated, err := amber.Open(target, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = migrated.Close() })
	deadline := time.Now().Add(5 * time.Second)
	for !migrated.IsReady() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	logs, err := migrated.QueryLogs(context.Background(), &amber.LogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	spans, err := migrated.QuerySpans(context.Background(), &amber.SpanQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	names := migrated.MetricStore().MetricNames()
	if len(logs.Entries) != 1 || logs.Entries[0].Body != "payment retry" {
		t.Fatalf("migrated logs = %+v", logs.Entries)
	}
	if len(spans.Spans) != 1 || spans.Spans[0].Operation != "POST /pay" {
		t.Fatalf("migrated spans = %+v", spans.Spans)
	}
	if !slices.Contains(names, "checkout_requests_total") {
		t.Fatalf("migrated metric names = %v", names)
	}
}

func treeDigest(root string) ([32]byte, error) {
	hash := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("non-regular fixture path %s", path)
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if _, err := io.WriteString(hash, relative+"\x00"); err != nil {
			return err
		}
		if _, err := hash.Write(payload); err != nil {
			return err
		}
		return nil
	})
	var digest [32]byte
	copy(digest[:], hash.Sum(nil))
	return digest, err
}
