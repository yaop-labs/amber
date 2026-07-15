package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/otlpv4"
)

func TestRunMigratesLegacyRoot(t *testing.T) {
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
	if result.DatabaseID != identity.ID || result.TargetRoot != target || len(result.Digest) != 64 {
		t.Fatalf("result = %+v", result)
	}
	if err := otlpv4.Replay(context.Background(), target, func(otlpv4.Envelope) error { return nil }); err != nil {
		t.Fatalf("replay migrated target: %v", err)
	}
}

func TestRunRejectsInvalidArguments(t *testing.T) {
	if err := run(context.Background(), []string{"otlp-v4"}, &bytes.Buffer{}); err == nil {
		t.Fatal("run() error = nil")
	}
}
