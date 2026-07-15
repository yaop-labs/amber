package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRunCreateVerifyRestore(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatalf("mkdir source: %v", err)
	}
	if err := os.WriteFile(filepath.Join(source, "payload"), []byte("data"), 0o600); err != nil {
		t.Fatalf("write source: %v", err)
	}
	snapshot := filepath.Join(root, "snapshot")
	restored := filepath.Join(root, "restored")

	create := runCommand(t, "create", "-data-dir", source, "-snapshot", snapshot)
	verify := runCommand(t, "verify", "-snapshot", snapshot)
	restore := runCommand(t, "restore", "-snapshot", snapshot, "-data-dir", restored)
	if create.Checkpoint == "" || create.Checkpoint != verify.Checkpoint || create.Checkpoint != restore.Checkpoint {
		t.Fatalf("checkpoint mismatch: create=%q verify=%q restore=%q", create.Checkpoint, verify.Checkpoint, restore.Checkpoint)
	}
	if create.DatabaseID != verify.DatabaseID || create.DatabaseID != restore.DatabaseID {
		t.Fatalf("database identity mismatch: create=%q verify=%q restore=%q", create.DatabaseID, verify.DatabaseID, restore.DatabaseID)
	}
	if payload, err := os.ReadFile(filepath.Join(restored, "payload")); err != nil || string(payload) != "data" {
		t.Fatalf("restored payload = %q, err=%v", payload, err)
	}
}

func TestRunRequiresFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"create"}, &stdout, &stderr); err == nil {
		t.Fatal("create accepted missing flags")
	}
	if err := run(context.Background(), []string{"unknown"}, &stdout, &stderr); err == nil {
		t.Fatal("accepted unknown operation")
	}
	if err := run(context.Background(), []string{"s3-upload", "-snapshot", "somewhere"}, &stdout, &stderr); err == nil {
		t.Fatal("s3-upload accepted missing bucket")
	}
	if err := run(context.Background(), []string{"s3-download", "-snapshot", "somewhere", "-bucket", "backups"}, &stdout, &stderr); err == nil {
		t.Fatal("s3-download accepted missing checkpoint")
	}
}

func runCommand(t *testing.T, args ...string) commandResult {
	t.Helper()
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), args, &stdout, &stderr); err != nil {
		t.Fatalf("run %v: %v (stderr=%q)", args, err, stderr.String())
	}
	var result commandResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatalf("decode %v result: %v", args, err)
	}
	return result
}
