package otlpv4

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/yaop-labs/amber/internal/dbmeta"
)

func TestMigrateLegacyV3PublishesVerifiedSibling(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := dbmeta.LoadOrCreate(source); err != nil {
		t.Fatal(err)
	}
	writeLegacyLogAndSpan(t, source)
	writeLegacyMetrics(t, source)
	target := filepath.Join(parent, "target")

	result, err := MigrateLegacyV3(context.Background(), source, target)
	if err != nil {
		t.Fatalf("MigrateLegacyV3() error = %v", err)
	}
	if result.RecordCount != 5 || result.SignalCount["logs"] != 1 ||
		result.SignalCount["traces"] != 1 || result.SignalCount["metrics"] != 3 || len(result.Digest) != 64 {
		t.Fatalf("migration result = %+v", result)
	}
	if _, err := os.Lstat(filepath.Join(source, DirectoryName)); !os.IsNotExist(err) {
		t.Fatalf("source journal exists after migration: %v", err)
	}
	count := 0
	if err := Replay(context.Background(), target, func(envelope Envelope) error {
		count++
		if envelope.Fidelity() != FidelityNormalizedV3 {
			t.Fatalf("migrated fidelity = %v", envelope.Fidelity())
		}
		return nil
	}); err != nil {
		t.Fatalf("Replay(target) error = %v", err)
	}
	if count != 5 {
		t.Fatalf("target replay count = %d", count)
	}
	statePayload, err := readRegular(filepath.Join(target, MigrationFileName))
	if err != nil {
		t.Fatal(err)
	}
	var state migrationState
	if err := decodeStrictJSON(statePayload, &state); err != nil {
		t.Fatal(err)
	}
	if state.Phase != "complete" || state.Digest != result.Digest {
		t.Fatalf("migration state = %+v", state)
	}
}

func TestMigrateLegacyV3RecoversOwnedStaging(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatal(err)
	}
	identity, err := dbmeta.LoadOrCreate(source)
	if err != nil {
		t.Fatal(err)
	}
	writeLegacyLogAndSpan(t, source)
	target := filepath.Join(parent, "target")
	staging := filepath.Join(parent, ".target.otlp-v4-migrating")
	if err := os.Mkdir(staging, 0o750); err != nil {
		t.Fatal(err)
	}
	state := migrationState{
		Version: migrationFileVersion, Phase: "verifying", SourceRoot: source, TargetRoot: target,
		DatabaseID: identity.ID, SourceFormat: 3, TargetFormat: int(FormatVersion),
	}
	if err := saveMigrationState(staging, state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "torn.data"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}

	result, err := MigrateLegacyV3(context.Background(), source, target)
	if err != nil {
		t.Fatalf("MigrateLegacyV3() recovery error = %v", err)
	}
	if result.RecordCount != 2 {
		t.Fatalf("migration result = %+v", result)
	}
	if _, err := os.Lstat(staging); !os.IsNotExist(err) {
		t.Fatalf("staging remains after publish: %v", err)
	}
}

func TestMigrateLegacyV3RejectsUnownedStaging(t *testing.T) {
	parent := t.TempDir()
	source := filepath.Join(parent, "source")
	if err := os.Mkdir(source, 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := dbmeta.LoadOrCreate(source); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(parent, "target")
	staging := filepath.Join(parent, ".target.otlp-v4-migrating")
	if err := os.Mkdir(staging, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(staging, "foreign"), []byte("do not delete"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := MigrateLegacyV3(context.Background(), source, target); err == nil {
		t.Fatal("MigrateLegacyV3() error = nil for unowned staging")
	}
	if _, err := os.Stat(filepath.Join(staging, "foreign")); err != nil {
		t.Fatalf("unowned staging was modified: %v", err)
	}
}
