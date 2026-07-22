package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/internal/storage"
	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

func TestHTTPAuthMetricsPolicy(t *testing.T) {
	request := func(t *testing.T, metricsPublic bool, path, token string) int {
		t.Helper()
		cfg := config.Default().API
		cfg.APIKeys = []config.NamedAPIKey{{Name: "scraper", Key: "secret"}}
		cfg.MetricsPublic = metricsPublic
		middleware, err := newHTTPAuth(cfg)
		if err != nil {
			t.Fatalf("newHTTPAuth: %v", err)
		}
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		recorder := httptest.NewRecorder()
		middleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})).ServeHTTP(recorder, req)
		return recorder.Code
	}

	if got := request(t, true, "/metrics", ""); got != http.StatusNoContent {
		t.Fatalf("public /metrics status = %d, want %d", got, http.StatusNoContent)
	}
	if got := request(t, false, "/metrics", ""); got != http.StatusUnauthorized {
		t.Fatalf("protected /metrics status = %d, want %d", got, http.StatusUnauthorized)
	}
	if got := request(t, false, "/metrics", "secret"); got != http.StatusNoContent {
		t.Fatalf("authenticated /metrics status = %d, want %d", got, http.StatusNoContent)
	}
	if got := request(t, false, "/health", ""); got != http.StatusNoContent {
		t.Fatalf("health status = %d, want %d", got, http.StatusNoContent)
	}

	cfg := config.Default().API
	cfg.MetricsPublic = false
	if _, err := newHTTPAuth(cfg); err == nil {
		t.Fatal("protected /metrics without bearer auth was accepted")
	}
}

func TestReadCheckpointMetrics(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	if got := readCheckpointMetrics(runtime.Status{}, now); got != (checkpointMetrics{}) {
		t.Fatalf("empty checkpoint metrics = %+v", got)
	}

	backupCompleted := now.Add(-2 * time.Hour)
	restoreVerified := now.Add(-30 * time.Minute)
	got := readCheckpointMetrics(runtime.Status{
		LastSuccessfulBackup: &runtime.BackupCheckpointStatus{CompletedAt: backupCompleted},
		LastVerifiedRestore:  &runtime.RestoreCheckpointStatus{VerifiedAt: restoreVerified},
	}, now)
	if got.backupTimestamp != unixSeconds(backupCompleted) || got.backupAge != 2*time.Hour.Seconds() {
		t.Errorf("backup metrics = %+v", got)
	}
	if got.restoreTimestamp != unixSeconds(restoreVerified) || got.restoreAge != 30*time.Minute.Seconds() {
		t.Errorf("restore metrics = %+v", got)
	}

	future := readCheckpointMetrics(runtime.Status{
		LastSuccessfulBackup: &runtime.BackupCheckpointStatus{CompletedAt: now.Add(time.Minute)},
	}, now)
	if future.backupAge != 0 {
		t.Fatalf("clock-skewed backup age = %v, want 0", future.backupAge)
	}
}

func TestRegisterCheckpointMetrics(t *testing.T) {
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	status := runtime.Status{
		LastSuccessfulBackup: &runtime.BackupCheckpointStatus{CompletedAt: now.Add(-2 * time.Hour)},
		LastVerifiedRestore:  &runtime.RestoreCheckpointStatus{VerifiedAt: now.Add(-30 * time.Minute)},
	}
	registerCheckpointMetrics(func() runtime.Status { return status }, func() time.Time { return now })

	values := make(map[string]float64)
	for _, sample := range selfobs.Snapshot() {
		values[sample.Name] = sample.Value
	}
	want := map[string]float64{
		"amber_backup_last_success_timestamp_seconds":   unixSeconds(status.LastSuccessfulBackup.CompletedAt),
		"amber_backup_age_seconds":                      2 * time.Hour.Seconds(),
		"amber_restore_last_verified_timestamp_seconds": unixSeconds(status.LastVerifiedRestore.VerifiedAt),
		"amber_restore_verification_age_seconds":        30 * time.Minute.Seconds(),
	}
	for name, expected := range want {
		if got, ok := values[name]; !ok || got != expected {
			t.Errorf("%s = %v, present=%v; want %v", name, got, ok, expected)
		}
	}
}

func TestRegisterOTLPReplayMetrics(t *testing.T) {
	root := t.TempDir()
	journal, err := otlpv4.OpenJournal(root, storage.RotationPolicy{MaxRecords: 1, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := journal.Close(); err != nil {
			t.Errorf("close journal: %v", err)
		}
	}()
	if err := journal.AppendRequest(otlpv4.SignalLogs, &collectorlogs.ExportLogsServiceRequest{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := journal.AppendRequest(otlpv4.SignalLogs, &collectorlogs.ExportLogsServiceRequest{}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Prune(time.Now().UTC(), otlpv4.RetentionPolicy{MaxSegments: 1}); err != nil {
		t.Fatal(err)
	}

	read := registerOTLPReplayMetrics(journal)
	metrics := read()
	if metrics.enabled != 1 || metrics.statsError != 0 || metrics.sealedSegments != 1 ||
		metrics.activeSegment != 1 || metrics.activeRecords != 0 || metrics.totalRecords != 1 ||
		metrics.segmentBytes <= 0 || metrics.walBytes != 0 || metrics.storageBytes != metrics.segmentBytes ||
		metrics.walRepairEvents != 0 || metrics.retentionRuns != 1 || metrics.retentionFailures != 0 ||
		metrics.retentionDeletedSegments != 1 || metrics.retentionDeletedRecords != 1 || metrics.retentionDeletedBytes <= 0 ||
		metrics.retentionLastSuccess <= 0 || metrics.retentionOldestRetained <= 0 {
		t.Fatalf("OTLP replay metrics = %+v", metrics)
	}

	values := make(map[string]float64)
	for _, sample := range selfobs.Snapshot() {
		values[sample.Name] = sample.Value
	}
	want := map[string]float64{
		"amber_otlp_replay_enabled":                                  metrics.enabled,
		"amber_otlp_replay_stats_error":                              metrics.statsError,
		"amber_otlp_replay_sealed_segments":                          metrics.sealedSegments,
		"amber_otlp_replay_active_segment":                           metrics.activeSegment,
		"amber_otlp_replay_active_records":                           metrics.activeRecords,
		"amber_otlp_replay_records":                                  metrics.totalRecords,
		"amber_otlp_replay_segment_bytes":                            metrics.segmentBytes,
		"amber_otlp_replay_wal_bytes":                                metrics.walBytes,
		"amber_otlp_replay_storage_bytes":                            metrics.storageBytes,
		"amber_otlp_replay_wal_repair_events_total":                  metrics.walRepairEvents,
		"amber_otlp_replay_retention_runs_total":                     metrics.retentionRuns,
		"amber_otlp_replay_retention_failures_total":                 metrics.retentionFailures,
		"amber_otlp_replay_retention_deleted_segments_total":         metrics.retentionDeletedSegments,
		"amber_otlp_replay_retention_deleted_records_total":          metrics.retentionDeletedRecords,
		"amber_otlp_replay_retention_deleted_bytes_total":            metrics.retentionDeletedBytes,
		"amber_otlp_replay_retention_last_success_timestamp_seconds": metrics.retentionLastSuccess,
		"amber_otlp_replay_oldest_retained_timestamp_seconds":        metrics.retentionOldestRetained,
	}
	for name, expected := range want {
		if got, ok := values[name]; !ok || got != expected {
			t.Errorf("%s = %v, present=%v; want %v", name, got, ok, expected)
		}
	}
}
