package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/internal/selfobs"
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
