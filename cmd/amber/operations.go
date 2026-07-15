package main

import (
	"errors"
	"net/http"
	"time"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/reef/bearer"
)

func newHTTPAuth(cfg config.APIConfig) (func(http.Handler) http.Handler, error) {
	exempt := []string{"/health", "/readyz"}
	if cfg.MetricsPublic {
		exempt = append(exempt, "/metrics")
	} else if auth := cfg.ResolvedBearerConfig(); auth == nil || len(auth.Bearer) == 0 {
		return nil, errors.New("configure protected /metrics: bearer authentication is required")
	}
	return bearer.Require(cfg.ResolvedBearerConfig(), bearer.ExemptPaths(exempt...))
}

type checkpointMetrics struct {
	backupTimestamp  float64
	backupAge        float64
	restoreTimestamp float64
	restoreAge       float64
}

func readCheckpointMetrics(status runtime.Status, now time.Time) checkpointMetrics {
	var metrics checkpointMetrics
	if checkpoint := status.LastSuccessfulBackup; checkpoint != nil {
		metrics.backupTimestamp = unixSeconds(checkpoint.CompletedAt)
		metrics.backupAge = nonNegativeAge(now, checkpoint.CompletedAt)
	}
	if checkpoint := status.LastVerifiedRestore; checkpoint != nil {
		metrics.restoreTimestamp = unixSeconds(checkpoint.VerifiedAt)
		metrics.restoreAge = nonNegativeAge(now, checkpoint.VerifiedAt)
	}
	return metrics
}

func registerCheckpointMetrics(status func() runtime.Status, now func() time.Time) {
	read := func(selectValue func(checkpointMetrics) float64) float64 {
		return selectValue(readCheckpointMetrics(status(), now()))
	}
	selfobs.RegisterGaugeFunc("amber_backup_last_success_timestamp_seconds", "Unix timestamp of the last completely published backup, or 0 if none is recorded.", func() float64 {
		return read(func(metrics checkpointMetrics) float64 { return metrics.backupTimestamp })
	})
	selfobs.RegisterGaugeFunc("amber_backup_age_seconds", "Seconds since the last completely published backup, or 0 if none is recorded.", func() float64 {
		return read(func(metrics checkpointMetrics) float64 { return metrics.backupAge })
	})
	selfobs.RegisterGaugeFunc("amber_restore_last_verified_timestamp_seconds", "Unix timestamp of the last verified restore, or 0 if none is recorded.", func() float64 {
		return read(func(metrics checkpointMetrics) float64 { return metrics.restoreTimestamp })
	})
	selfobs.RegisterGaugeFunc("amber_restore_verification_age_seconds", "Seconds since the last verified restore, or 0 if none is recorded.", func() float64 {
		return read(func(metrics checkpointMetrics) float64 { return metrics.restoreAge })
	})
}

func unixSeconds(value time.Time) float64 {
	return float64(value.UnixNano()) / float64(time.Second)
}

func nonNegativeAge(now, value time.Time) float64 {
	age := now.Sub(value).Seconds()
	if age < 0 {
		return 0
	}
	return age
}
