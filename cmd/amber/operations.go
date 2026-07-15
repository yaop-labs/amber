package main

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/config"
	"github.com/yaop-labs/amber/internal/otlpv4"
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

type otlpReplayMetrics struct {
	enabled         float64
	statsError      float64
	sealedSegments  float64
	activeSegment   float64
	activeRecords   float64
	totalRecords    float64
	segmentBytes    float64
	walBytes        float64
	storageBytes    float64
	walRepairEvents float64
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

func registerOTLPReplayMetrics(journal *otlpv4.Journal) func() otlpReplayMetrics {
	var (
		mu     sync.Mutex
		readAt time.Time
		cached otlpReplayMetrics
	)
	read := func() otlpReplayMetrics {
		mu.Lock()
		defer mu.Unlock()
		if !readAt.IsZero() && time.Since(readAt) < 100*time.Millisecond {
			return cached
		}
		readAt = time.Now()
		if journal == nil {
			cached = otlpReplayMetrics{}
			return cached
		}
		cached.enabled = 1
		stats, err := journal.Stats()
		if err != nil {
			cached.statsError = 1
			return cached
		}
		cached.statsError = 0
		cached.sealedSegments = float64(stats.SealedSegments)
		if stats.ActiveSegment {
			cached.activeSegment = 1
		} else {
			cached.activeSegment = 0
		}
		cached.activeRecords = float64(stats.ActiveRecords)
		cached.totalRecords = float64(stats.TotalRecords)
		cached.segmentBytes = float64(stats.SegmentBytes)
		cached.walBytes = float64(stats.WALBytes)
		cached.storageBytes = float64(stats.SegmentBytes + stats.WALBytes)
		cached.walRepairEvents = float64(stats.WALCorruptRecords)
		return cached
	}
	registerGauge := func(name, help string, value func(otlpReplayMetrics) float64) {
		selfobs.RegisterGaugeFunc(name, help, func() float64 { return value(read()) })
	}
	registerGauge("amber_otlp_replay_enabled", "1 when the canonical OTLP v4 replay journal is enabled.", func(metrics otlpReplayMetrics) float64 { return metrics.enabled })
	registerGauge("amber_otlp_replay_stats_error", "1 when replay-journal operational statistics cannot be read.", func(metrics otlpReplayMetrics) float64 { return metrics.statsError })
	registerGauge("amber_otlp_replay_sealed_segments", "Sealed segments in the canonical OTLP replay journal.", func(metrics otlpReplayMetrics) float64 { return metrics.sealedSegments })
	registerGauge("amber_otlp_replay_active_segment", "1 when the canonical OTLP replay journal has an active segment.", func(metrics otlpReplayMetrics) float64 { return metrics.activeSegment })
	registerGauge("amber_otlp_replay_active_records", "Replay envelopes in the active OTLP journal segment.", func(metrics otlpReplayMetrics) float64 { return metrics.activeRecords })
	registerGauge("amber_otlp_replay_records", "Replay envelopes retained across sealed and active OTLP journal segments.", func(metrics otlpReplayMetrics) float64 { return metrics.totalRecords })
	registerGauge("amber_otlp_replay_segment_bytes", "Physical bytes occupied by OTLP replay segment containers.", func(metrics otlpReplayMetrics) float64 { return metrics.segmentBytes })
	registerGauge("amber_otlp_replay_wal_bytes", "Physical bytes occupied by the active OTLP replay WAL.", func(metrics otlpReplayMetrics) float64 { return metrics.walBytes })
	registerGauge("amber_otlp_replay_storage_bytes", "Physical bytes occupied by OTLP replay segments and WAL, excluding small metadata files.", func(metrics otlpReplayMetrics) float64 { return metrics.storageBytes })
	selfobs.RegisterCounterFunc("amber_otlp_replay_wal_repair_events_total", "Malformed OTLP replay WAL tails repaired during the current process start.", func() float64 { return read().walRepairEvents })
	return read
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
