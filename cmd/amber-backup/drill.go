package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/yaop-labs/amber"
	"github.com/yaop-labs/amber/internal/backup"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/metricsengine"
)

func runSemanticProbe(ctx context.Context, restoredRoot string, verified backup.Verification) (result backup.SemanticProbe, retErr error) {
	result.OTLPSignalRecords = map[string]uint64{
		otlpv4.SignalLogs.String():    0,
		otlpv4.SignalTraces.String():  0,
		otlpv4.SignalMetrics.String(): 0,
	}
	if verified.Manifest.Checkpoints.OTLPReplay.Present {
		digest := sha256.New()
		if err := otlpv4.Replay(ctx, restoredRoot, func(envelope otlpv4.Envelope) error {
			payload, err := envelope.MarshalBinary()
			if err != nil {
				return err
			}
			var length [8]byte
			binary.LittleEndian.PutUint64(length[:], uint64(len(payload)))
			if _, err := digest.Write(length[:]); err != nil {
				return fmt.Errorf("hash OTLP envelope length: %w", err)
			}
			if _, err := digest.Write(payload); err != nil {
				return fmt.Errorf("hash OTLP envelope: %w", err)
			}
			result.OTLPReplayRecords++
			result.OTLPSignalRecords[envelope.Signal().String()]++
			return nil
		}); err != nil {
			return backup.SemanticProbe{}, fmt.Errorf("replay OTLP v4 journal: %w", err)
		}
		result.OTLPReplaySHA256 = hex.EncodeToString(digest.Sum(nil))
	}

	db, err := amber.Open(restoredRoot, &amber.Options{BatchTimeout: time.Hour})
	if err != nil {
		return backup.SemanticProbe{}, fmt.Errorf("open restored database: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("close restored database: %w", err))
		}
	}()

	readyPoll := time.NewTicker(10 * time.Millisecond)
	defer readyPoll.Stop()
	for !db.IsReady() {
		select {
		case <-ctx.Done():
			return backup.SemanticProbe{}, ctx.Err()
		case <-readyPoll.C:
		}
	}

	status := db.Status()
	if status.Degraded {
		return backup.SemanticProbe{}, fmt.Errorf("restored database is degraded: %+v", status.Reasons)
	}
	if status.DatabaseID != verified.Manifest.Database.ID {
		return backup.SemanticProbe{}, errors.New("restored database identity does not match snapshot")
	}
	if status.LastVerifiedRestore == nil || status.LastVerifiedRestore.Checkpoint != verified.Checkpoint {
		return backup.SemanticProbe{}, errors.New("restored database does not report the verified checkpoint")
	}

	logs, err := db.QueryLogs(ctx, &amber.LogQuery{Limit: 1})
	if err != nil {
		return backup.SemanticProbe{}, fmt.Errorf("query restored logs: %w", err)
	}
	spans, err := db.QuerySpans(ctx, &amber.SpanQuery{Limit: 1})
	if err != nil {
		return backup.SemanticProbe{}, fmt.Errorf("query restored spans: %w", err)
	}
	metricStore := db.MetricStore()
	if metricStore == nil {
		return backup.SemanticProbe{}, errors.New("restored metrics store is unavailable")
	}

	if _, err := metricStore.Stats(); err != nil {
		return backup.SemanticProbe{}, fmt.Errorf("read restored metrics stats: %w", err)
	}
	metricNames := metricStore.MetricNames()
	if len(metricNames) > 0 {
		groups, err := metricStore.AggregateByLabel(
			metricsengine.NewSelector(metricsengine.MetricName(metricNames[0])),
			metricsengine.QueryOptions{},
			metricsengine.MetricNameLabel,
		)
		if err != nil {
			return backup.SemanticProbe{}, fmt.Errorf("query restored metrics: %w", err)
		}
		if len(groups) == 0 {
			return backup.SemanticProbe{}, errors.New("restored metric catalog entry has no queryable series")
		}
		result.MetricQueryGroups = len(groups)
	}
	histogramNames, err := metricStore.HistogramMetricNames()
	if err != nil {
		return backup.SemanticProbe{}, fmt.Errorf("read restored histogram catalog: %w", err)
	}
	if _, err := metricStore.HistStats(); err != nil {
		return backup.SemanticProbe{}, fmt.Errorf("read restored histogram stats: %w", err)
	}

	result.Ready = status.Ready
	result.Degraded = status.Degraded
	result.DatabaseID = status.DatabaseID
	result.RestoreCheckpoint = status.LastVerifiedRestore.Checkpoint
	result.LogsReturned = len(logs.Entries)
	result.SpansReturned = len(spans.Spans)
	result.MetricNames = len(metricNames)
	result.HistogramNames = len(histogramNames)
	return result, nil
}
