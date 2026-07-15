package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// DrillOptions defines one isolated restore drill. Workspace must not exist;
// Drill owns it after creation and removes it before returning.
type DrillOptions struct {
	Workspace          string
	ExpectedDatabaseID string
	Probe              DrillProbe
}

// DrillProbe performs semantic checks against a restored, offline data root.
type DrillProbe func(context.Context, string, Verification) (SemanticProbe, error)

// SemanticProbe is the machine-readable result of opening and querying a
// restored database.
type SemanticProbe struct {
	Ready             bool              `json:"ready"`
	Degraded          bool              `json:"degraded"`
	DatabaseID        string            `json:"database_id"`
	RestoreCheckpoint string            `json:"restore_checkpoint"`
	LogsReturned      int               `json:"logs_returned"`
	SpansReturned     int               `json:"spans_returned"`
	MetricNames       int               `json:"metric_names"`
	MetricQueryGroups int               `json:"metric_query_groups"`
	HistogramNames    int               `json:"histogram_names"`
	OTLPReplayRecords uint64            `json:"otlp_replay_records"`
	OTLPSignalRecords map[string]uint64 `json:"otlp_signal_records"`
	OTLPReplaySHA256  string            `json:"otlp_replay_sha256,omitempty"`
}

// DrillResult describes the authenticated snapshot and timings observed by a
// completed remote restore drill.
type DrillResult struct {
	Verification    Verification
	DataBytes       int64
	DownloadElapsed time.Duration
	RestoreElapsed  time.Duration
	ProbeElapsed    time.Duration
	TotalElapsed    time.Duration
	Probe           SemanticProbe
}

// Drill downloads an immutable S3 checkpoint, restores it under an isolated
// workspace, runs semantic probes, and removes every artifact it created.
func (t *S3Transport) Drill(ctx context.Context, checkpoint string, opts DrillOptions) (result DrillResult, retErr error) {
	started := time.Now()
	if err := validateCheckpointDigest(checkpoint); err != nil {
		return DrillResult{}, err
	}
	if opts.Workspace == "" {
		return DrillResult{}, errors.New("backup drill: workspace is required")
	}
	if opts.Probe == nil {
		return DrillResult{}, errors.New("backup drill: semantic probe is required")
	}
	if err := ctx.Err(); err != nil {
		return DrillResult{}, err
	}

	workspace, err := filepath.Abs(opts.Workspace)
	if err != nil {
		return DrillResult{}, fmt.Errorf("backup drill: resolve workspace: %w", err)
	}
	if _, err := os.Lstat(workspace); err == nil {
		return DrillResult{}, fmt.Errorf("backup drill: workspace already exists: %s", workspace)
	} else if !errors.Is(err, os.ErrNotExist) {
		return DrillResult{}, fmt.Errorf("backup drill: inspect workspace: %w", err)
	}
	parentInfo, err := os.Lstat(filepath.Dir(workspace))
	if err != nil {
		return DrillResult{}, fmt.Errorf("backup drill: inspect workspace parent: %w", err)
	}
	if !parentInfo.IsDir() {
		return DrillResult{}, errors.New("backup drill: workspace parent is not a directory")
	}
	if err := os.Mkdir(workspace, 0o700); err != nil {
		return DrillResult{}, fmt.Errorf("backup drill: create workspace: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(workspace); err != nil {
			retErr = errors.Join(retErr, fmt.Errorf("backup drill: remove workspace: %w", err))
		}
	}()

	snapshotDir := filepath.Join(workspace, "snapshot")
	restoredRoot := filepath.Join(workspace, "restored")
	phaseStarted := time.Now()
	verified, err := t.Download(ctx, checkpoint, snapshotDir)
	result.DownloadElapsed = time.Since(phaseStarted)
	if err != nil {
		return DrillResult{}, fmt.Errorf("backup drill: download: %w", err)
	}
	result.Verification = verified
	for _, file := range verified.Manifest.Files {
		result.DataBytes += file.Size
	}
	if opts.ExpectedDatabaseID != "" && verified.Manifest.Database.ID != opts.ExpectedDatabaseID {
		return DrillResult{}, fmt.Errorf("backup drill: database identity %q does not match expected %q", verified.Manifest.Database.ID, opts.ExpectedDatabaseID)
	}

	phaseStarted = time.Now()
	restored, err := Restore(ctx, snapshotDir, restoredRoot)
	result.RestoreElapsed = time.Since(phaseStarted)
	if err != nil {
		return DrillResult{}, fmt.Errorf("backup drill: restore: %w", err)
	}
	if restored.Checkpoint != verified.Checkpoint || restored.Manifest.Database.ID != verified.Manifest.Database.ID {
		return DrillResult{}, errors.New("backup drill: restored checkpoint identity changed")
	}

	phaseStarted = time.Now()
	probe, err := opts.Probe(ctx, restoredRoot, restored)
	result.ProbeElapsed = time.Since(phaseStarted)
	if err != nil {
		return DrillResult{}, fmt.Errorf("backup drill: semantic probe: %w", err)
	}
	if !probe.Ready {
		return DrillResult{}, errors.New("backup drill: probe reported the restored database is not ready")
	}
	if probe.Degraded {
		return DrillResult{}, errors.New("backup drill: probe reported the restored database is degraded")
	}
	if probe.DatabaseID != verified.Manifest.Database.ID {
		return DrillResult{}, errors.New("backup drill: probe reported a different database identity")
	}
	if probe.RestoreCheckpoint != verified.Checkpoint {
		return DrillResult{}, errors.New("backup drill: probe reported a different restore checkpoint")
	}
	result.Probe = probe
	result.TotalElapsed = time.Since(started)
	return result, nil
}
