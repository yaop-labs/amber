// Command amber-backup creates, verifies, and restores offline Amber snapshots.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yaop-labs/amber/internal/backup"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type commandResult struct {
	Operation   string                `json:"operation"`
	Checkpoint  string                `json:"checkpoint"`
	DatabaseID  string                `json:"database_id"`
	CreatedAt   string                `json:"created_at"`
	Files       int                   `json:"files"`
	DataBytes   int64                 `json:"data_bytes,omitempty"`
	DurationsMS *drillDurations       `json:"durations_ms,omitempty"`
	Probe       *backup.SemanticProbe `json:"probe,omitempty"`
}

type drillDurations struct {
	Download int64 `json:"download"`
	Restore  int64 `json:"restore"`
	Probe    int64 `json:"probe"`
	Total    int64 `json:"total"`
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: amber-backup <create|verify|restore|s3-upload|s3-download|drill> [flags]")
	}
	operation := args[0]
	flags := flag.NewFlagSet(operation, flag.ContinueOnError)
	flags.SetOutput(stderr)
	var dataDir, snapshotDir, checkpoint, workDir, expectedDatabaseID string
	var drillTimeout time.Duration
	var s3Config backup.S3TransportConfig
	switch operation {
	case "create":
		flags.StringVar(&dataDir, "data-dir", "", "offline Amber data root")
		flags.StringVar(&snapshotDir, "snapshot", "", "new snapshot directory")
	case "verify":
		flags.StringVar(&snapshotDir, "snapshot", "", "snapshot directory")
	case "restore":
		flags.StringVar(&snapshotDir, "snapshot", "", "snapshot directory")
		flags.StringVar(&dataDir, "data-dir", "", "new Amber data root")
	case "s3-upload":
		flags.StringVar(&snapshotDir, "snapshot", "", "verified local snapshot directory")
		addS3Flags(flags, &s3Config)
	case "s3-download":
		flags.StringVar(&snapshotDir, "snapshot", "", "new local snapshot directory")
		flags.StringVar(&checkpoint, "checkpoint", "", "remote snapshot checkpoint")
		addS3Flags(flags, &s3Config)
	case "drill":
		flags.StringVar(&checkpoint, "checkpoint", "", "remote snapshot checkpoint")
		flags.StringVar(&workDir, "work-dir", "", "new isolated drill workspace")
		flags.StringVar(&expectedDatabaseID, "database-id", "", "expected database identity")
		flags.DurationVar(&drillTimeout, "timeout", 30*time.Minute, "maximum drill duration")
		addS3Flags(flags, &s3Config)
	default:
		return fmt.Errorf("unknown operation %q", operation)
	}
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("%s: unexpected positional arguments", operation)
	}
	if operation != "drill" && snapshotDir == "" {
		return fmt.Errorf("%s: -snapshot is required", operation)
	}
	switch operation {
	case "create", "restore":
		if dataDir == "" {
			return fmt.Errorf("%s: -data-dir is required", operation)
		}
	case "s3-upload", "s3-download", "drill":
		if s3Config.Bucket == "" {
			return fmt.Errorf("%s: -bucket is required", operation)
		}
	}
	if (operation == "s3-download" || operation == "drill") && checkpoint == "" {
		return fmt.Errorf("%s: -checkpoint is required", operation)
	}
	if operation == "drill" {
		if workDir == "" {
			return errors.New("drill: -work-dir is required")
		}
		if expectedDatabaseID == "" {
			return errors.New("drill: -database-id is required")
		}
		if drillTimeout <= 0 {
			return errors.New("drill: -timeout must be positive")
		}
	}

	var (
		verified backup.Verification
		drilled  *backup.DrillResult
		err      error
	)
	switch operation {
	case "create":
		verified, err = backup.Create(ctx, dataDir, snapshotDir)
	case "verify":
		verified, err = backup.Verify(ctx, snapshotDir)
	case "restore":
		verified, err = backup.Restore(ctx, snapshotDir, dataDir)
	case "s3-upload", "s3-download":
		var transport *backup.S3Transport
		transport, err = backup.NewS3Transport(ctx, s3Config)
		if err == nil && operation == "s3-upload" {
			verified, err = transport.Upload(ctx, snapshotDir)
		}
		if err == nil && operation == "s3-download" {
			verified, err = transport.Download(ctx, checkpoint, snapshotDir)
		}
	case "drill":
		var transport *backup.S3Transport
		transport, err = backup.NewS3Transport(ctx, s3Config)
		if err == nil {
			drillCtx, cancel := context.WithTimeout(ctx, drillTimeout)
			defer cancel()
			var drillResult backup.DrillResult
			drillResult, err = transport.Drill(drillCtx, checkpoint, backup.DrillOptions{
				Workspace:          workDir,
				ExpectedDatabaseID: expectedDatabaseID,
				Probe:              runSemanticProbe,
			})
			if err == nil {
				drilled = &drillResult
				verified = drillResult.Verification
			}
		}
	}
	if err != nil {
		return fmt.Errorf("%s: %w", operation, err)
	}
	result := commandResult{
		Operation:  operation,
		Checkpoint: verified.Checkpoint,
		DatabaseID: verified.Manifest.Database.ID,
		CreatedAt:  verified.Manifest.CreatedAt.Format("2006-01-02T15:04:05.999999999Z07:00"),
		Files:      len(verified.Manifest.Files),
	}
	if drilled != nil {
		result.DataBytes = drilled.DataBytes
		result.DurationsMS = &drillDurations{
			Download: drilled.DownloadElapsed.Milliseconds(),
			Restore:  drilled.RestoreElapsed.Milliseconds(),
			Probe:    drilled.ProbeElapsed.Milliseconds(),
			Total:    drilled.TotalElapsed.Milliseconds(),
		}
		result.Probe = &drilled.Probe
	}
	if err := json.NewEncoder(stdout).Encode(result); err != nil {
		return fmt.Errorf("%s: encode result: %w", operation, err)
	}
	return nil
}

func addS3Flags(flags *flag.FlagSet, cfg *backup.S3TransportConfig) {
	flags.StringVar(&cfg.Bucket, "bucket", "", "S3 bucket")
	flags.StringVar(&cfg.Prefix, "prefix", "", "S3 key prefix")
	flags.StringVar(&cfg.Region, "region", "", "AWS region")
	flags.StringVar(&cfg.Endpoint, "endpoint", "", "S3-compatible endpoint")
}
