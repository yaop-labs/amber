// Package runtime owns the storage, index, query, and ingest stack shared by
// the standalone binary and the embedded API.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/amber/internal/bootstrap"
	"github.com/yaop-labs/amber/internal/dbmeta"
	"github.com/yaop-labs/amber/internal/fslock"
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/ingest"
	"github.com/yaop-labs/amber/internal/otlpv4"
	"github.com/yaop-labs/amber/internal/query"
	"github.com/yaop-labs/amber/internal/retention"
	"github.com/yaop-labs/amber/internal/storage"
	"github.com/yaop-labs/amber/metricsengine"
)

func joinS3Prefix(parts ...string) string {
	joined := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.Trim(part, "/")
		if part != "" {
			joined = append(joined, part)
		}
	}
	return strings.Join(joined, "/")
}

// Options is the full configuration for assembling a Stack.
type Options struct {
	DataDir        string
	Logger         *slog.Logger
	Storage        StorageOptions
	Ingest         IngestOptions
	Cardinality    CardinalityOptions
	Metrics        MetricsOptions
	Retention      RetentionOptions
	IndexCacheSize int
	// IndexBootstrapWorkers bounds concurrent sealed-segment sidecar builds.
	// One worker is the default to keep startup peak memory independent of CPU count.
	IndexBootstrapWorkers int
	// MemoryLimit sets the Go runtime soft memory limit in bytes
	// (debug.SetMemoryLimit). Process-wide and sticky: it overrides
	// GOMEMLIMIT and stays in effect after Close. Zero leaves the
	// runtime (or GOMEMLIMIT) setting untouched.
	MemoryLimit int64
}

// RetentionOptions configures log/span local-tier eviction and terminal
// retention. It belongs to the shared runtime so embedded and standalone
// deployments have the same database semantics.
type RetentionOptions struct {
	Logs     StreamRetentionOptions
	Spans    StreamRetentionOptions
	Interval time.Duration
}

// StreamRetentionOptions is the retention policy for one segment stream.
type StreamRetentionOptions struct {
	LocalMaxAge   time.Duration
	LocalMaxBytes int64
	MaxAge        time.Duration
	MaxTotalBytes int64
	MaxSegments   int
}

// Status is a point-in-time operational view of the database runtime.
// Degraded means requests remain correct through a documented fallback, while
// Ready reports whether new traffic may be served.
type Status struct {
	Ready                bool                     `json:"ready"`
	Degraded             bool                     `json:"degraded"`
	Closing              bool                     `json:"closing"`
	DatabaseID           string                   `json:"database_id"`
	WALRepairEvents      uint64                   `json:"wal_repair_events"`
	IndexBootstrapErrors uint64                   `json:"index_bootstrap_errors"`
	LastSuccessfulBackup *BackupCheckpointStatus  `json:"last_successful_backup,omitempty"`
	LastVerifiedRestore  *RestoreCheckpointStatus `json:"last_verified_restore,omitempty"`
	Reasons              []StatusReason           `json:"reasons,omitempty"`
}

type BackupCheckpointStatus struct {
	Checkpoint        string    `json:"checkpoint"`
	SnapshotCreatedAt time.Time `json:"snapshot_created_at"`
	CompletedAt       time.Time `json:"completed_at"`
}

type RestoreCheckpointStatus struct {
	Checkpoint        string    `json:"checkpoint"`
	SnapshotCreatedAt time.Time `json:"snapshot_created_at"`
	VerifiedAt        time.Time `json:"verified_at"`
}

type StatusReason struct {
	Code  string `json:"code"`
	Count uint64 `json:"count"`
}

func (o StreamRetentionOptions) enabled() bool {
	return o.LocalMaxAge > 0 || o.LocalMaxBytes > 0 || o.MaxAge > 0 || o.MaxTotalBytes > 0 || o.MaxSegments > 0
}

func (o StreamRetentionOptions) hasLocalTier() bool {
	return o.LocalMaxAge > 0 || o.LocalMaxBytes > 0
}

func (o StreamRetentionOptions) policy() retention.Policy {
	return retention.Policy{
		LocalMaxAge: o.LocalMaxAge, LocalMaxBytes: o.LocalMaxBytes,
		MaxAge: o.MaxAge, MaxTotalBytes: o.MaxTotalBytes, MaxSegments: o.MaxSegments,
	}
}

// MetricsOptions configures the embedded metrics store.
// Zero limits use the metricsengine defaults.
type MetricsOptions struct {
	Disabled            bool
	Dir                 string
	FlushInterval       time.Duration
	MaxBufferedSamples  int
	MaxActiveSeries     int
	MaxLabelsPerSeries  int
	Retention           time.Duration
	CompactionMinBlocks int
	// DogfoodInterval enables the in-process selfobs scraper.
	// Zero disables it.
	DogfoodInterval time.Duration
	// CacheBudget is the combined byte budget for the metric store's block
	// caches (directories + resident indexes). Zero derives it from
	// MemoryLimit/2 when a memory limit is set; otherwise the store
	// defaults apply.
	CacheBudget int64
}

// StorageOptions configures segment rotation and the optional S3 tier.
type StorageOptions struct {
	SegmentMaxRecords uint64
	SegmentMaxBytes   int64
	// S3Bucket enables S3-compatible storage for sealed segments.
	S3Bucket   string
	S3Prefix   string
	S3Region   string
	S3Endpoint string // empty = AWS, non-empty = MinIO/R2/etc.
	// S3ReconcileOnStart adopts sealed remote segments at startup.
	S3ReconcileOnStart bool
}

// IngestOptions configures the batcher, with optional per-lane overrides.
type IngestOptions struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
	Logs             IngestLaneOptions
	Spans            IngestLaneOptions
}

// IngestLaneOptions overrides ingest settings for one lane (logs or spans).
type IngestLaneOptions struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
}

// CardinalityOptions configures the ingest cardinality guard.
type CardinalityOptions struct {
	MaxAttrsPerEntry      int
	MaxAttrValueBytes     int
	MaxAttrKeysPerService int
	MaxServices           int
}

const (
	defaultSegmentMaxRecords uint64 = 100_000
	defaultSegmentMaxBytes   int64  = 128 << 20

	defaultBatchSize             = 1000
	defaultBatchTimeout          = 100 * time.Millisecond
	defaultQueueSize             = 10_000
	defaultCardinalityServices   = 10_000
	defaultIndexBootstrapWorkers = 1
)

func (o Options) withDefaults() Options {
	out := o
	if out.Storage.SegmentMaxRecords == 0 {
		out.Storage.SegmentMaxRecords = defaultSegmentMaxRecords
	}
	if out.Storage.SegmentMaxBytes == 0 {
		out.Storage.SegmentMaxBytes = defaultSegmentMaxBytes
	}
	if out.Ingest.BatchSize == 0 {
		out.Ingest.BatchSize = defaultBatchSize
	}
	if out.Ingest.BatchTimeout == 0 {
		out.Ingest.BatchTimeout = defaultBatchTimeout
	}
	if out.Ingest.QueueSize == 0 {
		out.Ingest.QueueSize = defaultQueueSize
	}
	if out.Logger == nil {
		out.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if out.Cardinality.MaxAttrKeysPerService > 0 && out.Cardinality.MaxServices == 0 {
		out.Cardinality.MaxServices = defaultCardinalityServices
	}
	if out.IndexBootstrapWorkers == 0 {
		out.IndexBootstrapWorkers = defaultIndexBootstrapWorkers
	}
	return out
}

// Stack is the assembled runtime: storage managers, sparse indexes, the query
// executor, the ingest batcher, the optional metrics store, and the background
// workers (uploaders, dogfood scraper, sealed-index bootstrap). It holds the
// data-directory lock. amber.DB and cmd/amber are thin wrappers over it.
type Stack struct {
	Identity    dbmeta.Identity
	backupState dbmeta.BackupState
	LogManager  *storage.SegmentManager
	SpanManager *storage.SegmentManager
	LogSparse   *index.SparseIndex
	SpanSparse  *index.SparseIndex
	LogDir      string
	SpanDir     string
	Executor    *query.Executor
	Batcher     *ingest.Batcher
	OTLPJournal *otlpv4.Journal

	// MetricStore is nil when metrics are disabled.
	MetricStore *metricsengine.Store

	dogfoodStop chan struct{}
	dogfoodDone chan struct{}

	logUploader   *uploader
	spanUploader  *uploader
	remoteClosers []io.Closer

	ready                *atomic.Bool
	statusMu             sync.RWMutex
	degradedReasons      map[string]uint64
	walRepairEvents      uint64
	indexBootstrapErrors uint64
	closing              bool

	// bootstrapWG waits for the sealed-index bootstrap goroutine.
	bootstrapWG   sync.WaitGroup
	bootstrapDone chan struct{}

	retentionCancel context.CancelFunc
	retentionWG     sync.WaitGroup

	// lock guards the data directory against a second amber process or a
	// second embedded Open on the same path.
	lock *fslock.Lock

	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error
}

// IsReady reports whether bootstrap finished loading sealed indexes.
func (s *Stack) IsReady() bool { return s.ready.Load() }

// Status returns a consistent operational snapshot.
func (s *Stack) Status() Status {
	s.statusMu.RLock()
	reasonCounts := make(map[string]uint64, len(s.degradedReasons)+1)
	for code, count := range s.degradedReasons {
		reasonCounts[code] = count
	}
	closing := s.closing
	walRepairEvents := s.walRepairEvents
	indexBootstrapErrors := s.indexBootstrapErrors
	s.statusMu.RUnlock()

	if s.MetricStore != nil && s.MetricStore.LastBackgroundError() != nil {
		reasonCounts["metrics_background_error"]++
	}

	reasons := make([]StatusReason, 0, len(reasonCounts))
	for code, count := range reasonCounts {
		reasons = append(reasons, StatusReason{Code: code, Count: count})
	}
	sort.Slice(reasons, func(i, j int) bool { return reasons[i].Code < reasons[j].Code })
	status := Status{
		Ready:                s.ready.Load() && !closing,
		Degraded:             len(reasons) > 0,
		Closing:              closing,
		DatabaseID:           s.Identity.ID,
		WALRepairEvents:      walRepairEvents,
		IndexBootstrapErrors: indexBootstrapErrors,
		Reasons:              reasons,
	}
	if checkpoint := s.backupState.LastSuccessful; checkpoint != nil {
		status.LastSuccessfulBackup = &BackupCheckpointStatus{
			Checkpoint: checkpoint.Checkpoint, SnapshotCreatedAt: checkpoint.SnapshotCreatedAt, CompletedAt: checkpoint.CompletedAt,
		}
	}
	if checkpoint := s.backupState.LastVerifiedRestore; checkpoint != nil {
		status.LastVerifiedRestore = &RestoreCheckpointStatus{
			Checkpoint: checkpoint.Checkpoint, SnapshotCreatedAt: checkpoint.SnapshotCreatedAt, VerifiedAt: checkpoint.VerifiedAt,
		}
	}
	return status
}

func (s *Stack) markDegraded(reason string) { s.markDegradedN(reason, 1) }

func (s *Stack) markDegradedN(reason string, count uint64) {
	if reason == "" || count == 0 {
		return
	}
	s.statusMu.Lock()
	s.degradedReasons[reason] += count
	s.statusMu.Unlock()
}

// New assembles and starts a Stack: it takes the data-directory lock, opens
// storage and the metrics store, wires the executor and batcher, and launches
// the background workers. Bootstrap of sealed indexes runs asynchronously
// (track it with Stack.IsReady). Close shuts everything down.
func New(ctx context.Context, opts Options) (*Stack, error) {
	if opts.DataDir == "" {
		return nil, errors.New("runtime: DataDir required")
	}
	if opts.IndexBootstrapWorkers < 0 {
		return nil, errors.New("runtime: IndexBootstrapWorkers must be positive when set")
	}
	cfg := opts.withDefaults()
	if cfg.Storage.S3Bucket == "" && (cfg.Retention.Logs.hasLocalTier() || cfg.Retention.Spans.hasLocalTier()) {
		return nil, errors.New("runtime: local retention tier requires S3 storage")
	}

	if cfg.MemoryLimit > 0 {
		debug.SetMemoryLimit(cfg.MemoryLimit)
		cfg.Logger.Info("go runtime memory limit set", "bytes", cfg.MemoryLimit)
	}

	// Exclusive lock on the data dir: a second writer (another amber process
	// or another embedded Open) would silently corrupt WAL/segments/meta.
	dirLock, err := fslock.Acquire(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("runtime: data dir %s already in use? %w", cfg.DataDir, err)
	}
	opened := false
	defer func() {
		if !opened {
			_ = dirLock.Release()
		}
	}()
	if err := validateOTLPV4Root(cfg.DataDir); err != nil {
		return nil, err
	}
	identity, err := dbmeta.LoadOrCreate(cfg.DataDir)
	if err != nil {
		return nil, fmt.Errorf("runtime: open database identity: %w", err)
	}
	backupState, backupStateErr := dbmeta.LoadBackupState(cfg.DataDir)

	logDir := filepath.Join(cfg.DataDir, "logs")
	spanDir := filepath.Join(cfg.DataDir, "spans")

	policy := storage.RotationPolicy{
		MaxRecords: cfg.Storage.SegmentMaxRecords,
		MaxBytes:   cfg.Storage.SegmentMaxBytes,
	}

	logManager, err := storage.OpenSegmentManager(logDir, policy)
	if err != nil {
		return nil, fmt.Errorf("runtime: open log segment manager: %w", err)
	}

	spanManager, err := storage.OpenSegmentManager(spanDir, policy)
	if err != nil {
		_ = logManager.Close()
		return nil, fmt.Errorf("runtime: open span segment manager: %w", err)
	}

	var logUp, spanUp *uploader
	var logRemote, spanRemote storage.SegmentStore
	var reconcileFailures []string
	if cfg.Storage.S3Bucket != "" {
		s3cfg := storage.S3StoreConfig{
			Bucket:   cfg.Storage.S3Bucket,
			Prefix:   cfg.Storage.S3Prefix,
			Region:   cfg.Storage.S3Region,
			Endpoint: cfg.Storage.S3Endpoint,
		}
		logS3, err := storage.NewS3Store(ctx, storage.S3StoreConfig{
			Bucket: s3cfg.Bucket, Prefix: s3cfg.Prefix,
			Region: s3cfg.Region, Endpoint: s3cfg.Endpoint,
			LocalDir: logDir,
		})
		if err != nil {
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open log s3 store: %w", err)
		}
		spanS3, err := storage.NewS3Store(ctx, storage.S3StoreConfig{
			Bucket: s3cfg.Bucket, Prefix: joinS3Prefix(s3cfg.Prefix, "spans"),
			Region: s3cfg.Region, Endpoint: s3cfg.Endpoint,
			LocalDir: spanDir,
		})
		if err != nil {
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open span s3 store: %w", err)
		}
		logManager.SetStore(logS3)
		spanManager.SetStore(spanS3)
		logRemote, spanRemote = logS3, spanS3

		logUp = newUploader(logManager, logS3, logDir, cfg.Logger)
		spanUp = newUploader(spanManager, spanS3, spanDir, cfg.Logger)
		logUp.Start(ctx)
		spanUp.Start(ctx)

		logManager.SetOnSealComplete(func(storage.SegmentMeta) { logUp.Enqueue() })
		spanManager.SetOnSealComplete(func(storage.SegmentMeta) { spanUp.Enqueue() })

		runLogReconcile := cfg.Storage.S3ReconcileOnStart || len(logManager.Segments()) == 0
		runSpanReconcile := cfg.Storage.S3ReconcileOnStart || len(spanManager.Segments()) == 0
		if runLogReconcile {
			if n, err := bootstrap.ReconcileFromRemote(ctx, logManager, logS3, logDir, cfg.Logger); err != nil {
				reconcileFailures = append(reconcileFailures, "log_remote_reconcile_failure")
				cfg.Logger.Warn("log s3 reconcile failed", "err", err)
			} else if n > 0 {
				cfg.Logger.Info("log s3 reconcile adopted segments", "count", n)
			}
		}
		if runSpanReconcile {
			if n, err := bootstrap.ReconcileFromRemote(ctx, spanManager, spanS3, spanDir, cfg.Logger); err != nil {
				reconcileFailures = append(reconcileFailures, "span_remote_reconcile_failure")
				cfg.Logger.Warn("span s3 reconcile failed", "err", err)
			} else if n > 0 {
				cfg.Logger.Info("span s3 reconcile adopted segments", "count", n)
			}
		}
	}

	// sparse.idx is a disposable acceleration artifact, not authoritative
	// state. Rebuild it from the recovered managers after local WAL replay and
	// remote reconciliation, so a missing, stale, or corrupt cache cannot make
	// durable segments invisible to queries.
	logSparse := bootstrap.BuildSparseIndex(logManager)
	spanSparse := bootstrap.BuildSparseIndex(spanManager)
	exec := query.NewExecutorWithCache(
		logManager, spanManager, logSparse, spanSparse,
		logDir, spanDir, cfg.IndexCacheSize,
	)
	if logRemote != nil || spanRemote != nil {
		exec.SetSegmentStores(logRemote, spanRemote, cfg.Logger)
	}

	ready := &atomic.Bool{}
	s := &Stack{
		Identity:    identity,
		backupState: backupState,
		ready:       ready, closeDone: make(chan struct{}), bootstrapDone: make(chan struct{}),
		degradedReasons: make(map[string]uint64),
	}
	if backupStateErr != nil {
		s.markDegraded("backup_state_corrupt")
		cfg.Logger.Warn("backup operational state is invalid", "err", backupStateErr)
	}
	s.walRepairEvents = logManager.WALCorruptRecords() + spanManager.WALCorruptRecords()
	if s.walRepairEvents > 0 {
		s.markDegradedN("wal_tail_repaired", s.walRepairEvents)
	}
	for _, reason := range reconcileFailures {
		s.markDegraded(reason)
	}
	bootstrap.SetupSealCallbacks(ctx, exec, logManager, spanManager, logDir, spanDir, cfg.Logger, s.markDegraded)

	var guard *ingest.CardinalityGuard
	if cfg.Cardinality.MaxAttrsPerEntry > 0 || cfg.Cardinality.MaxAttrValueBytes > 0 || cfg.Cardinality.MaxAttrKeysPerService > 0 || cfg.Cardinality.MaxServices > 0 {
		guard = ingest.NewCardinalityGuard(
			cfg.Cardinality.MaxAttrsPerEntry,
			cfg.Cardinality.MaxAttrValueBytes,
			cfg.Cardinality.MaxAttrKeysPerService,
			cfg.Cardinality.MaxServices,
		)
	}

	batcher := ingest.NewBatcher(ingest.Deps{
		LogManager:  logManager,
		SpanManager: spanManager,
		LogSparse:   logSparse,
		SpanSparse:  spanSparse,
		Indexer:     exec.ActiveIndex(),
		Guard:       guard,
		Invalidator: exec,
		Logger:      cfg.Logger,
	}, ingest.Config{
		BatchSize:        cfg.Ingest.BatchSize,
		BatchTimeout:     cfg.Ingest.BatchTimeout,
		QueueSize:        cfg.Ingest.QueueSize,
		BreakerThreshold: cfg.Ingest.BreakerThreshold,
		Logs: ingest.LaneConfig{
			BatchSize:        cfg.Ingest.Logs.BatchSize,
			BatchTimeout:     cfg.Ingest.Logs.BatchTimeout,
			QueueSize:        cfg.Ingest.Logs.QueueSize,
			BreakerThreshold: cfg.Ingest.Logs.BreakerThreshold,
		},
		Spans: ingest.LaneConfig{
			BatchSize:        cfg.Ingest.Spans.BatchSize,
			BatchTimeout:     cfg.Ingest.Spans.BatchTimeout,
			QueueSize:        cfg.Ingest.Spans.QueueSize,
			BreakerThreshold: cfg.Ingest.Spans.BreakerThreshold,
		},
	})

	var metricStore *metricsengine.Store
	if !cfg.Metrics.Disabled {
		metricsDir := cfg.Metrics.Dir
		if metricsDir == "" {
			metricsDir = filepath.Join(cfg.DataDir, "metrics")
		}
		cacheBudget := cfg.Metrics.CacheBudget
		if cacheBudget == 0 {
			// Derive from the effective soft memory limit: the value set here
			// if any, else whatever GOMEMLIMIT/env already imposed (read back
			// via SetMemoryLimit(-1)). Half the limit for block caches leaves
			// the other half to query scratch, ingest buffers and the head.
			effectiveLimit := cfg.MemoryLimit
			if effectiveLimit == 0 {
				effectiveLimit = debug.SetMemoryLimit(-1)
			}
			if effectiveLimit > 0 && effectiveLimit != math.MaxInt64 {
				cacheBudget = effectiveLimit / 2
			}
		}
		ms, err := metricsengine.OpenStoreWithOptions(metricsDir, metricsengine.StoreOptions{
			FlushInterval:       cfg.Metrics.FlushInterval,
			MaxBufferedSamples:  cfg.Metrics.MaxBufferedSamples,
			MaxActiveSeries:     cfg.Metrics.MaxActiveSeries,
			MaxLabelsPerSeries:  cfg.Metrics.MaxLabelsPerSeries,
			Retention:           cfg.Metrics.Retention,
			CompactionMinBlocks: cfg.Metrics.CompactionMinBlocks,
			CacheBudget:         cacheBudget,
		})
		if err != nil {
			if logUp != nil {
				logUp.Stop()
			}
			if spanUp != nil {
				spanUp.Stop()
			}
			_ = logManager.Close()
			_ = spanManager.Close()
			return nil, fmt.Errorf("runtime: open metric store: %w", err)
		}
		if ms.WALRecoveryStats().TruncatedBytes > 0 {
			s.markDegraded("metrics_wal_tail_repaired")
		}
		if unknown := ms.UnknownWALSeries(); unknown > 0 {
			s.markDegradedN("metrics_wal_unknown_series", uint64(unknown))
		}
		metricStore = ms
	}

	otlpJournal, err := otlpv4.OpenJournal(cfg.DataDir, policy)
	if err != nil {
		if metricStore != nil {
			_ = metricStore.Close()
		}
		if logUp != nil {
			logUp.Stop()
		}
		if spanUp != nil {
			spanUp.Stop()
		}
		_ = logManager.Close()
		_ = spanManager.Close()
		return nil, fmt.Errorf("runtime: open OTLP v4 journal: %w", err)
	}
	if stats, statsErr := otlpJournal.Stats(); statsErr != nil {
		_ = otlpJournal.Close()
		if metricStore != nil {
			_ = metricStore.Close()
		}
		if logUp != nil {
			logUp.Stop()
		}
		if spanUp != nil {
			spanUp.Stop()
		}
		_ = logManager.Close()
		_ = spanManager.Close()
		return nil, fmt.Errorf("runtime: read OTLP v4 journal stats: %w", statsErr)
	} else if stats.WALCorruptRecords > 0 {
		s.walRepairEvents += stats.WALCorruptRecords
		s.markDegradedN("otlp_v4_wal_tail_repaired", stats.WALCorruptRecords)
	}
	batcher.SetReplaySink(otlpJournal)
	if metricStore != nil {
		metricStore.SetReplaySink(otlpJournal)
	}

	s.LogManager = logManager
	s.SpanManager = spanManager
	s.LogSparse = logSparse
	s.SpanSparse = spanSparse
	s.LogDir = logDir
	s.SpanDir = spanDir
	s.Executor = exec
	s.Batcher = batcher
	s.MetricStore = metricStore
	s.OTLPJournal = otlpJournal
	s.logUploader = logUp
	s.spanUploader = spanUp
	if closer, ok := logRemote.(io.Closer); ok {
		s.remoteClosers = append(s.remoteClosers, closer)
	}
	if closer, ok := spanRemote.(io.Closer); ok {
		s.remoteClosers = append(s.remoteClosers, closer)
	}
	s.lock = dirLock
	opened = true

	s.bootstrapWG.Go(func() {
		defer close(s.bootstrapDone)
		report := bootstrap.LoadSealedIndexes(ctx, exec, logManager, spanManager, logDir, spanDir, cfg.IndexBootstrapWorkers, cfg.Logger)
		if report.IndexErrors > 0 {
			s.statusMu.Lock()
			s.indexBootstrapErrors += report.IndexErrors
			s.statusMu.Unlock()
			s.markDegradedN("index_bootstrap_failure", report.IndexErrors)
		}
		if ctx.Err() == nil {
			ready.Store(true)
			cfg.Logger.Info("sealed indexes loaded")
		}
	})

	s.startRetention(ctx, cfg.Retention, cfg.Storage.S3Bucket != "", cfg.Logger)

	batcher.Start(ctx)

	if metricStore != nil && cfg.Metrics.DogfoodInterval > 0 {
		s.dogfoodStop = make(chan struct{})
		s.dogfoodDone = make(chan struct{})
		go runDogfoodScraper(cfg.Metrics.DogfoodInterval, metricStore, cfg.Logger, s.dogfoodStop, s.dogfoodDone)
	}

	return s, nil
}

func (s *Stack) startRetention(parent context.Context, opts RetentionOptions, remoteEnabled bool, log *slog.Logger) {
	if !opts.Logs.enabled() && !opts.Spans.enabled() {
		return
	}
	ctx, cancel := context.WithCancel(parent)
	s.retentionCancel = cancel
	interval := opts.Interval
	if interval <= 0 {
		interval = time.Hour
	}

	start := func(stream string, policy StreamRetentionOptions, manager *storage.SegmentManager, sparse *index.SparseIndex, dir string, invalidate func(storage.SegmentMeta)) {
		if !policy.enabled() {
			return
		}
		cleaner := retention.NewCleaner(manager, sparse, policy.policy(), dir, stream, log)
		cleaner.SetOnDelete(invalidate)
		cleaner.SetOnLocalEvict(invalidate)
		cleaner.RequireUploaded(remoteEnabled)
		s.retentionWG.Go(func() {
			select {
			case <-s.bootstrapDone:
			case <-ctx.Done():
				return
			}
			cleaner.StartLoop(interval, ctx.Done())
		})
	}

	start("logs", opts.Logs, s.LogManager, s.LogSparse, s.LogDir, s.Executor.InvalidateLogSegment)
	start("spans", opts.Spans, s.SpanManager, s.SpanSparse, s.SpanDir, s.Executor.InvalidateSpanSegment)
}

// Close first stops ingest admission and drains both lane barriers, then shuts
// down storage under ctx's deadline. Background contexts may already be
// cancelled; the batcher owns its worker lifetime so cancellation cannot race
// the final admission drain.
func (s *Stack) Close(ctx context.Context) error {
	s.statusMu.Lock()
	s.closing = true
	s.ready.Store(false)
	s.statusMu.Unlock()
	s.closeOnce.Do(func() {
		go func() {
			s.closeErr = s.close()
			close(s.closeDone)
		}()
	})
	select {
	case <-s.closeDone:
		return s.closeErr
	case <-ctx.Done():
		return fmt.Errorf("runtime: shutdown: %w", ctx.Err())
	}
}

func (s *Stack) close() error {
	var errs []error
	if s.retentionCancel != nil {
		s.retentionCancel()
		s.retentionWG.Wait()
	}
	if err := s.Batcher.Close(context.Background()); err != nil {
		errs = append(errs, fmt.Errorf("runtime: batcher drain: %w", err))
	}

	// Stop the dogfood scraper before closing the metric store.
	if s.dogfoodStop != nil {
		close(s.dogfoodStop)
		<-s.dogfoodDone
	}

	// Stop uploaders before closing segment managers.
	for _, closer := range s.remoteClosers {
		if err := closer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("runtime: close remote store: %w", err))
		}
	}
	if s.logUploader != nil {
		s.logUploader.Stop()
	}
	if s.spanUploader != nil {
		s.spanUploader.Stop()
	}

	// Wait for bootstrap readers before closing segment managers.
	s.bootstrapWG.Wait()

	if s.MetricStore != nil {
		if err := s.MetricStore.Close(); err != nil {
			errs = append(errs, fmt.Errorf("runtime: close metric store: %w", err))
		}
	}
	if s.OTLPJournal != nil {
		if err := s.OTLPJournal.Close(); err != nil {
			errs = append(errs, fmt.Errorf("runtime: close OTLP v4 journal: %w", err))
		}
	}
	if s.Executor != nil {
		s.Executor.Close()
	}
	if err := s.LogSparse.Save(s.LogDir); err != nil {
		errs = append(errs, fmt.Errorf("runtime: save log sparse: %w", err))
	}
	if err := s.SpanSparse.Save(s.SpanDir); err != nil {
		errs = append(errs, fmt.Errorf("runtime: save span sparse: %w", err))
	}
	if err := s.LogManager.Close(); err != nil {
		errs = append(errs, fmt.Errorf("runtime: close log manager: %w", err))
	}
	if err := s.SpanManager.Close(); err != nil {
		errs = append(errs, fmt.Errorf("runtime: close span manager: %w", err))
	}
	// All workers and file owners have terminated at this point, so releasing
	// the lock is safe even when one component reported a terminal error.
	if err := s.lock.Release(); err != nil {
		errs = append(errs, fmt.Errorf("runtime: release dir lock: %w", err))
	}
	return errors.Join(errs...)
}

func validateOTLPV4Root(root string) error {
	journalPath := filepath.Join(root, otlpv4.DirectoryName)
	if info, err := os.Lstat(journalPath); err == nil {
		if !info.IsDir() {
			return errors.New("runtime: OTLP v4 journal path is not a directory")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("runtime: inspect OTLP v4 journal: %w", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("runtime: inspect data root: %w", err)
	}
	for _, entry := range entries {
		switch entry.Name() {
		case fslock.LockFileName, dbmeta.IdentityFileName, dbmeta.BackupStateFileName:
			continue
		default:
			return fmt.Errorf("runtime: data root contains engine state but no %s journal", otlpv4.DirectoryName)
		}
	}
	return nil
}
