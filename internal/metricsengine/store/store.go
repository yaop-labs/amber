package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"

	"github.com/yaop-labs/amber/internal/fslock"
	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

var ErrNoSamples = errors.New("store: no buffered samples to flush")
var ErrInvalidLabels = errors.New("store: invalid labels")
var ErrLabelLimitExceeded = errors.New("store: label limit exceeded")
var ErrActiveSeriesLimitExceeded = errors.New("store: active series limit exceeded")

// Store is the durable metrics store: it wraps the append engine with the
// on-disk catalog, block manifest, directory and resident caches, retention,
// compaction, and index eviction. It is safe for concurrent use, and a process
// holds an exclusive lock on its directory.
type Store struct {
	dir      string
	engine   *engine.Engine
	opts     Options
	clock    func() time.Time
	mu       sync.RWMutex
	manifest Manifest
	catalog  Catalog
	// catalogKeys indexes catalog entries by canonical label key. Kept in
	// step with catalog.Series (register + evict) so ensureCatalog stays
	// O(batch) instead of rebuilding an O(active series) map per append.
	catalogKeys    map[string]uint64
	directoryCache *dirCache
	residentCache  *residentCache
	// blockLoads collapses concurrent cold loads of the same block: without
	// it, N concurrent queries hitting a cold cache (startup, post-compaction
	// reset) each decode the same ~40MiB directory for one cache entry.
	blockLoads        singleflight.Group
	allowGlobFallback bool
	stopBackground    chan struct{}
	backgroundDone    chan struct{}
	closeOnce         sync.Once
	closeErr          error
	backgroundErrMu   sync.RWMutex
	backgroundErr     error

	// catalogLog persists REGISTER and EVICT records after recovery.
	catalogLog *catalogLog

	// stopSweep and sweepDone own the eviction sweep lifecycle.
	stopSweep chan struct{}
	sweepDone chan struct{}

	// dirLock guards dir against a second store instance (this process or
	// another) writing the same WAL/blocks/manifest.
	dirLock *fslock.Lock
}

// Stats summarizes a store for the admin/stats endpoint: sealed block totals
// (counts, on-disk bytes, time span) plus the unflushed head buffers.
type Stats struct {
	Blocks  int
	Series  int
	Samples int
	Bytes   int64
	MinTime int64
	MaxTime int64
	HasTime bool

	BufferedSeries  int
	BufferedSamples int
}

// Open opens (creating if needed) a store at dir with default options.
func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{})
}

// OpenConfigured opens a store from a Config.
func OpenConfigured(cfg Config) (*Store, error) {
	return OpenWithOptions(cfg.Dir, cfg.Options)
}

// OpenWithOptions opens a store at dir, taking the directory lock, recovering
// the WAL, catalog log, and manifest, and starting background flush, retention,
// and eviction as configured by opts.
func OpenWithOptions(dir string, opts Options) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store: dir is required")
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	dirLock, err := fslock.Acquire(dir)
	if err != nil {
		return nil, fmt.Errorf("store: dir %s already in use? %w", dir, err)
	}
	opened := false
	defer func() {
		if !opened {
			_ = dirLock.Release()
		}
	}()
	// Prefer the append-only catalog log. Fall back to the legacy JSON catalog
	// when opening an older store or an empty log.
	logLive, logHighest, err := loadCatalogLogState(dir)
	if err != nil {
		return nil, fmt.Errorf("catalog log recovery: %w", err)
	}
	var catalog Catalog
	if len(logLive) > 0 {
		ids := make([]uint64, 0, len(logLive))
		for id := range logLive {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		catalog = Catalog{NextID: logHighest + 1}
		for _, id := range ids {
			catalog.Series = append(catalog.Series, CatalogEntry{ID: id, Labels: logLive[id]})
		}
		if catalog.NextID == 0 {
			catalog.NextID = 1
		}
	} else {
		catalog, err = loadCatalog(dir)
		if err != nil {
			return nil, err
		}
	}
	e, err := engine.OpenWithRegistry(catalog.Registry(), engine.Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		return nil, err
	}
	manifest, err := loadManifest(dir)
	if err != nil {
		return nil, err
	}
	if err := recoverPendingFlushes(dir, &manifest, filepath.Join(dir, "head.wal")); err != nil {
		return nil, err
	}
	allowGlobFallback := true
	if len(manifest.Blocks) == 0 {
		hasWALRecords, err := wal.HasRecords(filepath.Join(dir, "head.wal"))
		if err != nil {
			return nil, err
		}
		if hasWALRecords {
			allowGlobFallback = false
		} else {
			rebuilt, err := rebuildManifest(dir)
			if err != nil {
				return nil, err
			}
			if len(rebuilt.Blocks) > 0 {
				manifest = rebuilt
				if err := saveManifest(dir, manifest); err != nil {
					return nil, err
				}
			}
		}
	}
	rebuiltFromBlocks := false
	if len(catalog.Series) == 0 && len(manifest.Blocks) > 0 {
		catalog, err = rebuildCatalogFromManifest(dir, manifest)
		if err != nil {
			return nil, err
		}
		if err := saveCatalog(dir, catalog); err != nil {
			return nil, err
		}
		e, err = engine.OpenWithRegistry(catalog.Registry(), engine.Options{WALPath: filepath.Join(dir, "head.wal")})
		if err != nil {
			return nil, err
		}
		rebuiltFromBlocks = true
	}
	// Reconcile last-touch from blocks before enabling eviction.
	if err := reconcileLastTouchFromBlocks(dir, manifest, e.Registry()); err != nil {
		return nil, err
	}
	catLog, err := openCatalogLog(dir)
	if err != nil {
		return nil, err
	}
	// Seed the catalog log when the catalog was rebuilt from blocks.
	if rebuiltFromBlocks {
		for _, entry := range catalog.Series {
			if err := catLog.AppendRegister(entry.ID, entry.Labels); err != nil {
				_ = catLog.Close()
				return nil, fmt.Errorf("seed catalog log from rebuild: %w", err)
			}
		}
	}
	catLog.startCommitter(5 * time.Millisecond)
	// Use the block retention window as the index eviction window.
	if opts.Retention > 0 {
		retentionMs := opts.Retention.Milliseconds()
		gran := retentionMs / 12
		if gran < 1000 {
			gran = 1000
		}
		e.Registry().SetEvictionBucketing(retentionMs, gran)
	}
	dirBudget, residentBudget := splitCacheBudget(opts.CacheBudget)
	st := &Store{
		dir:               dir,
		engine:            e,
		opts:              opts,
		clock:             opts.Clock,
		manifest:          manifest,
		catalog:           catalog,
		catalogKeys:       catalogKeyMap(catalog),
		directoryCache:    newDirCache(dirBudget),
		residentCache:     newResidentCache(residentBudget),
		allowGlobFallback: allowGlobFallback,
		catalogLog:        catLog,
		dirLock:           dirLock,
	}
	st.startBackground()
	st.startEvictionSweep()
	opened = true
	return st, nil
}

// Append is AppendBatch for a single sample.
func (s *Store) Append(labels model.LabelSet, typ model.MetricType, timestamp int64, value int64) (index.SeriesID, error) {
	if err := s.ensureCatalog([]model.LabelSet{labels}); err != nil {
		return 0, err
	}
	id, err := s.engine.Append(labels, typ, timestamp, value)
	if err != nil {
		return 0, err
	}
	if err := s.flushAfterAppend(); err != nil {
		return id, err
	}
	return id, nil
}

// AppendBatch registers any new series in the catalog and appends the samples
// through the engine (durable on return). It enforces the cardinality and
// label limits from Options.
func (s *Store) AppendBatch(samples []model.Sample) ([]index.SeriesID, error) {
	labelSets := make([]model.LabelSet, 0, len(samples))
	for _, sample := range samples {
		labelSets = append(labelSets, sample.Labels)
	}
	if err := s.ensureCatalog(labelSets); err != nil {
		return nil, err
	}
	ids, err := s.engine.AppendBatch(samples)
	if err != nil {
		return nil, err
	}
	if err := s.flushAfterAppend(); err != nil {
		return ids, err
	}
	return ids, nil
}

// AppendScaledFloat appends a float64 as round(value*scale) in the int64 value
// model, rejecting NaN, +/-Inf, and values that overflow at the given scale.
func (s *Store) AppendScaledFloat(labels model.LabelSet, typ model.MetricType, timestamp int64, value float64, scale int64) (index.SeriesID, error) {
	if err := s.ensureCatalog([]model.LabelSet{labels}); err != nil {
		return 0, err
	}
	id, err := s.engine.AppendScaledFloat(labels, typ, timestamp, value, scale)
	if err != nil {
		return 0, err
	}
	if err := s.flushAfterAppend(); err != nil {
		return id, err
	}
	return id, nil
}

// catalogKeyMap indexes catalog entries by canonical label key.
func catalogKeyMap(c Catalog) map[string]uint64 {
	keys := make(map[string]uint64, len(c.Series))
	for _, entry := range c.Series {
		keys[entry.Labels.Canonical().Key()] = entry.ID
	}
	return keys
}

// headSnapshot copies only the head series matching the selector, so a
// selective query doesn't pay for copying the whole buffered head.
func (s *Store) headSnapshot(selector index.Selector) []block.Series {
	return s.engine.SnapshotMatching(func(labels model.LabelSet) bool {
		return index.MatchLabels(labels, selector)
	})
}

func (s *Store) ensureCatalog(labelSets []model.LabelSet) error {
	if len(labelSets) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	newSeries := make(map[string]model.LabelSet)
	for _, labels := range labelSets {
		if err := validateLabels(labels, s.opts); err != nil {
			return err
		}
		canonical := labels.Canonical()
		key := canonical.Key()
		if _, ok := s.catalogKeys[key]; ok {
			continue
		}
		if _, ok := newSeries[key]; ok {
			continue
		}
		newSeries[key] = canonical
	}
	if s.opts.MaxActiveSeries > 0 && len(s.catalog.Series)+len(newSeries) > s.opts.MaxActiveSeries {
		return fmt.Errorf("%w: have %d new %d max %d", ErrActiveSeriesLimitExceeded, len(s.catalog.Series), len(newSeries), s.opts.MaxActiveSeries)
	}
	if len(newSeries) == 0 {
		return nil
	}

	keys := make([]string, 0, len(newSeries))
	for key := range newSeries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	registered := make([]liveSeries, 0, len(keys))
	for _, key := range keys {
		id := s.catalog.NextID
		if id == 0 {
			id = 1
		}
		s.catalog.NextID = id + 1
		labels := newSeries[key]
		s.catalog.Series = append(s.catalog.Series, CatalogEntry{ID: id, Labels: labels})
		s.catalogKeys[key] = id
		s.engine.Registry().Import(index.SeriesID(id), labels)
		registered = append(registered, liveSeries{ID: id, Labels: labels})
	}
	// Persist to the append-only catalog log. INDEX_EVICTION_SPEC_v0
	// REGISTER per new series, O(1) per add. Replaces the legacy
	// JSON saveCatalog rewrite (which was O(N) per add under s.mu -
	// the catalog-mutex bottleneck identified in loadtest_v0).
	// loadCatalog (JSON) is still read at boot as a rollback-safety
	// fallback for stores that pre-date 3a. The whole batch rides one
	// commit wait - per-series waits collapse ingest when a batch
	// introduces thousands of series (metrics bench, 2026-06-12).
	if s.catalogLog != nil {
		if err := s.catalogLog.AppendRegisterBatch(registered); err != nil {
			return fmt.Errorf("catalog log append: %w", err)
		}
	}
	return nil
}

// Flush writes the buffered head (scalars and sketches) to a new block under
// the prepare/commit flush protocol and records it in the manifest. It returns
// the block path, or "" when the head was empty.
func (s *Store) Flush() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.engine.BufferedSeries() == 0 && s.engine.BufferedSketchSeries() == 0 {
		return "", ErrNoSamples
	}
	path := s.nextBlockPath()
	if err := s.engine.PrepareFlushBlock(path); err != nil {
		return "", err
	}
	// PrepareFlushBlock holds the engine's flush gate; release it on every
	// error path before CommitFlush, or appends would block forever.
	dir, err := block.ReadDirectory(path)
	if err != nil {
		s.engine.AbortFlush()
		return "", err
	}
	minTime, maxTime, _ := dir.TimeRange()
	meta := BlockMeta{
		Path:        filepath.Base(path),
		MinTime:     minTime,
		MaxTime:     maxTime,
		SeriesCount: len(dir.Series),
		SampleCount: directorySampleCount(dir),
		LabelValues: labelValues(dir),
	}

	// Histogram head flushes into a sibling hist block under the same gate,
	// markers, and commit.
	exp, explicit := s.engine.SketchSnapshot()
	histPath := ""
	var histMeta BlockMeta
	if len(exp) > 0 || len(explicit) > 0 {
		histPath = strings.TrimSuffix(s.nextBlockPathWithPrefix("hist"), ".meb") + ".mhb"
		if err := histogram.WriteBlock(histPath, exp, explicit); err != nil {
			s.engine.AbortFlush()
			return "", err
		}
		hdir, err := histogram.ReadDirectory(histPath)
		if err != nil {
			s.engine.AbortFlush()
			return "", err
		}
		hmin, hmax, _ := hdir.TimeRange()
		histMeta = BlockMeta{
			Path:        filepath.Base(histPath),
			Kind:        BlockKindHistogram,
			MinTime:     hmin,
			MaxTime:     hmax,
			SeriesCount: len(hdir.Series),
			LabelValues: histLabelValues(hdir),
		}
		if err := writeFlushPendingMarker(histPath, "prepared"); err != nil {
			s.engine.AbortFlush()
			return "", err
		}
	}

	if err := writeFlushPendingMarker(path, "prepared"); err != nil {
		s.engine.AbortFlush()
		return "", err
	}
	if err := s.engine.CommitFlush(); err != nil {
		return "", err
	}
	if err := writeFlushPendingMarker(path, "committed"); err != nil {
		return "", err
	}
	if histPath != "" {
		if err := writeFlushPendingMarker(histPath, "committed"); err != nil {
			return "", err
		}
	}
	if meta.SeriesCount > 0 {
		s.manifest.Blocks = append(s.manifest.Blocks, meta)
	}
	if histPath != "" {
		s.manifest.Blocks = append(s.manifest.Blocks, histMeta)
	}
	if err := saveManifest(s.dir, s.manifest); err != nil {
		return "", err
	}
	if meta.SeriesCount == 0 {
		// Sketch-only flush: drop the empty scalar block.
		_ = os.Remove(path)
	}
	if err := clearFlushPendingMarker(path); err != nil {
		return "", err
	}
	if histPath != "" {
		if err := clearFlushPendingMarker(histPath); err != nil {
			return "", err
		}
	}
	if meta.SeriesCount > 0 {
		s.directoryCache.put(path, dir)
	}
	if meta.SeriesCount == 0 && histPath == "" {
		return "", ErrNoSamples
	}
	if meta.SeriesCount == 0 {
		return histPath, nil
	}
	return path, nil
}

const flushPendingSuffix = ".flush-pending"

func writeFlushPendingMarker(blockPath, state string) error {
	marker := blockPath + flushPendingSuffix
	file, err := os.OpenFile(marker, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write([]byte(state)); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return syncDir(filepath.Dir(blockPath))
}

func clearFlushPendingMarker(blockPath string) error {
	marker := blockPath + flushPendingSuffix
	if err := os.Remove(marker); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(blockPath))
}

func recoverPendingFlushes(dir string, manifest *Manifest, walPath string) error {
	markers, err := filepath.Glob(filepath.Join(dir, "*"+flushPendingSuffix))
	if err != nil {
		return err
	}
	if len(markers) == 0 {
		return nil
	}

	known := make(map[string]struct{}, len(manifest.Blocks))
	for _, meta := range manifest.Blocks {
		known[meta.Path] = struct{}{}
	}
	hasWALRecords, err := wal.HasRecords(walPath)
	if err != nil {
		return err
	}

	changed := false
	for _, marker := range markers {
		base := strings.TrimSuffix(filepath.Base(marker), flushPendingSuffix)
		blockPath := filepath.Join(dir, base)
		if _, ok := known[base]; ok {
			_ = os.Remove(marker)
			continue
		}
		stateBytes, _ := os.ReadFile(marker)
		state := strings.TrimSpace(string(stateBytes))
		committed := state == "committed" || !hasWALRecords
		if !committed {
			_ = os.Remove(blockPath)
			_ = os.Remove(marker)
			continue
		}
		var adopted BlockMeta
		if strings.HasSuffix(base, ".mhb") {
			hdir, err := histogram.ReadDirectory(blockPath)
			if err != nil {
				return err
			}
			hmin, hmax, _ := hdir.TimeRange()
			adopted = BlockMeta{
				Path:        base,
				Kind:        BlockKindHistogram,
				MinTime:     hmin,
				MaxTime:     hmax,
				SeriesCount: len(hdir.Series),
				LabelValues: histLabelValues(hdir),
			}
		} else {
			dirInfo, err := block.ReadDirectory(blockPath)
			if err != nil {
				return err
			}
			minTime, maxTime, _ := dirInfo.TimeRange()
			adopted = BlockMeta{
				Path:        base,
				MinTime:     minTime,
				MaxTime:     maxTime,
				SeriesCount: len(dirInfo.Series),
				SampleCount: directorySampleCount(dirInfo),
				LabelValues: labelValues(dirInfo),
			}
		}
		if adopted.SeriesCount == 0 {
			_ = os.Remove(blockPath)
			_ = os.Remove(marker)
			continue
		}
		manifest.Blocks = append(manifest.Blocks, adopted)
		known[base] = struct{}{}
		changed = true
		_ = os.Remove(marker)
	}
	if changed {
		if err := saveManifest(dir, *manifest); err != nil {
			return err
		}
	}
	return syncDir(dir)
}

// FlushIfNeeded flushes only when the head holds at least maxBufferedSeries
// series, reporting whether a flush happened.
func (s *Store) FlushIfNeeded(maxBufferedSeries int) (string, bool, error) {
	return s.FlushIfNeededBy(maxBufferedSeries, 0)
}

// FlushIfNeededBy flushes when either the buffered series or sample count
// reaches its threshold (a zero threshold disables that trigger).
func (s *Store) FlushIfNeededBy(maxBufferedSeries int, maxBufferedSamples int) (string, bool, error) {
	if !s.flushThresholdExceeded(maxBufferedSeries, maxBufferedSamples) {
		return "", false, nil
	}
	path, err := s.Flush()
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}

func (s *Store) flushAfterAppend() error {
	_, _, err := s.FlushIfNeededBy(s.opts.MaxBufferedSeries, s.opts.MaxBufferedSamples)
	return err
}

func (s *Store) flushThresholdExceeded(maxBufferedSeries int, maxBufferedSamples int) bool {
	if maxBufferedSeries > 0 && s.engine.BufferedSeries() >= maxBufferedSeries {
		return true
	}
	if maxBufferedSamples > 0 && s.engine.BufferedSamples() >= maxBufferedSamples {
		return true
	}
	return false
}

// DeleteBefore drops whole blocks whose newest sample predates cutoffMillis and
// returns how many were removed. It is the retention primitive.
func (s *Store) DeleteBefore(cutoffMillis int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var kept []BlockMeta
	removePaths := make([]string, 0, len(s.manifest.Blocks))
	for _, meta := range s.manifest.Blocks {
		if meta.MaxTime >= cutoffMillis {
			kept = append(kept, meta)
			continue
		}
		removePaths = append(removePaths, filepath.Join(s.dir, meta.Path))
	}
	s.manifest.Blocks = kept
	s.directoryCache.reset()
	s.residentCache.reset()
	if err := saveManifest(s.dir, s.manifest); err != nil {
		return 0, err
	}
	deleted := 0
	for _, path := range removePaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return deleted, err
		}
		deleted++
	}
	return deleted, nil
}

// Compact merges all blocks into one. When Retention is configured, samples
// older than the retention cutoff are dropped during the merge: retention
// otherwise only deletes whole blocks (DeleteBefore), and a continuously
// re-compacted block always carries a fresh MaxTime, so without this drop
// expired data would survive every retention pass and disk usage would grow
// without bound.
func (s *Store) Compact() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	paths, err := s.blocksForQueryLocked(index.Selector{}, query.Options{})
	if err != nil {
		return "", err
	}
	if len(paths) <= 1 {
		// Nothing to do on the scalar side; histogram blocks may still merge.
		if histPath, err := s.compactHistLocked(); err != nil {
			return "", err
		} else if histPath != "" {
			return histPath, nil
		}
		return "", ErrNoSamples
	}

	var cutoff int64
	hasCutoff := false
	if s.opts.Retention > 0 {
		cutoff = s.clock().Add(-s.opts.Retention).UnixMilli()
		hasCutoff = true
	}

	grouped := make(map[string]block.Series)
	for _, path := range paths {
		decoded, err := block.ReadFile(path)
		if err != nil {
			return "", err
		}
		for _, series := range decoded {
			timestamps, values := series.Timestamps, series.Values
			if hasCutoff {
				timestamps, values = dropExpiredSamples(timestamps, values, cutoff)
				if len(timestamps) == 0 {
					continue
				}
			}
			key := series.Entry.Labels.Key()
			current := grouped[key]
			if current.ID == 0 {
				current.ID = series.Entry.SeriesID
				current.Type = series.Entry.Type
				current.Labels = series.Entry.Labels
			}
			current.Timestamps = append(current.Timestamps, timestamps...)
			current.Values = append(current.Values, values...)
			grouped[key] = current
		}
	}

	merged := make([]block.Series, 0, len(grouped))
	for _, series := range grouped {
		merged = append(merged, compactSeriesSamples(series))
	}

	// Everything expired: delete the old blocks without writing a new one.
	if len(merged) == 0 {
		oldBlocks := append([]BlockMeta(nil), s.manifest.Blocks...)
		s.manifest.Blocks = nil
		if err := saveManifest(s.dir, s.manifest); err != nil {
			return "", err
		}
		for _, meta := range oldBlocks {
			_ = os.Remove(filepath.Join(s.dir, meta.Path))
		}
		s.directoryCache.reset()
		s.residentCache.reset()
		return "", nil
	}

	path := s.nextBlockPathWithPrefix("compact")
	if err := block.WriteFile(path, merged); err != nil {
		return "", err
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		return "", err
	}
	minTime, maxTime, _ := dir.TimeRange()
	oldBlocks := make([]BlockMeta, 0, len(s.manifest.Blocks))
	kept := make([]BlockMeta, 0, len(s.manifest.Blocks))
	for _, meta := range s.manifest.Blocks {
		if meta.Kind == "" {
			oldBlocks = append(oldBlocks, meta)
		} else {
			kept = append(kept, meta)
		}
	}
	kept = append(kept, BlockMeta{
		Path:        filepath.Base(path),
		MinTime:     minTime,
		MaxTime:     maxTime,
		SeriesCount: len(dir.Series),
		SampleCount: directorySampleCount(dir),
		LabelValues: labelValues(dir),
	})
	s.manifest.Blocks = kept
	if err := saveManifest(s.dir, s.manifest); err != nil {
		return "", err
	}
	for _, meta := range oldBlocks {
		_ = os.Remove(filepath.Join(s.dir, meta.Path))
	}
	s.directoryCache.reset()
	s.residentCache.reset()
	s.directoryCache.put(path, dir)
	if _, err := s.compactHistLocked(); err != nil {
		return path, err
	}
	return path, nil
}

// CompactIfNeeded merges blocks into one when at least minBlocks exist,
// reporting whether a compaction ran and the resulting block path.
func (s *Store) CompactIfNeeded(minBlocks int) (string, bool, error) {
	if minBlocks <= 1 {
		minBlocks = 2
	}
	paths, err := s.Blocks()
	if err != nil {
		return "", false, err
	}
	if len(paths) < minBlocks {
		return "", false, nil
	}
	path, err := s.Compact()
	if err != nil {
		return "", false, err
	}
	return path, true, nil
}

// Blocks returns the paths of the sealed scalar blocks in the manifest.
func (s *Store) Blocks() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blocksForQueryLocked(index.Selector{}, query.Options{})
}

// BlockCount returns the number of sealed blocks currently tracked by the
// manifest. Cheap (RLock + len); safe to call from a Prometheus scrape path.
func (s *Store) BlockCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.manifest.Blocks)
}

// BufferedSeries returns the number of distinct series in the in-memory head.
func (s *Store) BufferedSeries() int { return s.engine.BufferedSeries() }

// BufferedSamples returns the number of samples in the in-memory head.
func (s *Store) BufferedSamples() int { return s.engine.BufferedSamples() }

// MetricNames returns the sorted unique metric names visible in the head index.
// Sealed blocks are not scanned; the head registry covers all series that have
// received at least one sample since the last flush.
func (s *Store) MetricNames() []string {
	return s.engine.Registry().LabelValues(model.MetricNameLabel)
}

// ActiveSeries returns the number of distinct series tracked by the registry.
func (s *Store) ActiveSeries() int {
	return s.engine.Registry().SeriesCount()
}

func (s *Store) blocksForQueryLocked(selector index.Selector, opts query.Options) ([]string, error) {
	paths, _, err := s.blocksAndRangesForQueryLocked(selector, opts)
	return paths, err
}

// blockTimeRange is a block's stored [min,max] event-time span, read from the
// manifest without decoding the block directory.
type blockTimeRange struct {
	min int64
	max int64
}

// blocksAndRangesForQueryLocked returns the matched block paths and, when the
// manifest drives the query, each block's stored time range (aligned with
// paths). The ranges let range-step queries decide from metadata alone whether
// a step window can straddle a block boundary - i.e. whether to run the streamed
// exact path instead of the per-step summary pass. ranges is nil for the glob
// fallback (no manifest), where callers keep the summary path's own check.
func (s *Store) blocksAndRangesForQueryLocked(selector index.Selector, opts query.Options) ([]string, []blockTimeRange, error) {
	if err := selector.Validate(); err != nil {
		return nil, nil, err
	}
	if len(s.manifest.Blocks) > 0 {
		paths := make([]string, 0, len(s.manifest.Blocks))
		ranges := make([]blockTimeRange, 0, len(s.manifest.Blocks))
		for _, meta := range s.manifest.Blocks {
			if meta.Kind != "" {
				// Histogram blocks have their own query path.
				continue
			}
			if !metaMatchesTime(meta, opts) || !metaMatchesSelector(meta, selector) {
				continue
			}
			paths = append(paths, filepath.Join(s.dir, meta.Path))
			ranges = append(ranges, blockTimeRange{min: meta.MinTime, max: meta.MaxTime})
		}
		return paths, ranges, nil
	}
	if !s.allowGlobFallback {
		return nil, nil, nil
	}
	blockPaths, err := filepath.Glob(filepath.Join(s.dir, "block-*.meb"))
	if err != nil {
		return nil, nil, err
	}
	compactPaths, err := filepath.Glob(filepath.Join(s.dir, "compact-*.meb"))
	if err != nil {
		return nil, nil, err
	}
	paths := make([]string, 0, len(blockPaths)+len(compactPaths))
	paths = append(paths, blockPaths...)
	paths = append(paths, compactPaths...)
	sort.Strings(paths)
	return paths, nil, nil
}

// blockWindowsCanStraddle reports whether a range-step window of the given
// length can span two of these blocks. When it can, a continuous series'
// per-step summaries from adjacent blocks would have to be merged across the
// boundary - which the speculative summary pass detects only after decoding
// every in-window block to compute per-step windows, then discards for the
// exact path. At campaign scale (100k series, ~15 in-window blocks) that summary
// pass is slower than going straight to the streamed exact path. This predicate
// is decode-free (manifest time ranges only) and conservative: a false positive
// only routes to the always-correct exact path, never changes results.
func blockWindowsCanStraddle(ranges []blockTimeRange, window time.Duration) bool {
	if len(ranges) < 2 {
		return false
	}
	windowMillis := window.Milliseconds()
	ordered := append([]blockTimeRange(nil), ranges...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].min == ordered[j].min {
			return ordered[i].max < ordered[j].max
		}
		return ordered[i].min < ordered[j].min
	})
	runningMax := ordered[0].max
	for _, r := range ordered[1:] {
		if r.min <= runningMax+windowMillis {
			return true
		}
		if r.max > runningMax {
			runningMax = r.max
		}
	}
	return false
}

// Select returns the matching series across all blocks and the head, merged.
func (s *Store) Select(selector index.Selector, opts query.Options) ([]block.DecodedSeries, error) {
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	var out []block.DecodedSeries
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return nil, err
		}
		series, err := query.SelectBlockWithDirectory(path, dir, selector, opts)
		if err != nil {
			return nil, err
		}
		out = append(out, series...)
	}
	headSeries, err := query.SelectSeries(s.headSnapshot(selector), selector, opts)
	if err != nil {
		return nil, err
	}
	out = append(out, headSeries...)
	return out, nil
}

// SelectRange is Select over the range selector's window ending at endMillis.
func (s *Store) SelectRange(rangeSelector query.RangeSelector, endMillis int64) ([]block.DecodedSeries, error) {
	return s.Select(rangeSelector.Selector, rangeSelector.Options(endMillis))
}

// Explain plans the query against current block/head cardinality and returns
// the chosen execution path and candidates without running it.
func (s *Store) Explain(plan query.Plan) (query.ExecutionPlan, error) {
	selector, opts, err := plan.StorageSelectorOptions()
	if err != nil {
		return query.ExecutionPlan{}, err
	}
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return query.ExecutionPlan{}, err
	}
	stats := query.CandidateStats{
		BlockCount:      len(paths),
		HasPointFilters: hasPointFilters(opts),
	}
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return query.ExecutionPlan{}, err
		}
		seriesCount, sampleCount, err := query.DirectoryStats(dir, selector, opts)
		if err != nil {
			return query.ExecutionPlan{}, err
		}
		stats.BlockSeries += seriesCount
		stats.BlockSamples += sampleCount
		bucketSeries, bucketSamples, partialBucketSeries, err := query.DirectoryBucketStats(dir, selector, opts)
		if err != nil {
			return query.ExecutionPlan{}, err
		}
		stats.BucketSeries += bucketSeries
		stats.BucketSamples += bucketSamples
		stats.PartialBucketSeries += partialBucketSeries
	}
	headSeries, headSamples, err := query.SeriesStats(s.headSnapshot(selector), selector, opts)
	if err != nil {
		return query.ExecutionPlan{}, err
	}
	stats.HeadSeries = headSeries
	stats.HeadSamples = headSamples
	if plan.Operation == query.OpRateByLabelRangeSteps || plan.Operation == query.OpIncreaseByLabelRangeSteps || plan.Operation == query.OpSumByLabelRangeSteps || plan.Operation == query.OpAggregateByLabelRangeSteps {
		steps, err := query.StepMillis(plan.StartMillis, plan.EndMillis, plan.Step)
		if err != nil {
			return query.ExecutionPlan{}, err
		}
		stats.StepCount = len(steps)
		if (plan.Operation == query.OpSumByLabelRangeSteps || plan.Operation == query.OpAggregateByLabelRangeSteps) && stats.HeadSeries == 0 && len(paths) > 0 {
			targets, err := s.readBlockDirectoryTargets(paths)
			if err != nil {
				return query.ExecutionPlan{}, err
			}
			if aggregateStepBucketsCoverTargets(targets, selector, plan.ByLabel, steps, plan.RangeSelector.Window) {
				stats.BucketSamples = stats.BlockSamples
				stats.PartialBucketSeries = 0
			}
		}
	}
	return query.PlanExecution(plan, stats)
}

// Execute runs a planned query and returns the result for the operation's
// shape. It is the generic entry point behind the typed helpers below.
func (s *Store) Execute(plan query.Plan) (query.Result, error) {
	if err := plan.Validate(); err != nil {
		return query.Result{}, err
	}
	switch plan.Operation {
	case query.OpSelect:
		series, err := s.Select(plan.Selector, plan.Options)
		return query.Result{Series: series}, err
	case query.OpSumByLabel:
		values, err := s.SumByLabel(plan.Selector, plan.Options, plan.ByLabel)
		return query.Result{IntValues: values}, err
	case query.OpAggregateByLabel:
		aggregates, err := s.AggregateByLabel(plan.Selector, plan.Options, plan.ByLabel)
		return query.Result{Aggregates: aggregates}, err
	case query.OpRateByLabel:
		values, err := s.RateByLabel(plan.Selector, plan.Options, plan.ByLabel)
		return query.Result{FloatValues: values}, err
	case query.OpIncreaseByLabel:
		values, err := s.IncreaseByLabel(plan.Selector, plan.Options, plan.ByLabel)
		return query.Result{IntValues: values}, err
	case query.OpRateByLabelRange:
		values, err := s.RateByLabelRange(plan.RangeSelector, plan.EndMillis, plan.ByLabel)
		return query.Result{FloatValues: values}, err
	case query.OpIncreaseByLabelRange:
		values, err := s.IncreaseByLabelRange(plan.RangeSelector, plan.EndMillis, plan.ByLabel)
		return query.Result{IntValues: values}, err
	case query.OpRateByLabelRangeSteps:
		steps, err := s.RateByLabelRangeSteps(plan.RangeSelector, plan.StartMillis, plan.EndMillis, plan.Step, plan.ByLabel)
		return query.Result{FloatSteps: steps}, err
	case query.OpIncreaseByLabelRangeSteps:
		steps, err := s.IncreaseByLabelRangeSteps(plan.RangeSelector, plan.StartMillis, plan.EndMillis, plan.Step, plan.ByLabel)
		return query.Result{IntSteps: steps}, err
	case query.OpSumByLabelRangeSteps:
		steps, err := s.SumByLabelRangeSteps(plan.RangeSelector, plan.StartMillis, plan.EndMillis, plan.Step, plan.ByLabel)
		return query.Result{IntSteps: steps}, err
	case query.OpAggregateByLabelRangeSteps:
		steps, err := s.AggregateByLabelRangeSteps(plan.RangeSelector, plan.StartMillis, plan.EndMillis, plan.Step, plan.ByLabel)
		return query.Result{AggregateSteps: steps}, err
	default:
		return query.Result{}, errors.New("store: unsupported query operation")
	}
}

// SumByLabel returns the per-group sum of matching samples, grouped by label.
func (s *Store) SumByLabel(selector index.Selector, opts query.Options, label string) (map[string]int64, error) {
	aggs, err := s.AggregateByLabel(selector, opts, label)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64, len(aggs))
	for key, agg := range aggs {
		out[key] = agg.Sum
	}
	return out, nil
}

// SumByLabelRangeSteps returns the per-group sum in each step's window.
func (s *Store) SumByLabelRangeSteps(rangeSelector query.RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]query.IntStep, error) {
	aggregateSteps, err := s.AggregateByLabelRangeSteps(rangeSelector, startMillis, endMillis, step, label)
	if err != nil {
		return nil, err
	}
	out := make([]query.IntStep, len(aggregateSteps))
	for i, aggregateStep := range aggregateSteps {
		values := make(map[string]int64, len(aggregateStep.Values))
		for key, agg := range aggregateStep.Values {
			values[key] = agg.Sum
		}
		out[i] = query.IntStep{TimestampMillis: aggregateStep.TimestampMillis, Values: values}
	}
	return out, nil
}

// AggregateByLabel returns the per-group sum/count/min/max of matching samples.
func (s *Store) AggregateByLabel(selector index.Selector, opts query.Options, label string) (map[string]query.Aggregate, error) {
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	out := make(map[string]query.Aggregate)
	for _, path := range paths {
		var partial map[string]query.Aggregate
		if opts.StartMillis == nil && opts.EndMillis == nil && opts.MinValue == nil && opts.MaxValue == nil {
			dir, err := s.readDirectory(path)
			if err != nil {
				return nil, err
			}
			partial = query.AggregateByLabelInDirectory(dir, selector, opts, label)
		} else {
			dir, err := s.readDirectory(path)
			if err != nil {
				return nil, err
			}
			partial, err = query.AggregateByLabelInBlockWithDirectory(path, dir, selector, opts, label)
			if err != nil {
				return nil, err
			}
		}
		mergeAggregates(out, partial)
	}
	headSeries, err := query.SelectSeries(s.headSnapshot(selector), selector, opts)
	if err != nil {
		return nil, err
	}
	mergeAggregates(out, query.AggregateByLabel(headSeries, label))
	return out, nil
}

// AggregateByLabelRangeSteps returns the per-group aggregate in each step's window.
func (s *Store) AggregateByLabelRangeSteps(rangeSelector query.RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]query.AggregateStep, error) {
	readStart := startMillis - rangeSelector.Window.Milliseconds()
	readOpts := query.TimeRange(readStart, endMillis)
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(rangeSelector.Selector, readOpts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 1 && s.engine.BufferedSeries() == 0 {
		dir, err := s.readDirectory(paths[0])
		if err != nil {
			return nil, err
		}
		return query.AggregateByLabelStepsInBlockWithDirectory(paths[0], dir, rangeSelector.Selector, label, startMillis, endMillis, step, rangeSelector.Window)
	}
	steps, err := query.StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if len(paths) > 1 && s.engine.BufferedSeries() == 0 {
		out, ok, err := s.aggregateByLabelRangeStepsBlockMerge(paths, rangeSelector.Selector, steps, rangeSelector.Window, label)
		if err != nil {
			return nil, err
		}
		if ok {
			return out, nil
		}
	}
	return s.aggregateByLabelRangeStepsExact(paths, rangeSelector.Selector, readOpts, steps, rangeSelector.Window, label)
}

// RateByLabel returns the per-group counter rate over the window in opts.
func (s *Store) RateByLabel(selector index.Selector, opts query.Options, label string) (map[string]float64, error) {
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return query.RateByLabelInSeries(s.headSnapshot(selector), selector, opts, label)
	}
	if opts.MaxSampleGapMillis != nil {
		return s.rateByLabelExact(selector, opts, label)
	}
	// Instant rate (qm1/qm4) runs on the resident index, the same compact form
	// the range-step path builds - so a mixed rate workload populates only the
	// resident cache instead of also filling the directory cache for the same
	// blocks. Each series is assembled once across all in-window blocks and its
	// rate computed exactly, which subsumes the old summary-merge + overlap
	// fallback.
	return s.rateByLabelResident(paths, selector, opts, label)
}

// rateByLabelResident computes the per-group instant rate over the resident
// index, partitioned by series across workers (see parallelRangeStepReduce). It
// falls back to the sequential resident scan for an oversized block.
func (s *Store) rateByLabelResident(paths []string, selector index.Selector, opts query.Options, label string) (map[string]float64, error) {
	out, ok, err := parallelRangeStepReduce(s, paths, selector, opts, label,
		func() map[string]float64 { return make(map[string]float64) },
		func() func(map[string]float64, *groupedRateSeries) error {
			return func(acc map[string]float64, gr *groupedRateSeries) error {
				rate, ok := rateFromGroupedSamples(gr.samples, opts)
				if ok {
					acc[gr.groupKey] += rate
				}
				return nil
			}
		},
		mergeFloatMap)
	if err != nil {
		return nil, err
	}
	if ok {
		return out, nil
	}
	grouped, err := s.collectRangeStepSeriesGrouped(paths, selector, opts, label)
	if err != nil {
		return nil, err
	}
	out = make(map[string]float64)
	for _, gr := range grouped {
		rate, ok := rateFromGroupedSamples(gr.samples, opts)
		if ok {
			out[gr.groupKey] += rate
		}
	}
	return out, nil
}

// rateFromGroupedSamples computes a single counter rate over a series' samples
// assembled from every in-window block. The samples arrive in block order, so
// they are sorted and deduplicated first. The window/value filtering already ran
// in the scan and MaxSampleGap routes to the exact path, so the rate is summed
// directly over the samples (positive deltas only, matching RateSummary) without
// materialising the timestamp/value slices RateSummaryForSamples would need.
func rateFromGroupedSamples(rawSamples []rateSample, _ query.Options) (float64, bool) {
	samples := compactExactRateSamples(rawSamples)
	if len(samples) < 2 {
		return 0, false
	}
	dtMillis := samples[len(samples)-1].timestamp - samples[0].timestamp
	if dtMillis <= 0 {
		return 0, false
	}
	var increase int64
	for i := 1; i < len(samples); i++ {
		if delta := samples[i].value - samples[i-1].value; delta > 0 {
			increase += delta
		}
	}
	return float64(increase) / (float64(dtMillis) / 1000.0), true
}

func mergeFloatMap(dst, src map[string]float64) {
	for key, value := range src {
		dst[key] += value
	}
}

// RateByLabelRange returns the per-group counter rate over the range selector's
// window ending at endMillis.
func (s *Store) RateByLabelRange(rangeSelector query.RangeSelector, endMillis int64, label string) (map[string]float64, error) {
	return s.RateByLabel(rangeSelector.Selector, rangeSelector.Options(endMillis), label)
}

// RateByLabelRangeSteps returns the per-group counter rate at each range step.
// This is the hot path: it routes between the summary and the streamed,
// series-partitioned exact paths (see the resident index and Explain).
func (s *Store) RateByLabelRangeSteps(rangeSelector query.RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]query.FloatStep, error) {
	readStart := startMillis - rangeSelector.Window.Milliseconds()
	readOpts := query.TimeRange(readStart, endMillis)
	if rangeSelector.MaxSampleGap > 0 {
		readOpts = readOpts.WithMaxSampleGap(rangeSelector.MaxSampleGap)
	}
	s.mu.RLock()
	paths, ranges, err := s.blocksAndRangesForQueryLocked(rangeSelector.Selector, readOpts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 1 && s.engine.BufferedSeries() == 0 && rangeSelector.MaxSampleGap <= 0 {
		dir, err := s.readDirectory(paths[0])
		if err != nil {
			return nil, err
		}
		return query.RateByLabelStepsInBlockWithDirectory(paths[0], dir, rangeSelector.Selector, label, startMillis, endMillis, step, rangeSelector.Window)
	}
	steps, err := query.StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if rangeSelector.MaxSampleGap <= 0 && s.shouldTryRangeStepSummaries(len(steps), len(paths), ranges, rangeSelector.Window) {
		seriesSummaries, err := s.collectRangeStepSummaries(paths, rangeSelector.Selector, readOpts, steps, rangeSelector.Window)
		if err != nil {
			return nil, err
		}
		out := makeFloatSteps(steps)
		if !hasOverlappingRangeStepChunks(seriesSummaries) {
			for _, series := range seriesSummaries {
				addRateSummarySteps(out, series, label)
			}
			return out, nil
		}
	}
	return s.rateByLabelRangeStepsExact(paths, rangeSelector.Selector, readOpts, steps, rangeSelector.Window, label)
}

// IncreaseByLabelRangeSteps returns the per-group counter increase at each step.
func (s *Store) IncreaseByLabelRangeSteps(rangeSelector query.RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]query.IntStep, error) {
	readStart := startMillis - rangeSelector.Window.Milliseconds()
	readOpts := query.TimeRange(readStart, endMillis)
	if rangeSelector.MaxSampleGap > 0 {
		readOpts = readOpts.WithMaxSampleGap(rangeSelector.MaxSampleGap)
	}
	s.mu.RLock()
	paths, ranges, err := s.blocksAndRangesForQueryLocked(rangeSelector.Selector, readOpts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 1 && s.engine.BufferedSeries() == 0 && rangeSelector.MaxSampleGap <= 0 {
		dir, err := s.readDirectory(paths[0])
		if err != nil {
			return nil, err
		}
		return query.IncreaseByLabelStepsInBlockWithDirectory(paths[0], dir, rangeSelector.Selector, label, startMillis, endMillis, step, rangeSelector.Window)
	}
	steps, err := query.StepMillis(startMillis, endMillis, step)
	if err != nil {
		return nil, err
	}
	if rangeSelector.MaxSampleGap <= 0 && s.shouldTryRangeStepSummaries(len(steps), len(paths), ranges, rangeSelector.Window) {
		seriesSummaries, err := s.collectRangeStepSummaries(paths, rangeSelector.Selector, readOpts, steps, rangeSelector.Window)
		if err != nil {
			return nil, err
		}
		out := makeIntSteps(steps)
		if !hasOverlappingRangeStepChunks(seriesSummaries) {
			for _, series := range seriesSummaries {
				addIncreaseSummarySteps(out, series, label)
			}
			return out, nil
		}
	}
	return s.increaseByLabelRangeStepsExact(paths, rangeSelector.Selector, readOpts, steps, rangeSelector.Window, label)
}

// IncreaseByLabel returns the per-group counter increase over the window in opts.
func (s *Store) IncreaseByLabel(selector index.Selector, opts query.Options, label string) (map[string]int64, error) {
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return query.IncreaseByLabelInSeries(s.headSnapshot(selector), selector, opts, label)
	}
	if len(paths) == 1 && s.engine.BufferedSeries() == 0 {
		dir, err := s.readDirectory(paths[0])
		if err != nil {
			return nil, err
		}
		return query.IncreaseByLabelInBlockWithDirectory(paths[0], dir, selector, opts, label)
	}
	if opts.MaxSampleGapMillis != nil {
		return s.increaseByLabelExact(selector, opts, label)
	}
	seriesRates := make(map[uint64]seriesRate)
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return nil, err
		}
		summaries, err := query.RateSummariesInBlockWithDirectory(path, dir, selector, opts)
		if err != nil {
			return nil, err
		}
		for _, summary := range summaries {
			addRateSummary(seriesRates, summary, summary.Labels)
		}
	}
	headSummaries, err := query.RateSummariesInSeries(s.headSnapshot(selector), selector, opts)
	if err != nil {
		return nil, err
	}
	for _, summary := range headSummaries {
		addRateSummary(seriesRates, summary, summary.Labels)
	}
	if hasOverlappingRateChunks(seriesRates) {
		return s.increaseByLabelExact(selector, opts, label)
	}
	return finalizeIncreaseByLabel(seriesRates, label), nil
}

// IncreaseByLabelRange returns the per-group counter increase over the range
// selector's window ending at endMillis.
func (s *Store) IncreaseByLabelRange(rangeSelector query.RangeSelector, endMillis int64, label string) (map[string]int64, error) {
	return s.IncreaseByLabel(rangeSelector.Selector, rangeSelector.Options(endMillis), label)
}

// Stats returns block totals and head buffer counts. Block sample counts come
// from the manifest (recorded at write time), so this is O(blocks), not a scan.
func (s *Store) Stats() (Stats, error) {
	s.mu.RLock()
	blocks := append([]BlockMeta(nil), s.manifest.Blocks...)
	s.mu.RUnlock()

	stats := Stats{
		Blocks:          len(blocks),
		BufferedSeries:  s.engine.BufferedSeries(),
		BufferedSamples: s.engine.BufferedSamples(),
	}
	for _, meta := range blocks {
		path := filepath.Join(s.dir, meta.Path)
		info, err := os.Stat(path)
		if err != nil {
			return Stats{}, err
		}
		stats.Bytes += info.Size()
		stats.Series += meta.SeriesCount
		if !stats.HasTime {
			stats.MinTime = meta.MinTime
			stats.MaxTime = meta.MaxTime
			stats.HasTime = true
		} else {
			if meta.MinTime < stats.MinTime {
				stats.MinTime = meta.MinTime
			}
			if meta.MaxTime > stats.MaxTime {
				stats.MaxTime = meta.MaxTime
			}
		}
		if meta.Kind != "" {
			continue
		}
		if meta.SampleCount > 0 || meta.SeriesCount == 0 {
			stats.Samples += meta.SampleCount
			continue
		}
		// Legacy manifest entry without sample_count: one directory read.
		// Loading every directory here is what Stats must never go back to -
		// 100k-series directories x all blocks blew the heap through the
		// soft memory limit and a stats call took minutes of GC assist.
		// Reuse a warm directory if the query path already cached it (peek,
		// so the probe never skews the cache metrics), but never populate or
		// evict here: a stats call must not disturb the directories the query
		// path is using just to count one legacy block's samples.
		s.mu.RLock()
		dir, cached := s.directoryCache.peek(path)
		s.mu.RUnlock()
		if !cached {
			var err error
			if dir, err = block.ReadDirectory(path); err != nil {
				return Stats{}, err
			}
		}
		stats.Samples += directorySampleCount(dir)
	}
	return stats, nil
}

// LastBackgroundError returns the most recent error from the background flush,
// retention, or eviction loops, or nil.
func (s *Store) LastBackgroundError() error {
	s.backgroundErrMu.RLock()
	defer s.backgroundErrMu.RUnlock()
	return s.backgroundErr
}

func (s *Store) setBackgroundError(err error) {
	if err == nil || errors.Is(err, ErrNoSamples) {
		return
	}
	s.backgroundErrMu.Lock()
	s.backgroundErr = err
	s.backgroundErrMu.Unlock()
}

func (s *Store) readDirectory(path string) (block.Directory, error) {
	s.mu.RLock()
	dir, ok := s.directoryCache.get(path)
	s.mu.RUnlock()
	if ok {
		return dir, nil
	}
	v, err, _ := s.blockLoads.Do("dir:"+path, func() (any, error) {
		// An earlier flight for this path may have completed and cached the
		// directory between our get miss above and entering this fn; peek (no
		// counter side effect) so we don't re-decode a now-warm block.
		s.mu.RLock()
		cachedDir, ok := s.directoryCache.peek(path)
		s.mu.RUnlock()
		if ok {
			return cachedDir, nil
		}
		dir, err := block.ReadDirectory(path)
		if err != nil {
			return block.Directory{}, err
		}
		s.mu.Lock()
		s.directoryCache.put(path, dir)
		s.mu.Unlock()
		return dir, nil
	})
	if err != nil {
		return block.Directory{}, err
	}
	return v.(block.Directory), nil
}

// readResidentBlock returns the compact resident index for a block, building it
// once from the directory on a cache miss. The transient decoded Directory is
// released after the build, so the resident path neither pollutes nor depends on
// the dirCache; subsequent range-step queries reuse the resident form with no
// directory decode (the campaign query's dominant cost).
func (s *Store) readResidentBlock(path string) (*block.ResidentBlock, error) {
	s.mu.RLock()
	rb, ok := s.residentCache.get(path)
	s.mu.RUnlock()
	if ok {
		return rb, nil
	}
	v, err, _ := s.blockLoads.Do("res:"+path, func() (any, error) {
		// See readDirectory: re-check via peek so a flight that raced an
		// earlier one to completion doesn't rebuild a now-cached block.
		s.mu.RLock()
		cachedRB, ok := s.residentCache.peek(path)
		s.mu.RUnlock()
		if ok {
			return cachedRB, nil
		}
		dir, err := block.ReadDirectory(path)
		if err != nil {
			return nil, err
		}
		rb := block.BuildResidentFromDirectory(dir)
		s.mu.Lock()
		s.residentCache.put(path, rb)
		s.mu.Unlock()
		return rb, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*block.ResidentBlock), nil
}

func metaMatchesTime(meta BlockMeta, opts query.Options) bool {
	if opts.StartMillis != nil && meta.MaxTime < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && meta.MinTime > *opts.EndMillis {
		return false
	}
	return true
}

func metaMatchesSelector(meta BlockMeta, selector index.Selector) bool {
	if len(selector.Matchers) == 0 || len(meta.LabelValues) == 0 {
		return true
	}
	for _, matcher := range selector.Matchers {
		if !supportedManifestMatcher(matcher.Op) {
			return true
		}
		values, ok := meta.LabelValues[matcher.Name]
		if !ok {
			return matcher.Op == index.MatchNotEqual || matcher.Op == index.MatchNotRegexp
		}
		if !containsMatchingValue(values, matcher) {
			return false
		}
	}
	return true
}

func containsMatchingValue(values []string, matcher index.Matcher) bool {
	if matcher.Op == index.MatchEqual {
		i := sort.SearchStrings(values, matcher.Value)
		return i < len(values) && values[i] == matcher.Value
	}
	if matcher.Op == index.MatchNotEqual {
		for _, value := range values {
			if value != matcher.Value {
				return true
			}
		}
		return false
	}
	for _, value := range values {
		if matcher.Matches(value) {
			return true
		}
	}
	return false
}

func supportedManifestMatcher(op index.MatchOp) bool {
	return op == index.MatchEqual || op == index.MatchRegexp || op == index.MatchNotEqual || op == index.MatchNotRegexp
}

func validateLabels(labels model.LabelSet, opts Options) error {
	if opts.MaxLabelsPerSeries > 0 && len(labels) > opts.MaxLabelsPerSeries {
		return fmt.Errorf("%w: labels %d max %d", ErrLabelLimitExceeded, len(labels), opts.MaxLabelsPerSeries)
	}
	canonical := labels.Canonical()
	for i, label := range canonical {
		if label.Name == "" {
			return fmt.Errorf("%w: empty label name", ErrInvalidLabels)
		}
		if opts.MaxLabelNameBytes > 0 && len(label.Name) > opts.MaxLabelNameBytes {
			return fmt.Errorf("%w: label name %q bytes %d max %d", ErrLabelLimitExceeded, label.Name, len(label.Name), opts.MaxLabelNameBytes)
		}
		if opts.MaxLabelValueBytes > 0 && len(label.Value) > opts.MaxLabelValueBytes {
			return fmt.Errorf("%w: label %q value bytes %d max %d", ErrLabelLimitExceeded, label.Name, len(label.Value), opts.MaxLabelValueBytes)
		}
		if i > 0 && canonical[i-1].Name == label.Name {
			return fmt.Errorf("%w: duplicate label name %q", ErrInvalidLabels, label.Name)
		}
	}
	return nil
}

// dropExpiredSamples returns the samples at or after cutoffMillis. Block
// samples are stored sorted by timestamp, so this is a binary search plus a
// subslice; unsorted input (not produced by WriteFile) still works because
// the slow path filters element-wise.
func dropExpiredSamples(timestamps []int64, values []int64, cutoffMillis int64) ([]int64, []int64) {
	if len(timestamps) == 0 || len(timestamps) != len(values) {
		return timestamps, values
	}
	if sort.SliceIsSorted(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] }) {
		i := sort.Search(len(timestamps), func(i int) bool { return timestamps[i] >= cutoffMillis })
		return timestamps[i:], values[i:]
	}
	keptTS := make([]int64, 0, len(timestamps))
	keptVals := make([]int64, 0, len(values))
	for i, ts := range timestamps {
		if ts >= cutoffMillis {
			keptTS = append(keptTS, ts)
			keptVals = append(keptVals, values[i])
		}
	}
	return keptTS, keptVals
}

type compactSample struct {
	timestamp int64
	value     int64
}

func compactSeriesSamples(series block.Series) block.Series {
	if len(series.Timestamps) != len(series.Values) || len(series.Timestamps) <= 1 {
		return series
	}
	samples := make([]compactSample, len(series.Timestamps))
	for i := range series.Timestamps {
		samples[i] = compactSample{timestamp: series.Timestamps[i], value: series.Values[i]}
	}
	sort.SliceStable(samples, func(i, j int) bool {
		return samples[i].timestamp < samples[j].timestamp
	})

	out := series
	out.Timestamps = make([]int64, 0, len(samples))
	out.Values = make([]int64, 0, len(samples))
	for _, sample := range samples {
		last := len(out.Timestamps) - 1
		if last >= 0 && out.Timestamps[last] == sample.timestamp {
			out.Values[last] = sample.value
			continue
		}
		out.Timestamps = append(out.Timestamps, sample.timestamp)
		out.Values = append(out.Values, sample.value)
	}
	return out
}

func mergeAggregates(out map[string]query.Aggregate, partial map[string]query.Aggregate) {
	for key, agg := range partial {
		out[key] = mergeAggregateValue(out[key], agg)
	}
}

func mergeAggregateValue(current query.Aggregate, next query.Aggregate) query.Aggregate {
	if next.Count == 0 {
		return current
	}
	if current.Count == 0 {
		return next
	}
	if next.Min < current.Min {
		current.Min = next.Min
	}
	if next.Max > current.Max {
		current.Max = next.Max
	}
	current.Sum += next.Sum
	current.Count += next.Count
	return current
}

type seriesRate struct {
	labels     model.LabelSet
	summary    query.RateSummary
	hasRate    bool
	hasOverlap bool
}

type rangeStepRateSeries struct {
	labels     model.LabelSet
	summaries  []query.RateSummary
	hasOverlap bool
}

func addRateSummary(seriesRates map[uint64]seriesRate, summary query.RateSummary, labels model.LabelSet) {
	current := seriesRates[summary.SeriesID]
	if len(current.labels) == 0 {
		current.labels = labels
	}
	if !current.hasRate {
		current.summary = summary
		current.hasRate = true
		seriesRates[summary.SeriesID] = current
		return
	}
	if rateSummariesOverlap(current.summary, summary) {
		current.hasOverlap = true
	}
	current.summary = mergeRateSummaryPair(current.summary, summary)
	seriesRates[summary.SeriesID] = current
}

func hasOverlappingRateChunks(seriesRates map[uint64]seriesRate) bool {
	for _, series := range seriesRates {
		if series.hasOverlap {
			return true
		}
	}
	return false
}

func finalizeIncreaseByLabel(seriesRates map[uint64]seriesRate, groupLabel string) map[string]int64 {
	out := make(map[string]int64)
	for _, series := range seriesRates {
		if !series.hasRate {
			continue
		}
		key, ok := series.labels.Get(groupLabel)
		if !ok {
			key = ""
		}
		out[key] += series.summary.Increase
	}
	return out
}

func makeFloatSteps(steps []int64) []query.FloatStep {
	out := make([]query.FloatStep, len(steps))
	for i, ts := range steps {
		out[i] = query.FloatStep{TimestampMillis: ts, Values: make(map[string]float64)}
	}
	return out
}

func makeIntSteps(steps []int64) []query.IntStep {
	out := make([]query.IntStep, len(steps))
	for i, ts := range steps {
		out[i] = query.IntStep{TimestampMillis: ts, Values: make(map[string]int64)}
	}
	return out
}

func makeAggregateSteps(steps []int64) []query.AggregateStep {
	out := make([]query.AggregateStep, len(steps))
	for i, ts := range steps {
		out[i] = query.AggregateStep{TimestampMillis: ts, Values: make(map[string]query.Aggregate)}
	}
	return out
}

func mergeRateSummaryPair(current query.RateSummary, next query.RateSummary) query.RateSummary {
	if next.Count == 0 {
		return current
	}
	if current.Count == 0 {
		return next
	}
	if next.FirstMillis < current.FirstMillis || (next.FirstMillis == current.FirstMillis && next.LastMillis < current.LastMillis) {
		return mergeRateSummaryPair(next, current)
	}
	if next.FirstMillis > current.LastMillis {
		if delta := next.FirstValue - current.LastValue; delta > 0 {
			current.Increase += delta
		} else if delta < 0 {
			current.ResetCount++
		}
	}
	if next.FirstMillis >= current.LastMillis {
		current.Increase += next.Increase
		current.ResetCount += next.ResetCount
	}
	if next.LastMillis >= current.LastMillis {
		current.LastMillis = next.LastMillis
		current.LastValue = next.LastValue
	}
	current.Count += next.Count
	return current
}

func rateSummariesOverlap(a query.RateSummary, b query.RateSummary) bool {
	return a.FirstMillis < b.LastMillis && b.FirstMillis < a.LastMillis
}

type rateSample struct {
	timestamp int64
	value     int64
}

type exactRateSeries struct {
	labels  model.LabelSet
	samples []rateSample
}

func coalesceDecodedSeries(chunks []block.DecodedSeries) []block.DecodedSeries {
	collector := newExactSeriesCollector(len(chunks), nil)
	for _, chunk := range chunks {
		_ = collector.addDecodedSamples(chunk, query.Options{})
	}
	out := make([]block.DecodedSeries, 0, len(collector.grouped))
	for seriesID, series := range collector.grouped {
		out = append(out, decodedRateSeries(seriesID, series))
	}
	return out
}

// groupedRateSeries is the resident-path counterpart of exactRateSeries: it
// carries the resolved group-label value instead of a full LabelSet, so the scan
// never materialises model.Label strings for the four non-group labels of every
// series (the campaign metric matches all 100k series of one metric name).
type groupedRateSeries struct {
	groupKey string
	samples  []rateSample
}

// collectRangeStepSeriesGrouped is the resident-index range-step collector. It
// reads each in-window block's compact resident index (built once, cached) and
// scans matching series by dict code, resolving the group value from the dict -
// eliminating the per-query directory decode and the per-series label-string
// allocation that dominated the campaign range-step query.
func (s *Store) collectRangeStepSeriesGrouped(paths []string, selector index.Selector, opts query.Options, groupLabel string) (map[uint64]*groupedRateSeries, error) {
	head := s.headSnapshot(selector)
	grouped := make(map[uint64]*groupedRateSeries, len(head))
	add := func(seriesID uint64, groupKey string, timestamps, values []int64) {
		current := grouped[seriesID]
		if current == nil {
			current = &groupedRateSeries{groupKey: groupKey, samples: make([]rateSample, 0, len(values))}
			grouped[seriesID] = current
		}
		for i, timestamp := range timestamps {
			value := values[i]
			if !sampleMatchesOptions(timestamp, value, opts) {
				continue
			}
			current.samples = append(current.samples, rateSample{timestamp: timestamp, value: value})
		}
	}
	for _, path := range paths {
		rb, err := s.readResidentBlock(path)
		if err != nil {
			return nil, err
		}
		if err := query.ScanResidentBlock(path, rb, selector, opts, groupLabel, func(seriesID uint64, _ model.MetricType, groupValue string, timestamps, values []int64) error {
			add(seriesID, groupValue, timestamps, values)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if err := query.ScanSeries(head, selector, opts, func(series block.DecodedSeries) error {
		key, ok := series.Entry.Labels.Get(groupLabel)
		if !ok {
			key = ""
		}
		add(series.Entry.SeriesID, key, series.Timestamps, series.Values)
		return nil
	}); err != nil {
		return nil, err
	}
	return grouped, nil
}

// maxRangeStepWorkers caps the series-partitioned parallel range-step fan-out.
// Bounded so the partial-result memory (one grouped map per worker) stays small
// under the store's soft memory limit; 8 saturates the typical core count
// without over-subscribing.
const maxRangeStepWorkers = 8

func rangeStepWorkerCount() int {
	w := runtime.GOMAXPROCS(0)
	if w > maxRangeStepWorkers {
		w = maxRangeStepWorkers
	}
	if w < 1 {
		w = 1
	}
	return w
}

type preparedResidentScan struct {
	scan    *query.ResidentScan
	section []byte
}

type headRangeStepMatch struct {
	id         uint64
	groupKey   string
	timestamps []int64
	values     []int64
}

// parallelRangeStepReduce runs the resident range-step exact path partitioned by
// seriesID. Each worker owns a hash partition of series, assembles each of its
// series once from every in-window block's shared read-only chunk section, and
// reduces them into a thread-local accumulator; the accumulators merge at the
// end. This parallelises the residual cost Phase 2 left CPU-bound (chunk decode +
// per-series rate math) without the per-block sample duplication that made the
// earlier block-parallel attempt GC-bound - a series lives in exactly one worker,
// so total sample memory matches the sequential path. ok=false signals the caller
// to use the sequential path (a block too large for an in-memory section).
func parallelRangeStepReduce[A any](
	s *Store,
	paths []string,
	selector index.Selector,
	opts query.Options,
	groupLabel string,
	newAcc func() A,
	newReducer func() func(A, *groupedRateSeries) error,
	mergeAcc func(dst, src A),
) (A, bool, error) {
	var zero A
	head := s.headSnapshot(selector)
	workers := rangeStepWorkerCount()

	// Phase A (sequential, cheap): prepare each block's resident scan and read its
	// chunk section once (shared read-only across workers); pre-filter the head so
	// selector matching runs once, not per worker. Bail to the sequential path if
	// any block is too large for an in-memory section.
	prepared := make([]preparedResidentScan, 0, len(paths))
	for _, path := range paths {
		rb, err := s.readResidentBlock(path)
		if err != nil {
			return zero, false, err
		}
		scan, err := query.PrepareResidentScan(rb, selector, opts, groupLabel)
		if err != nil {
			return zero, false, err
		}
		if !scan.Possible() {
			continue
		}
		section, err := scan.ReadChunkSection(path)
		if err != nil {
			return zero, false, err
		}
		if section == nil {
			return zero, false, nil
		}
		prepared = append(prepared, preparedResidentScan{scan: scan, section: section})
	}

	var headMatches []headRangeStepMatch
	if err := query.ScanSeries(head, selector, opts, func(series block.DecodedSeries) error {
		key, ok := series.Entry.Labels.Get(groupLabel)
		if !ok {
			key = ""
		}
		headMatches = append(headMatches, headRangeStepMatch{
			id:         series.Entry.SeriesID,
			groupKey:   key,
			timestamps: series.Timestamps,
			values:     series.Values,
		})
		return nil
	}); err != nil {
		return zero, false, err
	}

	accs := make([]A, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			acc := newAcc()
			accs[w] = acc
			reduce := newReducer()
			grouped := make(map[uint64]*groupedRateSeries)
			own := func(id uint64) bool { return int(id%uint64(workers)) == w }
			add := func(seriesID uint64, groupKey string, timestamps, values []int64) {
				current := grouped[seriesID]
				if current == nil {
					current = &groupedRateSeries{groupKey: groupKey, samples: make([]rateSample, 0, len(values))}
					grouped[seriesID] = current
				}
				for i, timestamp := range timestamps {
					value := values[i]
					if !sampleMatchesOptions(timestamp, value, opts) {
						continue
					}
					current.samples = append(current.samples, rateSample{timestamp: timestamp, value: value})
				}
			}
			for _, p := range prepared {
				if err := p.scan.ScanSection(p.section, own, func(seriesID uint64, _ model.MetricType, groupValue string, timestamps, values []int64) error {
					add(seriesID, groupValue, timestamps, values)
					return nil
				}); err != nil {
					errs[w] = err
					return
				}
			}
			for _, hm := range headMatches {
				if own(hm.id) {
					add(hm.id, hm.groupKey, hm.timestamps, hm.values)
				}
			}
			for _, gr := range grouped {
				if err := reduce(acc, gr); err != nil {
					errs[w] = err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return zero, false, err
		}
	}
	result := newAcc()
	for _, acc := range accs {
		mergeAcc(result, acc)
	}
	return result, true, nil
}

func mergeFloatSteps(dst, src []query.FloatStep) {
	n := len(dst)
	if len(src) < n {
		n = len(src)
	}
	for i := 0; i < n; i++ {
		for key, value := range src[i].Values {
			dst[i].Values[key] += value
		}
	}
}

func mergeIntSteps(dst, src []query.IntStep) {
	n := len(dst)
	if len(src) < n {
		n = len(src)
	}
	for i := 0; i < n; i++ {
		for key, value := range src[i].Values {
			dst[i].Values[key] += value
		}
	}
}

type exactSeriesCollector struct {
	grouped       map[uint64]*exactRateSeries
	capacityHints map[uint64]int
}

func newExactSeriesCollector(mapHint int, capacityHints map[uint64]int) *exactSeriesCollector {
	return &exactSeriesCollector{
		grouped:       make(map[uint64]*exactRateSeries, mapHint),
		capacityHints: capacityHints,
	}
}

type blockDirectoryTarget struct {
	path    string
	dir     block.Directory
	timeMin int64
	timeMax int64
	hasTime bool
}

func (s *Store) readBlockDirectoryTargets(paths []string) ([]blockDirectoryTarget, error) {
	targets := make([]blockDirectoryTarget, 0, len(paths))
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return nil, err
		}
		timeMin, timeMax, hasTime := dir.TimeRange()
		targets = append(targets, blockDirectoryTarget{
			path:    path,
			dir:     dir,
			timeMin: timeMin,
			timeMax: timeMax,
			hasTime: hasTime,
		})
	}
	return targets, nil
}

func blockTargetsHaveStrictOverlap(targets []blockDirectoryTarget) bool {
	ordered := append([]blockDirectoryTarget(nil), targets...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].timeMin == ordered[j].timeMin {
			return ordered[i].path < ordered[j].path
		}
		return ordered[i].timeMin < ordered[j].timeMin
	})
	hasPrevious := false
	previousMax := int64(0)
	for _, target := range ordered {
		if !target.hasTime {
			continue
		}
		if hasPrevious && target.timeMin <= previousMax {
			return true
		}
		if !hasPrevious || target.timeMax > previousMax {
			previousMax = target.timeMax
			hasPrevious = true
		}
	}
	return false
}

func aggregateStepBucketsCoverTargets(targets []blockDirectoryTarget, selector index.Selector, label string, steps []int64, window time.Duration) bool {
	if blockTargetsHaveStrictOverlap(targets) {
		return false
	}
	for _, target := range targets {
		if _, ok := query.AggregateByLabelStepsInDirectoryBuckets(target.dir, selector, label, steps, window); !ok {
			return false
		}
	}
	return true
}

func (s *Store) aggregateByLabelRangeStepsBlockMerge(paths []string, selector index.Selector, steps []int64, window time.Duration, label string) ([]query.AggregateStep, bool, error) {
	targets, err := s.readBlockDirectoryTargets(paths)
	if err != nil {
		return nil, false, err
	}
	if !aggregateStepBucketsCoverTargets(targets, selector, label, steps, window) {
		return nil, false, nil
	}
	out := makeAggregateSteps(steps)
	for _, target := range targets {
		partial, _ := query.AggregateByLabelStepsInDirectoryBuckets(target.dir, selector, label, steps, window)
		mergeAggregateSteps(out, partial)
	}
	return out, true, nil
}

func mergeAggregateSteps(out []query.AggregateStep, partial []query.AggregateStep) {
	limit := len(out)
	if len(partial) < limit {
		limit = len(partial)
	}
	for i := 0; i < limit; i++ {
		for key, agg := range partial[i].Values {
			out[i].Values[key] = mergeAggregateValue(out[i].Values[key], agg)
		}
	}
}

func (c *exactSeriesCollector) addDecodedSamples(chunk block.DecodedSeries, opts query.Options) error {
	if len(chunk.Timestamps) != len(chunk.Values) {
		return errors.New("store: timestamp/value length mismatch")
	}
	var current *exactRateSeries
	for i, timestamp := range chunk.Timestamps {
		value := chunk.Values[i]
		if !sampleMatchesOptions(timestamp, value, opts) {
			continue
		}
		if current == nil {
			current = c.grouped[chunk.Entry.SeriesID]
			if current == nil {
				capacity := len(chunk.Values)
				if c.capacityHints != nil && c.capacityHints[chunk.Entry.SeriesID] > capacity {
					capacity = c.capacityHints[chunk.Entry.SeriesID]
				}
				current = &exactRateSeries{
					labels:  chunk.Entry.Labels,
					samples: make([]rateSample, 0, capacity),
				}
				c.grouped[chunk.Entry.SeriesID] = current
			}
		}
		current.samples = append(current.samples, rateSample{timestamp: timestamp, value: value})
	}
	return nil
}

func decodedRateSeries(seriesID uint64, series *exactRateSeries) block.DecodedSeries {
	sort.SliceStable(series.samples, func(i, j int) bool {
		return series.samples[i].timestamp < series.samples[j].timestamp
	})
	decoded := block.DecodedSeries{
		Entry: block.DirectoryEntry{
			SeriesID: seriesID,
			Labels:   series.labels,
		},
		Timestamps: make([]int64, 0, len(series.samples)),
		Values:     make([]int64, 0, len(series.samples)),
	}
	for _, sample := range series.samples {
		last := len(decoded.Timestamps) - 1
		if last >= 0 && decoded.Timestamps[last] == sample.timestamp {
			decoded.Values[last] = sample.value
			continue
		}
		decoded.Timestamps = append(decoded.Timestamps, sample.timestamp)
		decoded.Values = append(decoded.Values, sample.value)
	}
	return decoded
}

func sampleMatchesOptions(timestamp int64, value int64, opts query.Options) bool {
	if opts.StartMillis != nil && timestamp < *opts.StartMillis {
		return false
	}
	if opts.EndMillis != nil && timestamp > *opts.EndMillis {
		return false
	}
	if opts.MinValue != nil && value < *opts.MinValue {
		return false
	}
	if opts.MaxValue != nil && value > *opts.MaxValue {
		return false
	}
	return true
}

func (s *Store) collectRangeStepSummaries(paths []string, selector index.Selector, opts query.Options, steps []int64, window time.Duration) (map[uint64]*rangeStepRateSeries, error) {
	grouped := make(map[uint64]*rangeStepRateSeries)
	// One scratch buffer reused across every series of every block - the scan
	// callbacks run sequentially, so rateSummariesForSteps stops allocating a
	// fresh summary/prefix set per series (the qm2 hotspot).
	buf := &query.RateStepBuf{}
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return nil, err
		}
		if err := query.ScanBlockWithDirectoryShared(path, dir, selector, opts, func(series block.DecodedSeries) error {
			return addRangeStepSummaries(grouped, buf, series, steps, window, opts)
		}); err != nil {
			return nil, err
		}
	}
	if err := query.ScanSeries(s.headSnapshot(selector), selector, opts, func(series block.DecodedSeries) error {
		return addRangeStepSummaries(grouped, buf, series, steps, window, opts)
	}); err != nil {
		return nil, err
	}
	return grouped, nil
}

func addRangeStepSummaries(grouped map[uint64]*rangeStepRateSeries, buf *query.RateStepBuf, series block.DecodedSeries, steps []int64, window time.Duration, opts query.Options) error {
	summaries, err := query.RateWindowSummariesForStepsReuse(buf, series.Entry.SeriesID, series.Timestamps, series.Values, steps, window, opts)
	if err != nil {
		return err
	}
	current := grouped[series.Entry.SeriesID]
	if current == nil {
		current = &rangeStepRateSeries{
			labels:    series.Entry.Labels,
			summaries: make([]query.RateSummary, len(steps)),
		}
		grouped[series.Entry.SeriesID] = current
	}
	for i, summary := range summaries {
		if summary.Count == 0 {
			continue
		}
		if current.summaries[i].Count == 0 {
			current.summaries[i] = summary
			continue
		}
		if rateSummariesOverlap(current.summaries[i], summary) {
			current.hasOverlap = true
		}
		current.summaries[i] = mergeRateSummaryPair(current.summaries[i], summary)
	}
	return nil
}

func hasOverlappingRangeStepChunks(seriesSummaries map[uint64]*rangeStepRateSeries) bool {
	for _, series := range seriesSummaries {
		if series.hasOverlap {
			return true
		}
	}
	return false
}

func addRateSummarySteps(out []query.FloatStep, series *rangeStepRateSeries, groupLabel string) {
	key, ok := series.labels.Get(groupLabel)
	if !ok {
		key = ""
	}
	for i, summary := range series.summaries {
		if summary.Count == 0 {
			continue
		}
		rate, ok, err := query.RateFromSummary(summary)
		if err != nil || !ok {
			continue
		}
		out[i].Values[key] += rate
	}
}

func addIncreaseSummarySteps(out []query.IntStep, series *rangeStepRateSeries, groupLabel string) {
	key, ok := series.labels.Get(groupLabel)
	if !ok {
		key = ""
	}
	for i, summary := range series.summaries {
		if summary.Count == 0 {
			continue
		}
		out[i].Values[key] += summary.Increase
	}
}

func preferRangeStepSummaries(stepCount int, blockCount int, headSeries int) bool {
	// Shares query.SummaryStepCeiling with the planner so Explain matches the
	// path that actually runs. Beyond the ceiling the steps x series summary
	// map outgrows the exact materialization it would replace.
	return stepCount <= query.SummaryStepCeiling && blockCount > 1 && headSeries == 0
}

// shouldTryRangeStepSummaries decides whether to run the per-step summary pass
// before the exact path. The summary pass decodes every in-window block and
// computes a windowed rate summary per series per step; at campaign scale
// (100k series, ~15 blocks) that per-step windowing is slower than the streamed
// exact path, and when step windows straddle block boundaries it is discarded
// for the exact path anyway. So when block metadata proves a window can straddle
// (the continuous-ingest shape), skip the summary pass and go straight to exact.
// ranges is nil under the glob fallback, where we keep the summary path's own
// post-hoc overlap check.
func (s *Store) shouldTryRangeStepSummaries(stepCount, blockCount int, ranges []blockTimeRange, window time.Duration) bool {
	if !preferRangeStepSummaries(stepCount, blockCount, s.engine.BufferedSeries()) {
		return false
	}
	if ranges != nil && blockWindowsCanStraddle(ranges, window) {
		return false
	}
	return true
}

func addExactRateSampleSteps(out []query.FloatStep, steps []int64, rawSamples []rateSample, key string, window time.Duration, opts query.Options) error {
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return errors.New("store: window must be at least 1ms")
	}
	samples := compactExactRateSamples(rawSamples)
	if len(samples) < 2 {
		return nil
	}
	increasePrefix := counterIncreasePrefix(samples)
	stalePrefix := staleGapPrefix(samples, opts)
	lo := 0
	hi := -1
	for stepIndex, stepMillis := range steps {
		windowStart := stepMillis - windowMillis
		for lo < len(samples) && samples[lo].timestamp < windowStart {
			lo++
		}
		if hi < lo-1 {
			hi = lo - 1
		}
		for hi+1 < len(samples) && samples[hi+1].timestamp <= stepMillis {
			hi++
		}
		if hi-lo+1 < 2 {
			continue
		}
		if stalePrefixHasGap(stalePrefix, lo, hi) {
			continue
		}
		dtMillis := samples[hi].timestamp - samples[lo].timestamp
		if dtMillis <= 0 {
			continue
		}
		out[stepIndex].Values[key] += float64(increasePrefix[hi]-increasePrefix[lo]) / (float64(dtMillis) / 1000.0)
	}
	return nil
}

func addExactIncreaseSampleSteps(out []query.IntStep, steps []int64, rawSamples []rateSample, key string, window time.Duration, opts query.Options) error {
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return errors.New("store: window must be at least 1ms")
	}
	samples := compactExactRateSamples(rawSamples)
	if len(samples) < 2 {
		return nil
	}
	increasePrefix := counterIncreasePrefix(samples)
	stalePrefix := staleGapPrefix(samples, opts)
	lo := 0
	hi := -1
	for stepIndex, stepMillis := range steps {
		windowStart := stepMillis - windowMillis
		for lo < len(samples) && samples[lo].timestamp < windowStart {
			lo++
		}
		if hi < lo-1 {
			hi = lo - 1
		}
		for hi+1 < len(samples) && samples[hi+1].timestamp <= stepMillis {
			hi++
		}
		if hi-lo+1 < 2 {
			continue
		}
		if stalePrefixHasGap(stalePrefix, lo, hi) {
			continue
		}
		out[stepIndex].Values[key] += increasePrefix[hi] - increasePrefix[lo]
	}
	return nil
}

type aggregateStepWorkspace struct {
	prefix   []int64
	minDeque []int
	maxDeque []int
}

func addExactAggregateSampleSteps(out []query.AggregateStep, steps []int64, rawSamples []rateSample, key string, window time.Duration, workspace *aggregateStepWorkspace) error {
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return errors.New("store: window must be at least 1ms")
	}
	samples := compactExactRateSamples(rawSamples)
	if len(samples) == 0 {
		return nil
	}
	if workspace == nil {
		workspace = &aggregateStepWorkspace{}
	}
	if cap(workspace.prefix) < len(samples)+1 {
		workspace.prefix = make([]int64, len(samples)+1)
	}
	prefix := workspace.prefix[:len(samples)+1]
	prefix[0] = 0
	for i, sample := range samples {
		prefix[i+1] = prefix[i] + sample.value
	}
	minDeque := workspace.minDeque[:0]
	maxDeque := workspace.maxDeque[:0]
	lo := 0
	hi := -1
	for stepIndex, stepMillis := range steps {
		windowStart := stepMillis - windowMillis
		for lo < len(samples) && samples[lo].timestamp < windowStart {
			lo++
		}
		for len(minDeque) > 0 && minDeque[0] < lo {
			minDeque = minDeque[1:]
		}
		for len(maxDeque) > 0 && maxDeque[0] < lo {
			maxDeque = maxDeque[1:]
		}
		if hi < lo-1 {
			hi = lo - 1
		}
		for hi+1 < len(samples) && samples[hi+1].timestamp <= stepMillis {
			hi++
			for len(minDeque) > 0 && samples[minDeque[len(minDeque)-1]].value >= samples[hi].value {
				minDeque = minDeque[:len(minDeque)-1]
			}
			minDeque = append(minDeque, hi)
			for len(maxDeque) > 0 && samples[maxDeque[len(maxDeque)-1]].value <= samples[hi].value {
				maxDeque = maxDeque[:len(maxDeque)-1]
			}
			maxDeque = append(maxDeque, hi)
		}
		count := hi - lo + 1
		if count <= 0 {
			continue
		}
		agg := query.Aggregate{
			Sum:   prefix[hi+1] - prefix[lo],
			Count: count,
			Min:   samples[minDeque[0]].value,
			Max:   samples[maxDeque[0]].value,
		}
		out[stepIndex].Values[key] = mergeAggregateValue(out[stepIndex].Values[key], agg)
	}
	workspace.minDeque = minDeque[:0]
	workspace.maxDeque = maxDeque[:0]
	return nil
}

func compactExactRateSamples(samples []rateSample) []rateSample {
	if len(samples) == 0 {
		return nil
	}
	sort.SliceStable(samples, func(i, j int) bool {
		return samples[i].timestamp < samples[j].timestamp
	})
	write := 0
	for _, sample := range samples {
		if write > 0 && samples[write-1].timestamp == sample.timestamp {
			samples[write-1].value = sample.value
			continue
		}
		samples[write] = sample
		write++
	}
	return samples[:write]
}

func counterIncreasePrefix(samples []rateSample) []int64 {
	prefix := make([]int64, len(samples))
	for i := 1; i < len(samples); i++ {
		prefix[i] = prefix[i-1]
		if delta := samples[i].value - samples[i-1].value; delta > 0 {
			prefix[i] += delta
		}
	}
	return prefix
}

func staleGapPrefix(samples []rateSample, opts query.Options) []int {
	if opts.MaxSampleGapMillis == nil {
		return nil
	}
	prefix := make([]int, len(samples))
	for i := 1; i < len(samples); i++ {
		prefix[i] = prefix[i-1]
		if samples[i].timestamp-samples[i-1].timestamp > *opts.MaxSampleGapMillis {
			prefix[i]++
		}
	}
	return prefix
}

func stalePrefixHasGap(prefix []int, lo int, hi int) bool {
	return prefix != nil && prefix[hi]-prefix[lo] > 0
}

func (s *Store) rateByLabelRangeStepsExact(paths []string, selector index.Selector, opts query.Options, steps []int64, window time.Duration, label string) ([]query.FloatStep, error) {
	out, ok, err := parallelRangeStepReduce(s, paths, selector, opts, label,
		func() []query.FloatStep { return makeFloatSteps(steps) },
		func() func([]query.FloatStep, *groupedRateSeries) error {
			return func(acc []query.FloatStep, gr *groupedRateSeries) error {
				return addExactRateSampleSteps(acc, steps, gr.samples, gr.groupKey, window, opts)
			}
		},
		mergeFloatSteps)
	if err != nil {
		return nil, err
	}
	if ok {
		return out, nil
	}
	grouped, err := s.collectRangeStepSeriesGrouped(paths, selector, opts, label)
	if err != nil {
		return nil, err
	}
	out = makeFloatSteps(steps)
	for _, series := range grouped {
		if err := addExactRateSampleSteps(out, steps, series.samples, series.groupKey, window, opts); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) increaseByLabelRangeStepsExact(paths []string, selector index.Selector, opts query.Options, steps []int64, window time.Duration, label string) ([]query.IntStep, error) {
	out, ok, err := parallelRangeStepReduce(s, paths, selector, opts, label,
		func() []query.IntStep { return makeIntSteps(steps) },
		func() func([]query.IntStep, *groupedRateSeries) error {
			return func(acc []query.IntStep, gr *groupedRateSeries) error {
				return addExactIncreaseSampleSteps(acc, steps, gr.samples, gr.groupKey, window, opts)
			}
		},
		mergeIntSteps)
	if err != nil {
		return nil, err
	}
	if ok {
		return out, nil
	}
	grouped, err := s.collectRangeStepSeriesGrouped(paths, selector, opts, label)
	if err != nil {
		return nil, err
	}
	out = makeIntSteps(steps)
	for _, series := range grouped {
		if err := addExactIncreaseSampleSteps(out, steps, series.samples, series.groupKey, window, opts); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func hasPointFilters(opts query.Options) bool {
	return opts.StartMillis != nil || opts.EndMillis != nil || opts.MinValue != nil || opts.MaxValue != nil
}

func (s *Store) rateByLabelExact(selector index.Selector, opts query.Options, label string) (map[string]float64, error) {
	series, err := s.Select(selector, opts)
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64)
	for _, series := range coalesceDecodedSeries(series) {
		summary, ok, err := query.RateSummaryForSamples(0, series.Timestamps, series.Values, opts)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		rate, ok, err := query.RateFromSummary(summary)
		if err != nil || !ok {
			continue
		}
		key, ok := series.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += rate
	}
	return out, nil
}

func (s *Store) increaseByLabelExact(selector index.Selector, opts query.Options, label string) (map[string]int64, error) {
	series, err := s.Select(selector, opts)
	if err != nil {
		return nil, err
	}
	out := make(map[string]int64)
	for _, series := range coalesceDecodedSeries(series) {
		summary, ok, err := query.RateSummaryForSamples(0, series.Timestamps, series.Values, opts)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		key, ok := series.Entry.Labels.Get(label)
		if !ok {
			key = ""
		}
		out[key] += summary.Increase
	}
	return out, nil
}

func (s *Store) aggregateByLabelRangeStepsExact(paths []string, selector index.Selector, opts query.Options, steps []int64, window time.Duration, label string) ([]query.AggregateStep, error) {
	out, ok, err := parallelRangeStepReduce(s, paths, selector, opts, label,
		func() []query.AggregateStep { return makeAggregateSteps(steps) },
		func() func([]query.AggregateStep, *groupedRateSeries) error {
			workspace := &aggregateStepWorkspace{}
			return func(acc []query.AggregateStep, gr *groupedRateSeries) error {
				return addExactAggregateSampleSteps(acc, steps, gr.samples, gr.groupKey, window, workspace)
			}
		},
		mergeAggregateSteps)
	if err != nil {
		return nil, err
	}
	if ok {
		return out, nil
	}
	grouped, err := s.collectRangeStepSeriesGrouped(paths, selector, opts, label)
	if err != nil {
		return nil, err
	}
	out = makeAggregateSteps(steps)
	workspace := &aggregateStepWorkspace{}
	for _, series := range grouped {
		if err := addExactAggregateSampleSteps(out, steps, series.samples, series.groupKey, window, workspace); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) startBackground() {
	if s.opts.FlushInterval <= 0 && s.opts.Retention <= 0 && s.opts.CompactionMinBlocks <= 0 {
		return
	}
	s.stopBackground = make(chan struct{})
	s.backgroundDone = make(chan struct{})
	go s.backgroundLoop()
}

func (s *Store) backgroundLoop() {
	defer close(s.backgroundDone)

	interval := s.opts.FlushInterval
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.setBackgroundError(s.maintenance())
		case <-s.stopBackground:
			return
		}
	}
}

func (s *Store) maintenance() error {
	if s.engine.BufferedSeries() > 0 {
		if _, err := s.Flush(); err != nil && !errors.Is(err, ErrNoSamples) {
			return err
		}
	}
	if s.opts.Retention > 0 {
		cutoff := s.clock().Add(-s.opts.Retention).UnixMilli()
		if _, err := s.DeleteBefore(cutoff); err != nil {
			return err
		}
	}
	if s.opts.CompactionMinBlocks > 0 {
		if _, _, err := s.CompactIfNeeded(s.opts.CompactionMinBlocks); err != nil && !errors.Is(err, ErrNoSamples) {
			return err
		}
	}
	return nil
}

// Close stops the background loops, drains and closes the engine (preserving
// durability of acknowledged appends), and releases the directory lock. It is
// idempotent.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		if s.stopBackground != nil {
			close(s.stopBackground)
			<-s.backgroundDone
		}
		s.stopEvictionSweep()
		if s.engine.BufferedSeries() > 0 {
			if _, err := s.Flush(); err != nil && !errors.Is(err, ErrNoSamples) {
				s.closeErr = err
			}
		}
		if err := s.engine.Close(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
		if s.catalogLog != nil {
			if err := s.catalogLog.Close(); err != nil && s.closeErr == nil {
				s.closeErr = err
			}
		}
		if err := s.dirLock.Release(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *Store) nextBlockPath() string {
	return s.nextBlockPathWithPrefix("block")
}

func (s *Store) nextBlockPathWithPrefix(prefix string) string {
	base := s.clock().UTC().Format("20060102T150405.000000000Z")
	path := filepath.Join(s.dir, prefix+"-"+base+".meb")
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return path
	}
	for i := 1; ; i++ {
		candidate := filepath.Join(s.dir, prefix+"-"+base+"-"+strconv.Itoa(i)+".meb")
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}
