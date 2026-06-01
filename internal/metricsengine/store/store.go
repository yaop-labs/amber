package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

var ErrNoSamples = errors.New("store: no buffered samples to flush")
var ErrInvalidLabels = errors.New("store: invalid labels")
var ErrLabelLimitExceeded = errors.New("store: label limit exceeded")
var ErrActiveSeriesLimitExceeded = errors.New("store: active series limit exceeded")

type Store struct {
	dir               string
	engine            *engine.Engine
	opts              Options
	clock             func() time.Time
	mu                sync.RWMutex
	manifest          Manifest
	catalog           Catalog
	directoryCache    map[string]block.Directory
	allowGlobFallback bool
	stopBackground    chan struct{}
	backgroundDone    chan struct{}
	closeOnce         sync.Once
	closeErr          error
	backgroundErrMu   sync.RWMutex
	backgroundErr     error
}

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

func Open(dir string) (*Store, error) {
	return OpenWithOptions(dir, Options{})
}

func OpenConfigured(cfg Config) (*Store, error) {
	return OpenWithOptions(cfg.Dir, cfg.Options)
}

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
	catalog, err := loadCatalog(dir)
	if err != nil {
		return nil, err
	}
	e, err := engine.OpenWithRegistry(catalog.Registry(), engine.Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		return nil, err
	}
	manifest, err := loadManifest(dir)
	if err != nil {
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
	}
	st := &Store{
		dir:               dir,
		engine:            e,
		opts:              opts,
		clock:             opts.Clock,
		manifest:          manifest,
		catalog:           catalog,
		directoryCache:    make(map[string]block.Directory),
		allowGlobFallback: allowGlobFallback,
	}
	st.startBackground()
	return st, nil
}

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

func (s *Store) ensureCatalog(labelSets []model.LabelSet) error {
	if len(labelSets) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	existing := make(map[string]uint64, len(s.catalog.Series))
	for _, entry := range s.catalog.Series {
		existing[entry.Labels.Canonical().Key()] = entry.ID
	}

	newSeries := make(map[string]model.LabelSet)
	for _, labels := range labelSets {
		if err := validateLabels(labels, s.opts); err != nil {
			return err
		}
		canonical := labels.Canonical()
		key := canonical.Key()
		if _, ok := existing[key]; ok {
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
	for _, key := range keys {
		id := s.catalog.NextID
		if id == 0 {
			id = 1
		}
		s.catalog.NextID = id + 1
		labels := newSeries[key]
		s.catalog.Series = append(s.catalog.Series, CatalogEntry{ID: id, Labels: labels})
		s.engine.Registry().Import(index.SeriesID(id), labels)
	}
	return saveCatalog(s.dir, s.catalog)
}

func (s *Store) Flush() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.engine.BufferedSeries() == 0 {
		return "", ErrNoSamples
	}
	path := s.nextBlockPath()
	if err := s.engine.PrepareFlushBlock(path); err != nil {
		return "", err
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		return "", err
	}
	minTime, maxTime, _ := dir.TimeRange()
	s.manifest.Blocks = append(s.manifest.Blocks, BlockMeta{
		Path:        filepath.Base(path),
		MinTime:     minTime,
		MaxTime:     maxTime,
		SeriesCount: len(dir.Series),
		LabelValues: labelValues(dir),
	})
	if err := saveManifest(s.dir, s.manifest); err != nil {
		return "", err
	}
	if err := s.engine.CommitFlush(); err != nil {
		return "", err
	}
	s.directoryCache[path] = dir
	return path, nil
}

func (s *Store) FlushIfNeeded(maxBufferedSeries int) (string, bool, error) {
	return s.FlushIfNeededBy(maxBufferedSeries, 0)
}

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

func (s *Store) DeleteBefore(cutoffMillis int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var kept []BlockMeta
	var removePaths []string
	for _, meta := range s.manifest.Blocks {
		if meta.MaxTime >= cutoffMillis {
			kept = append(kept, meta)
			continue
		}
		removePaths = append(removePaths, filepath.Join(s.dir, meta.Path))
	}
	s.manifest.Blocks = kept
	s.directoryCache = make(map[string]block.Directory)
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

func (s *Store) Compact() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	paths, err := s.blocksForQueryLocked(index.Selector{}, query.Options{})
	if err != nil {
		return "", err
	}
	if len(paths) <= 1 {
		return "", ErrNoSamples
	}

	grouped := make(map[string]block.Series)
	for _, path := range paths {
		decoded, err := block.ReadFile(path)
		if err != nil {
			return "", err
		}
		for _, series := range decoded {
			key := series.Entry.Labels.Key()
			current := grouped[key]
			if current.ID == 0 {
				current.ID = series.Entry.SeriesID
				current.Type = series.Entry.Type
				current.Labels = series.Entry.Labels
			}
			current.Timestamps = append(current.Timestamps, series.Timestamps...)
			current.Values = append(current.Values, series.Values...)
			grouped[key] = current
		}
	}

	merged := make([]block.Series, 0, len(grouped))
	for _, series := range grouped {
		merged = append(merged, compactSeriesSamples(series))
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
	oldBlocks := append([]BlockMeta(nil), s.manifest.Blocks...)
	s.manifest.Blocks = []BlockMeta{{
		Path:        filepath.Base(path),
		MinTime:     minTime,
		MaxTime:     maxTime,
		SeriesCount: len(dir.Series),
		LabelValues: labelValues(dir),
	}}
	if err := saveManifest(s.dir, s.manifest); err != nil {
		return "", err
	}
	for _, meta := range oldBlocks {
		_ = os.Remove(filepath.Join(s.dir, meta.Path))
	}
	s.directoryCache = map[string]block.Directory{path: dir}
	return path, nil
}

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

func (s *Store) Blocks() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.blocksForQueryLocked(index.Selector{}, query.Options{})
}

func (s *Store) blocksForQueryLocked(selector index.Selector, opts query.Options) ([]string, error) {
	if err := selector.Validate(); err != nil {
		return nil, err
	}
	if len(s.manifest.Blocks) > 0 {
		paths := make([]string, 0, len(s.manifest.Blocks))
		for _, meta := range s.manifest.Blocks {
			if !metaMatchesTime(meta, opts) || !metaMatchesSelector(meta, selector) {
				continue
			}
			paths = append(paths, filepath.Join(s.dir, meta.Path))
		}
		return paths, nil
	}
	if !s.allowGlobFallback {
		return nil, nil
	}
	blockPaths, err := filepath.Glob(filepath.Join(s.dir, "block-*.meb"))
	if err != nil {
		return nil, err
	}
	compactPaths, err := filepath.Glob(filepath.Join(s.dir, "compact-*.meb"))
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0, len(blockPaths)+len(compactPaths))
	paths = append(paths, blockPaths...)
	paths = append(paths, compactPaths...)
	sort.Strings(paths)
	return paths, nil
}

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
	headSeries, err := query.SelectSeries(s.engine.Snapshot(), selector, opts)
	if err != nil {
		return nil, err
	}
	out = append(out, headSeries...)
	return out, nil
}

func (s *Store) SelectRange(rangeSelector query.RangeSelector, endMillis int64) ([]block.DecodedSeries, error) {
	return s.Select(rangeSelector.Selector, rangeSelector.Options(endMillis))
}

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
	headSeries, headSamples, err := query.SeriesStats(s.engine.Snapshot(), selector, opts)
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
	headSeries, err := query.SelectSeries(s.engine.Snapshot(), selector, opts)
	if err != nil {
		return nil, err
	}
	mergeAggregates(out, query.AggregateByLabel(headSeries, label))
	return out, nil
}

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

func (s *Store) RateByLabel(selector index.Selector, opts query.Options, label string) (map[string]float64, error) {
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return query.RateByLabelInSeries(s.engine.Snapshot(), selector, opts, label)
	}
	if len(paths) == 1 && s.engine.BufferedSeries() == 0 {
		dir, err := s.readDirectory(paths[0])
		if err != nil {
			return nil, err
		}
		return query.RateByLabelInBlockWithDirectory(paths[0], dir, selector, opts, label)
	}
	if opts.MaxSampleGapMillis != nil {
		return s.rateByLabelExact(selector, opts, label)
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
	head := s.engine.Snapshot()
	headSummaries, err := query.RateSummariesInSeries(head, selector, opts)
	if err != nil {
		return nil, err
	}
	for _, summary := range headSummaries {
		addRateSummary(seriesRates, summary, summary.Labels)
	}
	if hasOverlappingRateChunks(seriesRates) {
		return s.rateByLabelExact(selector, opts, label)
	}
	return finalizeRateByLabel(seriesRates, label), nil
}

func (s *Store) RateByLabelRange(rangeSelector query.RangeSelector, endMillis int64, label string) (map[string]float64, error) {
	return s.RateByLabel(rangeSelector.Selector, rangeSelector.Options(endMillis), label)
}

func (s *Store) RateByLabelRangeSteps(rangeSelector query.RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]query.FloatStep, error) {
	readStart := startMillis - rangeSelector.Window.Milliseconds()
	readOpts := query.TimeRange(readStart, endMillis)
	if rangeSelector.MaxSampleGap > 0 {
		readOpts = readOpts.WithMaxSampleGap(rangeSelector.MaxSampleGap)
	}
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(rangeSelector.Selector, readOpts)
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
	if rangeSelector.MaxSampleGap <= 0 && preferRangeStepSummaries(len(steps), len(paths), s.engine.BufferedSeries()) {
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

func (s *Store) IncreaseByLabelRangeSteps(rangeSelector query.RangeSelector, startMillis int64, endMillis int64, step time.Duration, label string) ([]query.IntStep, error) {
	readStart := startMillis - rangeSelector.Window.Milliseconds()
	readOpts := query.TimeRange(readStart, endMillis)
	if rangeSelector.MaxSampleGap > 0 {
		readOpts = readOpts.WithMaxSampleGap(rangeSelector.MaxSampleGap)
	}
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(rangeSelector.Selector, readOpts)
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
	if rangeSelector.MaxSampleGap <= 0 && preferRangeStepSummaries(len(steps), len(paths), s.engine.BufferedSeries()) {
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

func (s *Store) IncreaseByLabel(selector index.Selector, opts query.Options, label string) (map[string]int64, error) {
	s.mu.RLock()
	paths, err := s.blocksForQueryLocked(selector, opts)
	s.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return query.IncreaseByLabelInSeries(s.engine.Snapshot(), selector, opts, label)
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
	headSummaries, err := query.RateSummariesInSeries(s.engine.Snapshot(), selector, opts)
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

func (s *Store) IncreaseByLabelRange(rangeSelector query.RangeSelector, endMillis int64, label string) (map[string]int64, error) {
	return s.IncreaseByLabel(rangeSelector.Selector, rangeSelector.Options(endMillis), label)
}

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
		dir, err := s.readDirectory(path)
		if err != nil {
			return Stats{}, err
		}
		for _, entry := range dir.Series {
			stats.Samples += entry.ValueN
		}
	}
	return stats, nil
}

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
	dir, ok := s.directoryCache[path]
	s.mu.RUnlock()
	if ok {
		return dir, nil
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		return block.Directory{}, err
	}
	s.mu.Lock()
	s.directoryCache[path] = dir
	s.mu.Unlock()
	return dir, nil
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

func finalizeRateByLabel(seriesRates map[uint64]seriesRate, groupLabel string) map[string]float64 {
	out := make(map[string]float64)
	for _, series := range seriesRates {
		if !series.hasRate {
			continue
		}
		rate, ok, err := query.RateFromSummary(series.summary)
		if err != nil || !ok {
			continue
		}
		key, ok := series.labels.Get(groupLabel)
		if !ok {
			key = ""
		}
		out[key] += rate
	}
	return out
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

func (s *Store) collectRangeStepSeries(paths []string, selector index.Selector, opts query.Options) (map[uint64]*exactRateSeries, error) {
	type blockScanTarget struct {
		path string
		dir  block.Directory
	}
	targets := make([]blockScanTarget, 0, len(paths))
	mapHint := 0
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return nil, err
		}
		targets = append(targets, blockScanTarget{path: path, dir: dir})
		mapHint += len(dir.Series)
	}
	head := s.engine.Snapshot()
	mapHint += len(head)
	capacityHints := make(map[uint64]int, mapHint)
	for _, target := range targets {
		for _, entry := range target.dir.Series {
			capacityHints[entry.SeriesID] += entry.ValueN
		}
	}
	for _, series := range head {
		capacityHints[series.ID] += len(series.Values)
	}

	collector := newExactSeriesCollector(len(capacityHints), capacityHints)
	for _, target := range targets {
		if err := query.ScanBlockWithDirectoryShared(target.path, target.dir, selector, opts, func(series block.DecodedSeries) error {
			return collector.addDecodedSamples(series, opts)
		}); err != nil {
			return nil, err
		}
	}
	if err := query.ScanSeries(head, selector, opts, func(series block.DecodedSeries) error {
		return collector.addDecodedSamples(series, opts)
	}); err != nil {
		return nil, err
	}
	return collector.grouped, nil
}

type exactSeriesCollector struct {
	grouped       map[uint64]*exactRateSeries
	arena         []exactRateSeries
	capacityHints map[uint64]int
}

func newExactSeriesCollector(mapHint int, capacityHints map[uint64]int) *exactSeriesCollector {
	return &exactSeriesCollector{
		grouped:       make(map[uint64]*exactRateSeries, mapHint),
		arena:         make([]exactRateSeries, 0, mapHint),
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
				c.arena = append(c.arena, exactRateSeries{
					labels:  chunk.Entry.Labels,
					samples: make([]rateSample, 0, capacity),
				})
				current = &c.arena[len(c.arena)-1]
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
	for _, path := range paths {
		dir, err := s.readDirectory(path)
		if err != nil {
			return nil, err
		}
		if err := query.ScanBlockWithDirectoryShared(path, dir, selector, opts, func(series block.DecodedSeries) error {
			return addRangeStepSummaries(grouped, series, steps, window, opts)
		}); err != nil {
			return nil, err
		}
	}
	if err := query.ScanSeries(s.engine.Snapshot(), selector, opts, func(series block.DecodedSeries) error {
		return addRangeStepSummaries(grouped, series, steps, window, opts)
	}); err != nil {
		return nil, err
	}
	return grouped, nil
}

func addRangeStepSummaries(grouped map[uint64]*rangeStepRateSeries, series block.DecodedSeries, steps []int64, window time.Duration, opts query.Options) error {
	summaries, err := query.RateWindowSummariesForStepsWithOptions(series.Entry.SeriesID, series.Timestamps, series.Values, steps, window, opts)
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
	return stepCount <= 2 && blockCount > 1 && headSeries == 0
}

func addExactRateSampleSteps(out []query.FloatStep, steps []int64, series *exactRateSeries, groupLabel string, window time.Duration, opts query.Options) error {
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return errors.New("store: window must be at least 1ms")
	}
	samples := compactExactRateSamples(series)
	if len(samples) < 2 {
		return nil
	}
	key, ok := series.labels.Get(groupLabel)
	if !ok {
		key = ""
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

func addExactIncreaseSampleSteps(out []query.IntStep, steps []int64, series *exactRateSeries, groupLabel string, window time.Duration, opts query.Options) error {
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return errors.New("store: window must be at least 1ms")
	}
	samples := compactExactRateSamples(series)
	if len(samples) < 2 {
		return nil
	}
	key, ok := series.labels.Get(groupLabel)
	if !ok {
		key = ""
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

func addExactAggregateSampleSteps(out []query.AggregateStep, steps []int64, series *exactRateSeries, groupLabel string, window time.Duration, workspace *aggregateStepWorkspace) error {
	windowMillis := window.Milliseconds()
	if windowMillis <= 0 {
		return errors.New("store: window must be at least 1ms")
	}
	samples := compactExactRateSamples(series)
	if len(samples) == 0 {
		return nil
	}
	key, ok := series.labels.Get(groupLabel)
	if !ok {
		key = ""
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

func compactExactRateSamples(series *exactRateSeries) []rateSample {
	if len(series.samples) == 0 {
		return nil
	}
	sort.SliceStable(series.samples, func(i, j int) bool {
		return series.samples[i].timestamp < series.samples[j].timestamp
	})
	write := 0
	for _, sample := range series.samples {
		if write > 0 && series.samples[write-1].timestamp == sample.timestamp {
			series.samples[write-1].value = sample.value
			continue
		}
		series.samples[write] = sample
		write++
	}
	return series.samples[:write]
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
	grouped, err := s.collectRangeStepSeries(paths, selector, opts)
	if err != nil {
		return nil, err
	}
	out := makeFloatSteps(steps)
	for _, series := range grouped {
		if err := addExactRateSampleSteps(out, steps, series, label, window, opts); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (s *Store) increaseByLabelRangeStepsExact(paths []string, selector index.Selector, opts query.Options, steps []int64, window time.Duration, label string) ([]query.IntStep, error) {
	grouped, err := s.collectRangeStepSeries(paths, selector, opts)
	if err != nil {
		return nil, err
	}
	out := makeIntSteps(steps)
	for _, series := range grouped {
		if err := addExactIncreaseSampleSteps(out, steps, series, label, window, opts); err != nil {
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
	grouped, err := s.collectRangeStepSeries(paths, selector, opts)
	if err != nil {
		return nil, err
	}
	out := makeAggregateSteps(steps)
	workspace := &aggregateStepWorkspace{}
	for _, series := range grouped {
		if err := addExactAggregateSampleSteps(out, steps, series, label, window, workspace); err != nil {
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

func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		if s.stopBackground != nil {
			close(s.stopBackground)
			<-s.backgroundDone
		}
		if s.engine.BufferedSeries() > 0 {
			if _, err := s.Flush(); err != nil && !errors.Is(err, ErrNoSamples) {
				s.closeErr = err
			}
		}
		if err := s.engine.Close(); err != nil && s.closeErr == nil {
			s.closeErr = err
		}
	})
	return s.closeErr
}

func (s *Store) nextBlockPath() string {
	return s.nextBlockPathWithPrefix("block")
}

func (s *Store) nextBlockPathWithPrefix(prefix string) string {
	base := time.Now().UTC().Format("20060102T150405.000000000Z")
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
