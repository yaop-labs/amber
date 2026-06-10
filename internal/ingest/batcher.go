// Package ingest accepts entries, batches them, and writes through to storage,
// applying cardinality and circuit-breaker policies on the hot path.
package ingest

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/internal/storage"
)

type item struct {
	log  *model.LogEntry
	span *model.SpanEntry
}

var (
	ErrQueueFull   = errors.New("ingest queue full")
	ErrBreakerOpen = errors.New("ingest circuit breaker open")
	ErrCardinality = errors.New("ingest cardinality limit exceeded")
)

type Batcher struct {
	logManager       *storage.SegmentManager
	spanManager      *storage.SegmentManager
	logSparse        *index.SparseIndex
	spanSparse       *index.SparseIndex
	indexer          ActiveIndexer
	logBatchSize     int
	spanBatchSize    int
	logBatchTimeout  time.Duration
	spanBatchTimeout time.Duration
	logQueue         chan item
	spanQueue        chan item
	log              *slog.Logger
	wg               sync.WaitGroup
	logBreaker       uint64
	spanBreaker      uint64
	logFailures      atomic.Uint64
	spanFailures     atomic.Uint64
	guard            *CardinalityGuard
	cacheInvalidator CacheInvalidator

	logM, spanM kindMetrics
}

// kindMetrics holds precomputed metric handles for one entry kind ("log" or
// "span"). Resolving the CounterVec child via WithLabelValues each call adds
// a strings.Join allocation per metric event (~50ns + 16B garbage). For
// success paths this fires on every accepted entry. Bind once at NewBatcher
// time and store pointers.
type kindMetrics struct {
	accepted     *selfobs.Counter
	breakerOpen  *selfobs.Counter
	queueFull    *selfobs.Counter
	serializeErr *selfobs.Counter
	writeFailed  *selfobs.Counter
	cardAttrs    *selfobs.Counter
	cardValueLen *selfobs.Counter
	cardKeys     *selfobs.Counter
}

func newKindMetrics(kind string) kindMetrics {
	return kindMetrics{
		accepted:     selfobs.IngestAccepted.WithLabelValues(kind),
		breakerOpen:  selfobs.IngestDropped.WithLabelValues(kind, "breaker_open"),
		queueFull:    selfobs.IngestDropped.WithLabelValues(kind, "queue_full"),
		serializeErr: selfobs.IngestDropped.WithLabelValues(kind, "serialize_error"),
		writeFailed:  selfobs.IngestDropped.WithLabelValues(kind, "write_failed"),
		cardAttrs:    selfobs.IngestDropped.WithLabelValues(kind, "attrs_per_entry"),
		cardValueLen: selfobs.IngestDropped.WithLabelValues(kind, "attr_value_too_long"),
		cardKeys:     selfobs.IngestDropped.WithLabelValues(kind, "key_cardinality"),
	}
}

// dropCardinality dispatches a guard.Check return string to the right
// counter. Mirrors the reason strings defined in cardinality.go — keep in
// sync. Returns nil for unknown reasons (defensive; caller checks).
func (m *kindMetrics) dropCardinality(reason string) *selfobs.Counter {
	switch reason {
	case "attrs_per_entry":
		return m.cardAttrs
	case "attr_value_too_long":
		return m.cardValueLen
	case "key_cardinality":
		return m.cardKeys
	}
	return nil
}

type Deps struct {
	LogManager  *storage.SegmentManager
	SpanManager *storage.SegmentManager
	LogSparse   *index.SparseIndex
	SpanSparse  *index.SparseIndex
	Indexer     ActiveIndexer
	Guard       *CardinalityGuard
	Invalidator CacheInvalidator
	Logger      *slog.Logger
}

type Config struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
	Logs             LaneConfig
	Spans            LaneConfig
}

type LaneConfig struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
}

type laneConfig struct {
	batchSize        int
	batchTimeout     time.Duration
	queueSize        int
	breakerThreshold uint64
}

func NewBatcher(deps Deps, cfg Config) *Batcher {
	logCfg := resolveLaneConfig(cfg, cfg.Logs)
	spanCfg := resolveLaneConfig(cfg, cfg.Spans)
	return &Batcher{
		logManager:       deps.LogManager,
		spanManager:      deps.SpanManager,
		logSparse:        deps.LogSparse,
		spanSparse:       deps.SpanSparse,
		indexer:          deps.Indexer,
		logBatchSize:     logCfg.batchSize,
		spanBatchSize:    spanCfg.batchSize,
		logBatchTimeout:  logCfg.batchTimeout,
		spanBatchTimeout: spanCfg.batchTimeout,
		logQueue:         make(chan item, logCfg.queueSize),
		spanQueue:        make(chan item, spanCfg.queueSize),
		log:              deps.Logger,
		logBreaker:       logCfg.breakerThreshold,
		spanBreaker:      spanCfg.breakerThreshold,
		guard:            deps.Guard,
		cacheInvalidator: deps.Invalidator,
		logM:             newKindMetrics("log"),
		spanM:            newKindMetrics("span"),
	}
}

func resolveLaneConfig(base Config, lane LaneConfig) laneConfig {
	batchSize := base.BatchSize
	if lane.BatchSize > 0 {
		batchSize = lane.BatchSize
	}
	batchTimeout := base.BatchTimeout
	if lane.BatchTimeout > 0 {
		batchTimeout = lane.BatchTimeout
	}
	queueSize := base.QueueSize
	if lane.QueueSize > 0 {
		queueSize = lane.QueueSize
	}
	breakerThreshold := base.BreakerThreshold
	if lane.BreakerThreshold > 0 {
		breakerThreshold = lane.BreakerThreshold
	}
	var threshold uint64
	if breakerThreshold > 0 {
		threshold = uint64(breakerThreshold)
	}
	return laneConfig{
		batchSize:        batchSize,
		batchTimeout:     batchTimeout,
		queueSize:        queueSize,
		breakerThreshold: threshold,
	}
}

func (b *Batcher) IsBreakerOpen() bool {
	return b.IsLogBreakerOpen() || b.IsSpanBreakerOpen()
}

func (b *Batcher) IsLogBreakerOpen() bool {
	return b.logBreaker > 0 && b.logFailures.Load() >= b.logBreaker
}

func (b *Batcher) IsSpanBreakerOpen() bool {
	return b.spanBreaker > 0 && b.spanFailures.Load() >= b.spanBreaker
}

func (b *Batcher) Start(ctx context.Context) {
	b.wg.Add(2)
	go b.run(ctx, b.logQueue, b.logBatchSize, b.logBatchTimeout, b.processLogBatch)
	go b.run(ctx, b.spanQueue, b.spanBatchSize, b.spanBatchTimeout, b.processSpanBatch)
}

func (b *Batcher) Wait() {
	b.wg.Wait()
}

func (b *Batcher) SendLog(entry model.LogEntry) error {
	if b.IsLogBreakerOpen() {
		b.logM.breakerOpen.Inc()
		return ErrBreakerOpen
	}
	if reason := b.guard.Check(entry.Service, entry.Attrs); reason != "" {
		if c := b.logM.dropCardinality(reason); c != nil {
			c.Inc()
		}
		return ErrCardinality
	}
	select {
	case b.logQueue <- item{log: &entry}:
		return nil
	default:
		b.logM.queueFull.Inc()
		b.log.Warn("ingest queue full, dropping log entry",
			"entry_id", entry.ID.String(),
			"service", entry.Service,
		)
		return ErrQueueFull
	}
}

func (b *Batcher) SendSpan(span model.SpanEntry) error {
	if b.IsSpanBreakerOpen() {
		b.spanM.breakerOpen.Inc()
		return ErrBreakerOpen
	}
	if reason := b.guard.Check(span.Service, span.Attrs); reason != "" {
		if c := b.spanM.dropCardinality(reason); c != nil {
			c.Inc()
		}
		return ErrCardinality
	}
	select {
	case b.spanQueue <- item{span: &span}:
		return nil
	default:
		b.spanM.queueFull.Inc()
		b.log.Warn("ingest queue full, dropping span",
			"entry_id", span.ID.String(),
			"service", span.Service,
			"operation", span.Operation,
		)
		return ErrQueueFull
	}
}

func (b *Batcher) QueueLen() int { return b.LogQueueLen() + b.SpanQueueLen() }

func (b *Batcher) LogQueueLen() int { return len(b.logQueue) }

func (b *Batcher) SpanQueueLen() int { return len(b.spanQueue) }

func (b *Batcher) TrySendLog(entry model.LogEntry) bool {
	return b.SendLog(entry) == nil
}

var bufPool = sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

func (b *Batcher) run(ctx context.Context, queue <-chan item, batchSize int, batchTimeout time.Duration, process func(context.Context, []item)) {
	defer b.wg.Done()

	batch := make([]item, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		process(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case it := <-queue:
					batch = append(batch, it)
					if len(batch) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case it := <-queue:
			batch = append(batch, it)
			if len(batch) >= batchSize {
				flush()
				ticker.Reset(batchTimeout)
			}

		case <-ticker.C:
			flush()
		}
	}
}

func (b *Batcher) processBatch(_ context.Context, batch []item) {
	if len(batch) == 0 {
		return
	}

	logItems, logEntries := b.prepareLogBatch(batch)
	if len(logItems) > 0 {
		b.writeLogBatch(logItems, logEntries)
	}

	spanItems, spanEntries := b.prepareSpanBatch(batch)
	if len(spanItems) > 0 {
		b.writeSpanBatch(spanItems, spanEntries)
	}
}

func (b *Batcher) processLogBatch(_ context.Context, batch []item) {
	if len(batch) == 0 {
		return
	}
	logItems, logEntries := b.prepareLogBatch(batch)
	if len(logItems) == 0 {
		return
	}
	b.writeLogBatch(logItems, logEntries)
}

func (b *Batcher) processSpanBatch(_ context.Context, batch []item) {
	if len(batch) == 0 {
		return
	}
	spanItems, spanEntries := b.prepareSpanBatch(batch)
	if len(spanItems) == 0 {
		return
	}
	b.writeSpanBatch(spanItems, spanEntries)
}

func (b *Batcher) prepareLogBatch(batch []item) ([]storage.BatchItem, []*model.LogEntry) {
	logItems := make([]storage.BatchItem, 0, len(batch))
	var logEntries []*model.LogEntry
	for _, it := range batch {
		if it.log == nil {
			continue
		}
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		_, writeErr := it.log.WriteTo(buf)
		if writeErr != nil {
			b.logM.serializeErr.Inc()
			b.log.Error("serialize log entry", "err", writeErr)
			bufPool.Put(buf)
			continue
		}
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		bufPool.Put(buf)

		logItems = append(logItems, storage.BatchItem{Data: data, TS: it.log.Timestamp.UnixNano()})
		if b.indexer != nil {
			if logEntries == nil {
				logEntries = make([]*model.LogEntry, 0, len(batch))
			}
			logEntries = append(logEntries, it.log)
		}
	}
	return logItems, logEntries
}

func (b *Batcher) prepareSpanBatch(batch []item) ([]storage.BatchItem, []*model.SpanEntry) {
	spanItems := make([]storage.BatchItem, 0, len(batch))
	var spanEntries []*model.SpanEntry
	for _, it := range batch {
		if it.span == nil {
			continue
		}
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()
		_, writeErr := it.span.WriteTo(buf)
		if writeErr != nil {
			b.spanM.serializeErr.Inc()
			b.log.Error("serialize span entry", "err", writeErr)
			bufPool.Put(buf)
			continue
		}
		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		bufPool.Put(buf)

		spanItems = append(spanItems, storage.BatchItem{Data: data, TS: it.span.StartTime.UnixNano()})
		if b.indexer != nil {
			if spanEntries == nil {
				spanEntries = make([]*model.SpanEntry, 0, len(batch))
			}
			spanEntries = append(spanEntries, it.span)
		}
	}
	return spanItems, spanEntries
}

func (b *Batcher) writeLogBatch(logItems []storage.BatchItem, logEntries []*model.LogEntry) bool {
	targetMeta, hasTarget := b.logManager.ActiveSegmentMeta()
	if err := b.logManager.WriteBatch(logItems); err != nil {
		b.logM.writeFailed.Add(uint64(len(logItems)))
		b.log.Error("log batch write failed", "err", err, "count", len(logItems))
		b.recordLogFailure()
		return true
	}
	if err := b.logManager.Flush(); err != nil {
		b.logM.writeFailed.Add(uint64(len(logItems)))
		b.log.Error("log segment flush failed", "err", err, "count", len(logItems))
		b.recordLogFailure()
		return true
	}
	b.logM.accepted.Add(uint64(len(logItems)))
	if hasTarget {
		updateSparseFromBatchMeta(b.logSparse, targetMeta, logItems)
	}
	if b.indexer != nil && len(logEntries) > 0 && b.segmentStillActive(b.logManager, targetMeta, hasTarget) {
		b.indexer.IndexLogEntries(logEntries)
	}
	if b.cacheInvalidator != nil {
		b.cacheInvalidator.ClearResultCache()
	}
	b.resetLogFailures()
	return false
}

func (b *Batcher) writeSpanBatch(spanItems []storage.BatchItem, spanEntries []*model.SpanEntry) bool {
	targetMeta, hasTarget := b.spanManager.ActiveSegmentMeta()
	if err := b.spanManager.WriteBatch(spanItems); err != nil {
		b.spanM.writeFailed.Add(uint64(len(spanItems)))
		b.log.Error("span batch write failed", "err", err, "count", len(spanItems))
		b.recordSpanFailure()
		return true
	}
	if err := b.spanManager.Flush(); err != nil {
		b.spanM.writeFailed.Add(uint64(len(spanItems)))
		b.log.Error("span segment flush failed", "err", err, "count", len(spanItems))
		b.recordSpanFailure()
		return true
	}
	b.spanM.accepted.Add(uint64(len(spanItems)))
	if hasTarget {
		updateSparseFromBatchMeta(b.spanSparse, targetMeta, spanItems)
	}
	if b.indexer != nil && len(spanEntries) > 0 && b.segmentStillActive(b.spanManager, targetMeta, hasTarget) {
		b.indexer.IndexSpanEntries(spanEntries)
	}
	if b.cacheInvalidator != nil {
		b.cacheInvalidator.ClearResultCache()
	}
	b.resetSpanFailures()
	return false
}

func (b *Batcher) recordLogFailure() {
	n := b.logFailures.Add(1)
	if b.logBreaker > 0 && n == b.logBreaker {
		b.log.Error("log ingest breaker tripped", "consecutive_failures", n)
	}
}

func (b *Batcher) recordSpanFailure() {
	n := b.spanFailures.Add(1)
	if b.spanBreaker > 0 && n == b.spanBreaker {
		b.log.Error("span ingest breaker tripped", "consecutive_failures", n)
	}
}

func (b *Batcher) resetLogFailures() {
	if b.logFailures.Swap(0) >= b.logBreaker && b.logBreaker > 0 {
		b.log.Info("log ingest breaker reset")
	}
}

func (b *Batcher) resetSpanFailures() {
	if b.spanFailures.Swap(0) >= b.spanBreaker && b.spanBreaker > 0 {
		b.log.Info("span ingest breaker reset")
	}
}

func (b *Batcher) segmentStillActive(manager *storage.SegmentManager, meta storage.SegmentMeta, ok bool) bool {
	if !ok {
		return false
	}
	active, activeOK := manager.ActiveSegmentMeta()
	return activeOK && active.ID == meta.ID && active.FileName == meta.FileName
}

func updateSparseFromBatchMeta(sparse *index.SparseIndex, meta storage.SegmentMeta, items []storage.BatchItem) {
	if sparse == nil || len(items) == 0 {
		return
	}
	minTS, maxTS := items[0].TS, items[0].TS
	for _, it := range items[1:] {
		if it.TS < minTS {
			minTS = it.TS
		}
		if it.TS > maxTS {
			maxTS = it.TS
		}
	}
	sparse.TouchRange(meta.ID, meta.FileName, minTS, maxTS)
}
