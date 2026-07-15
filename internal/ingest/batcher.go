// Package ingest accepts entries, batches them, and writes through to storage,
// applying cardinality and circuit-breaker policies on the hot path.
package ingest

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	log          *model.LogEntry
	span         *model.SpanEntry
	nativeReplay bool
	// barrier marks a Flush sentinel: the worker flushes the pending batch,
	// then reports the lane's sticky outcome. Never carries data.
	barrier chan error
}

// Errors returned by the Send methods when an entry is rejected on the hot path.
var (
	ErrQueueFull      = errors.New("ingest queue full")
	ErrBreakerOpen    = errors.New("ingest circuit breaker open")
	ErrCardinality    = errors.New("ingest cardinality limit exceeded")
	ErrClosed         = errors.New("ingest is closing or closed")
	ErrRecordTooLarge = storage.ErrRecordTooLarge
)

// Batcher is the asynchronous ingest path: SendLog/SendSpan enqueue entries
// that per-kind background workers batch and write through to storage,
// applying the cardinality guard and a per-lane circuit breaker. It is the
// production ingest entry point. Safe for concurrent use.
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
	replaySink       ReplaySink

	// admitMu makes the open->closing transition atomic with respect to queue
	// admission. Once beginClose returns, every Send that held the read lock has
	// either enqueued its item or returned an error, so the following barriers
	// cover all accepted work.
	admitMu   sync.RWMutex
	accepting bool
	cancel    context.CancelFunc
	closeOnce sync.Once
	closeDone chan struct{}
	closeErr  error

	logM, spanM kindMetrics
}

// kindMetrics holds precomputed metric handles for one entry kind ("log" or
// "span"). Resolving the CounterVec child via WithLabelValues each call adds
// a strings.Join allocation per metric event (~50ns + 16B garbage). For
// success paths this fires on every accepted entry. Bind once at NewBatcher
// time and store pointers.
type kindMetrics struct {
	accepted       *selfobs.Counter
	durable        *selfobs.Counter
	breakerOpen    *selfobs.Counter
	queueFull      *selfobs.Counter
	serializeErr   *selfobs.Counter
	writeFailed    *selfobs.Counter
	cardAttrs      *selfobs.Counter
	cardValueLen   *selfobs.Counter
	cardKeys       *selfobs.Counter
	cardServices   *selfobs.Counter
	recordTooLarge *selfobs.Counter
}

func newKindMetrics(kind string) kindMetrics {
	return kindMetrics{
		accepted:       selfobs.IngestAccepted.WithLabelValues(kind),
		durable:        selfobs.IngestDurable.WithLabelValues(kind),
		breakerOpen:    selfobs.IngestDropped.WithLabelValues(kind, "breaker_open"),
		queueFull:      selfobs.IngestDropped.WithLabelValues(kind, "queue_full"),
		serializeErr:   selfobs.IngestDropped.WithLabelValues(kind, "serialize_error"),
		writeFailed:    selfobs.IngestDropped.WithLabelValues(kind, "write_failed"),
		cardAttrs:      selfobs.IngestDropped.WithLabelValues(kind, "attrs_per_entry"),
		cardValueLen:   selfobs.IngestDropped.WithLabelValues(kind, "attr_value_too_long"),
		cardKeys:       selfobs.IngestDropped.WithLabelValues(kind, "key_cardinality"),
		cardServices:   selfobs.IngestDropped.WithLabelValues(kind, "service_cardinality"),
		recordTooLarge: selfobs.IngestDropped.WithLabelValues(kind, "record_too_large"),
	}
}

// dropCardinality dispatches a guard.Check return string to the right
// counter. Mirrors the reason strings defined in cardinality.go - keep in
// sync. Returns nil for unknown reasons (defensive; caller checks).
func (m *kindMetrics) dropCardinality(reason string) *selfobs.Counter {
	switch reason {
	case "attrs_per_entry":
		return m.cardAttrs
	case "attr_value_too_long":
		return m.cardValueLen
	case "key_cardinality":
		return m.cardKeys
	case "service_cardinality":
		return m.cardServices
	}
	return nil
}

// Deps are the collaborators a Batcher writes through to: the segment managers,
// sparse indexes, active indexer, cardinality guard, cache invalidator, and logger.
type Deps struct {
	LogManager  *storage.SegmentManager
	SpanManager *storage.SegmentManager
	LogSparse   *index.SparseIndex
	SpanSparse  *index.SparseIndex
	Indexer     ActiveIndexer
	Guard       *CardinalityGuard
	Invalidator CacheInvalidator
	Logger      *slog.Logger
	ReplaySink  ReplaySink
}

// ReplaySink records native model entries after their query projection is durable.
type ReplaySink interface {
	AppendNormalizedLogs([]model.LogEntry) error
	AppendNormalizedSpans([]model.SpanEntry) error
}

// Config tunes the batcher. The top-level fields are defaults; Logs and Spans
// override them per lane (a zero lane field falls back to the top-level value).
type Config struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
	Logs             LaneConfig
	Spans            LaneConfig
}

// LaneConfig overrides batching and breaker settings for one lane (logs or spans).
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

// NewBatcher builds a batcher from its dependencies and config. Call Start to
// launch the workers.
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
		replaySink:       deps.ReplaySink,
		accepting:        true,
		closeDone:        make(chan struct{}),
		logM:             newKindMetrics("log"),
		spanM:            newKindMetrics("span"),
	}
}

// SetReplaySink attaches the native replay sink before Start.
func (b *Batcher) SetReplaySink(sink ReplaySink) { b.replaySink = sink }

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

// IsBreakerOpen reports whether either lane's circuit breaker is open.
func (b *Batcher) IsBreakerOpen() bool {
	return b.IsLogBreakerOpen() || b.IsSpanBreakerOpen()
}

// IsLogBreakerOpen reports whether the log lane's breaker is open.
func (b *Batcher) IsLogBreakerOpen() bool {
	return b.logBreaker > 0 && b.logFailures.Load() >= b.logBreaker
}

// IsSpanBreakerOpen reports whether the span lane's breaker is open.
func (b *Batcher) IsSpanBreakerOpen() bool {
	return b.spanBreaker > 0 && b.spanFailures.Load() >= b.spanBreaker
}

// Start launches the per-lane worker goroutines. Parent cancellation is
// translated into the same graceful Close transition (stop admission,
// barriers, worker cancellation) rather than cancelling workers directly.
func (b *Batcher) Start(ctx context.Context) {
	workerCtx, cancel := context.WithCancel(context.Background())
	b.cancel = cancel
	b.wg.Add(2)
	go b.run(workerCtx, b.logQueue, b.logBatchSize, b.logBatchTimeout, b.processLogBatch)
	go b.run(workerCtx, b.spanQueue, b.spanBatchSize, b.spanBatchTimeout, b.processSpanBatch)
	if ctx.Done() != nil {
		shutdownCtx := context.WithoutCancel(ctx)
		go func() {
			select {
			case <-ctx.Done():
				_ = b.Close(shutdownCtx)
			case <-b.closeDone:
			}
		}()
	}
}

// Wait blocks until the worker goroutines have drained and exited.
func (b *Batcher) Wait() {
	b.wg.Wait()
}

// SendLog enqueues a log entry. It returns ErrBreakerOpen, ErrQueueFull, or
// ErrCardinality when the entry is rejected; a nil return means queued, not yet
// durable (the worker writes and fsyncs it).
func (b *Batcher) SendLog(entry model.LogEntry) error {
	return b.sendLog(entry, true)
}

// SendOTLPLog enqueues a log whose original OTLP representation is recorded
// by the transport after its durability barrier.
func (b *Batcher) SendOTLPLog(entry model.LogEntry) error {
	return b.sendLog(entry, false)
}

func (b *Batcher) sendLog(entry model.LogEntry, nativeReplay bool) error {
	b.admitMu.RLock()
	defer b.admitMu.RUnlock()
	if !b.accepting {
		return ErrClosed
	}
	if b.IsLogBreakerOpen() {
		b.logM.breakerOpen.Inc()
		return ErrBreakerOpen
	}
	encodedSize, err := entry.EncodedSize()
	if err != nil {
		return fmt.Errorf("ingest: invalid log record: %w", err)
	}
	if err := validateEventRecordSize("log", encodedSize); err != nil {
		b.logM.recordTooLarge.Inc()
		return err
	}
	reason, admitted := b.guard.Admit(entry.Service, entry.Attrs, func() bool {
		select {
		case b.logQueue <- item{log: &entry, nativeReplay: nativeReplay}:
			return true
		default:
			return false
		}
	})
	if reason != "" {
		if c := b.logM.dropCardinality(reason); c != nil {
			c.Inc()
		}
		return ErrCardinality
	}
	if admitted {
		b.logM.accepted.Inc()
		return nil
	}
	b.logM.queueFull.Inc()
	b.log.Warn("ingest queue full, dropping log entry",
		"entry_id", entry.ID.String(),
		"service", entry.Service,
	)
	return ErrQueueFull
}

// SendSpan enqueues a span, mirroring SendLog's reject errors and async semantics.
func (b *Batcher) SendSpan(span model.SpanEntry) error {
	return b.sendSpan(span, true)
}

// SendOTLPSpan is SendOTLPLog for a trace span.
func (b *Batcher) SendOTLPSpan(span model.SpanEntry) error {
	return b.sendSpan(span, false)
}

func (b *Batcher) sendSpan(span model.SpanEntry, nativeReplay bool) error {
	b.admitMu.RLock()
	defer b.admitMu.RUnlock()
	if !b.accepting {
		return ErrClosed
	}
	if b.IsSpanBreakerOpen() {
		b.spanM.breakerOpen.Inc()
		return ErrBreakerOpen
	}
	encodedSize, err := span.EncodedSize()
	if err != nil {
		return fmt.Errorf("ingest: invalid span record: %w", err)
	}
	if err := validateEventRecordSize("span", encodedSize); err != nil {
		b.spanM.recordTooLarge.Inc()
		return err
	}
	reason, admitted := b.guard.Admit(span.Service, span.Attrs, func() bool {
		select {
		case b.spanQueue <- item{span: &span, nativeReplay: nativeReplay}:
			return true
		default:
			return false
		}
	})
	if reason != "" {
		if c := b.spanM.dropCardinality(reason); c != nil {
			c.Inc()
		}
		return ErrCardinality
	}
	if admitted {
		b.spanM.accepted.Inc()
		return nil
	}
	b.spanM.queueFull.Inc()
	b.log.Warn("ingest queue full, dropping span",
		"entry_id", span.ID.String(),
		"service", span.Service,
		"operation", span.Operation,
	)
	return ErrQueueFull
}

func validateEventRecordSize(kind string, encodedSize uint64) error {
	if encodedSize > uint64(storage.MaxEventRecordBytes) {
		return fmt.Errorf("%w: %s encodes to %d bytes, max %d", ErrRecordTooLarge, kind, encodedSize, storage.MaxEventRecordBytes)
	}
	return nil
}

// QueueLen returns the combined depth of the log and span queues.
func (b *Batcher) QueueLen() int { return b.LogQueueLen() + b.SpanQueueLen() }

// LogQueueLen returns the current depth of the log queue.
func (b *Batcher) LogQueueLen() int { return len(b.logQueue) }

// SpanQueueLen returns the current depth of the span queue.
func (b *Batcher) SpanQueueLen() int { return len(b.spanQueue) }

// TrySendLog enqueues a log entry without blocking, returning false if the
// queue is full. Used by self-observation to avoid feedback when ingesting its
// own metrics.
func (b *Batcher) TrySendLog(entry model.LogEntry) bool {
	return b.SendLog(entry) == nil
}

var bufPool = sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

func (b *Batcher) run(ctx context.Context, queue <-chan item, batchSize int, batchTimeout time.Duration, process func(context.Context, []item) error) {
	defer b.wg.Done()

	batch := make([]item, 0, batchSize)
	var stickyErr error
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := process(ctx, batch); err != nil {
			stickyErr = errors.Join(stickyErr, err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case it := <-queue:
					if it.barrier != nil {
						flush()
						it.barrier <- stickyErr
						continue
					}
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
			if it.barrier != nil {
				flush()
				it.barrier <- stickyErr
				continue
			}
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

// Flush blocks until both lanes have processed everything enqueued before the
// call. It returns any serialization or storage error observed by either lane
// before its barrier; nil means all covered entries reached their storage
// manager successfully.
func (b *Batcher) Flush(ctx context.Context) error {
	b.admitMu.RLock()
	defer b.admitMu.RUnlock()
	if !b.accepting {
		return ErrClosed
	}
	return b.flush(ctx)
}

// FlushLogs and FlushSpans are per-signal durability barriers used by OTLP
// Export: success means every record admitted by that request's lane is durable
// and queryable.
func (b *Batcher) FlushLogs(ctx context.Context) error  { return b.flushOne(ctx, b.logQueue) }
func (b *Batcher) FlushSpans(ctx context.Context) error { return b.flushOne(ctx, b.spanQueue) }

func (b *Batcher) flushOne(ctx context.Context, queue chan item) error {
	b.admitMu.RLock()
	defer b.admitMu.RUnlock()
	if !b.accepting {
		return ErrClosed
	}
	done := make(chan error, 1)
	select {
	case queue <- item{barrier: done}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) flush(ctx context.Context) error {
	lanes := [2]chan item{b.logQueue, b.spanQueue}
	var dones [2]chan error
	for i, queue := range lanes {
		done := make(chan error, 1)
		select {
		case queue <- item{barrier: done}:
			dones[i] = done
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var errs []error
	for _, done := range dones {
		select {
		case err := <-done:
			if err != nil {
				errs = append(errs, err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return errors.Join(errs...)
}

// Close atomically stops admission, waits for both lane barriers, then stops
// the workers. The shutdown itself continues if a caller's context expires;
// subsequent Close calls observe the same terminal result.
func (b *Batcher) Close(ctx context.Context) error {
	b.closeOnce.Do(func() {
		go func() {
			b.admitMu.Lock()
			b.accepting = false
			b.admitMu.Unlock()

			b.closeErr = b.flush(context.Background())
			if b.cancel != nil {
				b.cancel()
			}
			b.Wait()
			close(b.closeDone)
		}()
	})

	select {
	case <-b.closeDone:
		return b.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) processBatch(_ context.Context, batch []item) error {
	if len(batch) == 0 {
		return nil
	}

	logItems, logEntries, replayLogs, logErr := b.prepareLogBatch(batch)
	if len(logItems) > 0 {
		logErr = errors.Join(logErr, b.writeLogBatch(logItems, logEntries, replayLogs))
	}

	spanItems, spanEntries, replaySpans, spanErr := b.prepareSpanBatch(batch)
	if len(spanItems) > 0 {
		spanErr = errors.Join(spanErr, b.writeSpanBatch(spanItems, spanEntries, replaySpans))
	}
	return errors.Join(logErr, spanErr)
}

func (b *Batcher) processLogBatch(_ context.Context, batch []item) error {
	if len(batch) == 0 {
		return nil
	}
	logItems, logEntries, replayLogs, err := b.prepareLogBatch(batch)
	if len(logItems) == 0 {
		return err
	}
	return errors.Join(err, b.writeLogBatch(logItems, logEntries, replayLogs))
}

func (b *Batcher) processSpanBatch(_ context.Context, batch []item) error {
	if len(batch) == 0 {
		return nil
	}
	spanItems, spanEntries, replaySpans, err := b.prepareSpanBatch(batch)
	if len(spanItems) == 0 {
		return err
	}
	return errors.Join(err, b.writeSpanBatch(spanItems, spanEntries, replaySpans))
}

func (b *Batcher) prepareLogBatch(batch []item) ([]storage.BatchItem, []*model.LogEntry, []model.LogEntry, error) {
	logItems := make([]storage.BatchItem, 0, len(batch))
	var logEntries []*model.LogEntry
	var replayEntries []model.LogEntry
	var errs []error
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
			errs = append(errs, fmt.Errorf("serialize log: %w", writeErr))
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
		if b.replaySink != nil && it.nativeReplay {
			replayEntries = append(replayEntries, *it.log)
		}
	}
	return logItems, logEntries, replayEntries, errors.Join(errs...)
}

func (b *Batcher) prepareSpanBatch(batch []item) ([]storage.BatchItem, []*model.SpanEntry, []model.SpanEntry, error) {
	spanItems := make([]storage.BatchItem, 0, len(batch))
	var spanEntries []*model.SpanEntry
	var replayEntries []model.SpanEntry
	var errs []error
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
			errs = append(errs, fmt.Errorf("serialize span: %w", writeErr))
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
		if b.replaySink != nil && it.nativeReplay {
			replayEntries = append(replayEntries, *it.span)
		}
	}
	return spanItems, spanEntries, replayEntries, errors.Join(errs...)
}

func (b *Batcher) writeLogBatch(logItems []storage.BatchItem, logEntries []*model.LogEntry, replayEntries []model.LogEntry) error {
	targetMeta, hasTarget := b.logManager.ActiveSegmentMeta()
	if err := b.logManager.WriteBatch(logItems); err != nil {
		b.logM.writeFailed.Add(uint64(len(logItems)))
		b.log.Error("log batch write failed", "err", err, "count", len(logItems))
		b.recordLogFailure()
		return err
	}
	if err := b.logManager.Flush(); err != nil {
		b.logM.writeFailed.Add(uint64(len(logItems)))
		b.log.Error("log segment flush failed", "err", err, "count", len(logItems))
		b.recordLogFailure()
		return err
	}
	if len(replayEntries) != 0 {
		if err := b.replaySink.AppendNormalizedLogs(replayEntries); err != nil {
			b.logM.writeFailed.Add(uint64(len(replayEntries)))
			b.log.Error("native log replay write failed", "err", err, "count", len(replayEntries))
			b.recordLogFailure()
			return err
		}
	}
	b.logM.durable.Add(uint64(len(logItems)))
	if hasTarget {
		updateSparseFromBatchMeta(b.logSparse, targetMeta, logItems)
	}
	if b.indexer != nil && len(logEntries) > 0 && b.segmentStillActive(b.logManager, targetMeta, hasTarget) {
		b.indexer.IndexLogEntries(logEntries)
	}
	if b.cacheInvalidator != nil && len(logItems) > 0 {
		from, to := batchTimeRange(logItems)
		b.cacheInvalidator.InvalidateLogResultRange(from, to)
	}
	b.resetLogFailures()
	return nil
}

func (b *Batcher) writeSpanBatch(spanItems []storage.BatchItem, spanEntries []*model.SpanEntry, replayEntries []model.SpanEntry) error {
	targetMeta, hasTarget := b.spanManager.ActiveSegmentMeta()
	if err := b.spanManager.WriteBatch(spanItems); err != nil {
		b.spanM.writeFailed.Add(uint64(len(spanItems)))
		b.log.Error("span batch write failed", "err", err, "count", len(spanItems))
		b.recordSpanFailure()
		return err
	}
	if err := b.spanManager.Flush(); err != nil {
		b.spanM.writeFailed.Add(uint64(len(spanItems)))
		b.log.Error("span segment flush failed", "err", err, "count", len(spanItems))
		b.recordSpanFailure()
		return err
	}
	if len(replayEntries) != 0 {
		if err := b.replaySink.AppendNormalizedSpans(replayEntries); err != nil {
			b.spanM.writeFailed.Add(uint64(len(replayEntries)))
			b.log.Error("native span replay write failed", "err", err, "count", len(replayEntries))
			b.recordSpanFailure()
			return err
		}
	}
	b.spanM.durable.Add(uint64(len(spanItems)))
	if hasTarget {
		updateSparseFromBatchMeta(b.spanSparse, targetMeta, spanItems)
	}
	if b.indexer != nil && len(spanEntries) > 0 && b.segmentStillActive(b.spanManager, targetMeta, hasTarget) {
		b.indexer.IndexSpanEntries(spanEntries)
	}
	if b.cacheInvalidator != nil && len(spanItems) > 0 {
		from, to := batchTimeRange(spanItems)
		b.cacheInvalidator.InvalidateSpanResultRange(from, to)
	}
	b.resetSpanFailures()
	return nil
}

// batchTimeRange returns the min/max event timestamps of a written batch.
func batchTimeRange(items []storage.BatchItem) (int64, int64) {
	from, to := items[0].TS, items[0].TS
	for _, item := range items[1:] {
		if item.TS < from {
			from = item.TS
		}
		if item.TS > to {
			to = item.TS
		}
	}
	return from, to
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
