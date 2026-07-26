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

type logQueueItem struct {
	entry        model.LogEntry
	nativeReplay bool
	// barrier marks a Flush sentinel: the worker flushes the pending batch,
	// then reports the lane's sticky outcome. Never carries data.
	barrier chan error
}

type spanQueueItem struct {
	entry        model.SpanEntry
	nativeReplay bool
	barrier      chan error
}

// Queue channels deliberately hold pointers to pooled items. With the default
// 100k slots per lane, embedding the 144-byte LogEntry and 160-byte SpanEntry
// values in channel rings would reserve tens of MiB before any telemetry is
// accepted. Pool ownership still removes the per-send allocation while keeping
// each ring slot pointer-sized.

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
	logQueue         chan *logQueueItem
	spanQueue        chan *spanQueueItem
	logItemPool      sync.Pool
	spanItemPool     sync.Pool
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
	// Implementations consume entries synchronously and must not retain the
	// slice or mutate its elements after the method returns.
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
	b := &Batcher{
		logManager:       deps.LogManager,
		spanManager:      deps.SpanManager,
		logSparse:        deps.LogSparse,
		spanSparse:       deps.SpanSparse,
		indexer:          deps.Indexer,
		logBatchSize:     logCfg.batchSize,
		spanBatchSize:    spanCfg.batchSize,
		logBatchTimeout:  logCfg.batchTimeout,
		spanBatchTimeout: spanCfg.batchTimeout,
		logQueue:         make(chan *logQueueItem, logCfg.queueSize),
		spanQueue:        make(chan *spanQueueItem, spanCfg.queueSize),
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
	b.logItemPool.New = func() any { return new(logQueueItem) }
	b.spanItemPool.New = func() any { return new(spanQueueItem) }
	return b
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
	go func() {
		var scratch logBatchScratch
		runLane(b, workerCtx, b.logQueue, b.logBatchSize, b.logBatchTimeout, func(ctx context.Context, batch []logQueueItem) error {
			return b.processLogBatchWithScratch(ctx, batch, &scratch)
		}, logItemBarrier, b.releaseLogItem)
	}()
	go func() {
		var scratch spanBatchScratch
		runLane(b, workerCtx, b.spanQueue, b.spanBatchSize, b.spanBatchTimeout, func(ctx context.Context, batch []spanQueueItem) error {
			return b.processSpanBatchWithScratch(ctx, batch, &scratch)
		}, spanItemBarrier, b.releaseSpanItem)
	}()
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
		it := b.acquireLogItem(entry, nativeReplay)
		select {
		case b.logQueue <- it:
			return true
		default:
			b.releaseLogItem(it)
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
		it := b.acquireSpanItem(span, nativeReplay)
		select {
		case b.spanQueue <- it:
			return true
		default:
			b.releaseSpanItem(it)
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

func (b *Batcher) acquireLogItem(entry model.LogEntry, nativeReplay bool) *logQueueItem {
	it := b.logItemPool.Get().(*logQueueItem)
	it.entry = entry
	it.nativeReplay = nativeReplay
	return it
}

func (b *Batcher) releaseLogItem(it *logQueueItem) {
	*it = logQueueItem{}
	b.logItemPool.Put(it)
}

func (b *Batcher) acquireSpanItem(entry model.SpanEntry, nativeReplay bool) *spanQueueItem {
	it := b.spanItemPool.Get().(*spanQueueItem)
	it.entry = entry
	it.nativeReplay = nativeReplay
	return it
}

func (b *Batcher) releaseSpanItem(it *spanQueueItem) {
	*it = spanQueueItem{}
	b.spanItemPool.Put(it)
}

func logItemBarrier(it *logQueueItem) chan error   { return it.barrier }
func spanItemBarrier(it *spanQueueItem) chan error { return it.barrier }

func runLane[T any](
	b *Batcher,
	ctx context.Context,
	queue <-chan *T,
	batchSize int,
	batchTimeout time.Duration,
	process func(context.Context, []T) error,
	barrier func(*T) chan error,
	release func(*T),
) {
	defer b.wg.Done()

	batch := make([]T, 0, batchSize)
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
		clear(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case it := <-queue:
					if done := barrier(it); done != nil {
						release(it)
						flush()
						done <- stickyErr
						continue
					}
					batch = append(batch, *it)
					release(it)
					if len(batch) >= batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case it := <-queue:
			if done := barrier(it); done != nil {
				release(it)
				flush()
				done <- stickyErr
				continue
			}
			batch = append(batch, *it)
			release(it)
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
func (b *Batcher) FlushLogs(ctx context.Context) error {
	b.admitMu.RLock()
	defer b.admitMu.RUnlock()
	if !b.accepting {
		return ErrClosed
	}
	done, err := b.enqueueLogBarrier(ctx)
	if err != nil {
		return err
	}
	return waitBarrier(ctx, done)
}

func (b *Batcher) FlushSpans(ctx context.Context) error {
	b.admitMu.RLock()
	defer b.admitMu.RUnlock()
	if !b.accepting {
		return ErrClosed
	}
	done, err := b.enqueueSpanBarrier(ctx)
	if err != nil {
		return err
	}
	return waitBarrier(ctx, done)
}

func (b *Batcher) enqueueLogBarrier(ctx context.Context) (chan error, error) {
	done := make(chan error, 1)
	it := b.logItemPool.Get().(*logQueueItem)
	it.barrier = done
	select {
	case b.logQueue <- it:
	case <-ctx.Done():
		b.releaseLogItem(it)
		return nil, ctx.Err()
	}
	return done, nil
}

func (b *Batcher) enqueueSpanBarrier(ctx context.Context) (chan error, error) {
	done := make(chan error, 1)
	it := b.spanItemPool.Get().(*spanQueueItem)
	it.barrier = done
	select {
	case b.spanQueue <- it:
	case <-ctx.Done():
		b.releaseSpanItem(it)
		return nil, ctx.Err()
	}
	return done, nil
}

func waitBarrier(ctx context.Context, done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) flush(ctx context.Context) error {
	logDone, err := b.enqueueLogBarrier(ctx)
	if err != nil {
		return err
	}
	spanDone, err := b.enqueueSpanBarrier(ctx)
	if err != nil {
		return err
	}
	var errs []error
	for _, done := range []<-chan error{logDone, spanDone} {
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

func (b *Batcher) processLogBatch(ctx context.Context, batch []logQueueItem) error {
	var scratch logBatchScratch
	return b.processLogBatchWithScratch(ctx, batch, &scratch)
}

func (b *Batcher) processLogBatchWithScratch(_ context.Context, batch []logQueueItem, scratch *logBatchScratch) error {
	if len(batch) == 0 {
		return nil
	}
	logItems, logEntries, replayLogs, err := b.prepareLogBatch(batch, scratch)
	defer scratch.finish()
	if len(logItems) == 0 {
		return err
	}
	return errors.Join(err, b.writeLogBatch(logItems, logEntries, replayLogs))
}

func (b *Batcher) processSpanBatch(ctx context.Context, batch []spanQueueItem) error {
	var scratch spanBatchScratch
	return b.processSpanBatchWithScratch(ctx, batch, &scratch)
}

func (b *Batcher) processSpanBatchWithScratch(_ context.Context, batch []spanQueueItem, scratch *spanBatchScratch) error {
	if len(batch) == 0 {
		return nil
	}
	spanItems, spanEntries, replaySpans, err := b.prepareSpanBatch(batch, scratch)
	defer scratch.finish()
	if len(spanItems) == 0 {
		return err
	}
	return errors.Join(err, b.writeSpanBatch(spanItems, spanEntries, replaySpans))
}

func (b *Batcher) prepareLogBatch(batch []logQueueItem, scratch *logBatchScratch) ([]storage.BatchItem, []*model.LogEntry, []model.LogEntry, error) {
	payloadSize, sizeErr := logBatchPayloadSize(batch)
	if sizeErr != nil {
		return nil, nil, nil, sizeErr
	}
	scratch.prepare(len(batch), payloadSize, b.indexer != nil, b.replaySink != nil)
	var errs []error
	for i := range batch {
		it := &batch[i]
		start := len(scratch.payload)
		payload, writeErr := it.entry.AppendTo(scratch.payload)
		if writeErr != nil {
			b.logM.serializeErr.Inc()
			b.log.Error("serialize log entry", "err", writeErr)
			errs = append(errs, fmt.Errorf("serialize log: %w", writeErr))
			continue
		}
		scratch.payload = payload

		scratch.items = append(scratch.items, storage.BatchItem{
			Data: scratch.payload[start:],
			TS:   it.entry.Timestamp.UnixNano(),
		})
		if b.indexer != nil {
			scratch.index = append(scratch.index, &it.entry)
		}
		if b.replaySink != nil && it.nativeReplay {
			scratch.replay = append(scratch.replay, it.entry)
		}
	}
	return scratch.items, scratch.index, scratch.replay, errors.Join(errs...)
}

func (b *Batcher) prepareSpanBatch(batch []spanQueueItem, scratch *spanBatchScratch) ([]storage.BatchItem, []*model.SpanEntry, []model.SpanEntry, error) {
	payloadSize, sizeErr := spanBatchPayloadSize(batch)
	if sizeErr != nil {
		return nil, nil, nil, sizeErr
	}
	scratch.prepare(len(batch), payloadSize, b.indexer != nil, b.replaySink != nil)
	var errs []error
	for i := range batch {
		it := &batch[i]
		start := len(scratch.payload)
		payload, writeErr := it.entry.AppendTo(scratch.payload)
		if writeErr != nil {
			b.spanM.serializeErr.Inc()
			b.log.Error("serialize span entry", "err", writeErr)
			errs = append(errs, fmt.Errorf("serialize span: %w", writeErr))
			continue
		}
		scratch.payload = payload

		scratch.items = append(scratch.items, storage.BatchItem{
			Data: scratch.payload[start:],
			TS:   it.entry.StartTime.UnixNano(),
		})
		if b.indexer != nil {
			scratch.index = append(scratch.index, &it.entry)
		}
		if b.replaySink != nil && it.nativeReplay {
			scratch.replay = append(scratch.replay, it.entry)
		}
	}
	return scratch.items, scratch.index, scratch.replay, errors.Join(errs...)
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
