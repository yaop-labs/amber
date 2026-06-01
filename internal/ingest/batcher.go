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
	batchSize        int
	batchTimeout     time.Duration
	queue            chan item
	log              *slog.Logger
	wg               sync.WaitGroup
	breakerThreshold uint64
	consecFailures   atomic.Uint64
	guard            *CardinalityGuard

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
	Logger      *slog.Logger
}

type Config struct {
	BatchSize        int
	BatchTimeout     time.Duration
	QueueSize        int
	BreakerThreshold int
}

func NewBatcher(deps Deps, cfg Config) *Batcher {
	var threshold uint64
	if cfg.BreakerThreshold > 0 {
		threshold = uint64(cfg.BreakerThreshold)
	}
	return &Batcher{
		logManager:       deps.LogManager,
		spanManager:      deps.SpanManager,
		logSparse:        deps.LogSparse,
		spanSparse:       deps.SpanSparse,
		indexer:          deps.Indexer,
		batchSize:        cfg.BatchSize,
		batchTimeout:     cfg.BatchTimeout,
		queue:            make(chan item, cfg.QueueSize),
		log:              deps.Logger,
		breakerThreshold: threshold,
		guard:            deps.Guard,
		logM:             newKindMetrics("log"),
		spanM:            newKindMetrics("span"),
	}
}

func (b *Batcher) IsBreakerOpen() bool {
	return b.breakerThreshold > 0 && b.consecFailures.Load() >= b.breakerThreshold
}

func (b *Batcher) Start(ctx context.Context) {
	b.wg.Add(1)
	go b.run(ctx)
}

func (b *Batcher) Wait() {
	b.wg.Wait()
}

func (b *Batcher) SendLog(entry model.LogEntry) error {
	if b.IsBreakerOpen() {
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
	case b.queue <- item{log: &entry}:
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
	if b.IsBreakerOpen() {
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
	case b.queue <- item{span: &span}:
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

func (b *Batcher) QueueLen() int { return len(b.queue) }

func (b *Batcher) TrySendLog(entry model.LogEntry) bool {
	return b.SendLog(entry) == nil
}

var bufPool = sync.Pool{
	New: func() any { return &bytes.Buffer{} },
}

func (b *Batcher) run(ctx context.Context) {
	defer b.wg.Done()

	batch := make([]item, 0, b.batchSize)
	ticker := time.NewTicker(b.batchTimeout)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.processBatch(ctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case it := <-b.queue:
					batch = append(batch, it)
					if len(batch) >= b.batchSize {
						flush()
					}
				default:
					flush()
					return
				}
			}

		case it := <-b.queue:
			batch = append(batch, it)
			if len(batch) >= b.batchSize {
				flush()
				ticker.Reset(b.batchTimeout)
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

	logItems := make([]storage.BatchItem, 0, len(batch))
	spanItems := make([]storage.BatchItem, 0)
	var logEntries []*model.LogEntry
	var spanEntries []*model.SpanEntry

	for _, it := range batch {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

		var ts int64
		var writeErr error

		if it.log != nil {
			_, writeErr = it.log.WriteTo(buf)
			ts = it.log.Timestamp.UnixNano()
		} else if it.span != nil {
			_, writeErr = it.span.WriteTo(buf)
			ts = it.span.StartTime.UnixNano()
		}

		if writeErr != nil {
			if it.span != nil {
				b.spanM.serializeErr.Inc()
			} else {
				b.logM.serializeErr.Inc()
			}
			b.log.Error("serialize entry", "err", writeErr)
			bufPool.Put(buf)
			continue
		}

		data := make([]byte, buf.Len())
		copy(data, buf.Bytes())
		bufPool.Put(buf)

		bi := storage.BatchItem{Data: data, TS: ts}
		if it.log != nil {
			logItems = append(logItems, bi)
			if b.indexer != nil {
				if logEntries == nil {
					logEntries = make([]*model.LogEntry, 0, len(batch))
				}
				logEntries = append(logEntries, it.log)
			}
		} else {
			spanItems = append(spanItems, bi)
			if b.indexer != nil {
				if spanEntries == nil {
					spanEntries = make([]*model.SpanEntry, 0, len(batch))
				}
				spanEntries = append(spanEntries, it.span)
			}
		}
	}

	anyAttempted := len(logItems) > 0 || len(spanItems) > 0
	anyFailed := false

	if len(logItems) > 0 {
		if err := b.logManager.WriteBatch(logItems); err != nil {
			anyFailed = true
			b.logM.writeFailed.Add(uint64(len(logItems)))
			b.log.Error("log batch write failed", "err", err, "count", len(logItems))
		} else {
			b.logM.accepted.Add(uint64(len(logItems)))
			updateSparseFromBatch(b.logSparse, b.logManager, logItems)
			if b.indexer != nil && len(logEntries) > 0 {
				b.indexer.IndexLogEntries(logEntries)
			}
			if err := b.logManager.Flush(); err != nil {
				b.log.Warn("log segment flush failed", "err", err)
			}
		}
	}

	if len(spanItems) > 0 {
		if err := b.spanManager.WriteBatch(spanItems); err != nil {
			anyFailed = true
			b.spanM.writeFailed.Add(uint64(len(spanItems)))
			b.log.Error("span batch write failed", "err", err, "count", len(spanItems))
		} else {
			b.spanM.accepted.Add(uint64(len(spanItems)))
			updateSparseFromBatch(b.spanSparse, b.spanManager, spanItems)
			if b.indexer != nil && len(spanEntries) > 0 {
				b.indexer.IndexSpanEntries(spanEntries)
			}
			if err := b.spanManager.Flush(); err != nil {
				b.log.Warn("span segment flush failed", "err", err)
			}
		}
	}

	if !anyAttempted {
		return
	}
	if anyFailed {
		n := b.consecFailures.Add(1)
		if b.breakerThreshold > 0 && n == b.breakerThreshold {
			b.log.Error("ingest breaker tripped", "consecutive_failures", n)
		}
	} else if b.consecFailures.Swap(0) >= b.breakerThreshold && b.breakerThreshold > 0 {
		b.log.Info("ingest breaker reset")
	}
}

func updateSparseFromBatch(sparse *index.SparseIndex, manager *storage.SegmentManager, items []storage.BatchItem) {
	if len(items) == 0 {
		return
	}
	activeMeta, ok := manager.ActiveSegmentMeta()
	if !ok {
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
	sparse.TouchRange(activeMeta.ID, activeMeta.FileName, minTS, maxTS)
}
