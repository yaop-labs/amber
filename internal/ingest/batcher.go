package ingest

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hnlbs/amber/internal/index"
	"github.com/hnlbs/amber/internal/metrics"
	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/storage"
)

type item struct {
	log  *model.LogEntry
	span *model.SpanEntry
}

var (
	ErrQueueFull   = errors.New("ingest queue full")
	ErrBreakerOpen = errors.New("ingest circuit breaker open")
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
}

func NewBatcher(
	logManager *storage.SegmentManager,
	spanManager *storage.SegmentManager,
	logSparse *index.SparseIndex,
	spanSparse *index.SparseIndex,
	indexer ActiveIndexer,
	batchSize int,
	batchTimeout time.Duration,
	queueSize int,
	breakerThreshold int,
	log *slog.Logger,
) *Batcher {
	var threshold uint64
	if breakerThreshold > 0 {
		threshold = uint64(breakerThreshold)
	}
	return &Batcher{
		logManager:       logManager,
		spanManager:      spanManager,
		logSparse:        logSparse,
		spanSparse:       spanSparse,
		indexer:          indexer,
		batchSize:        batchSize,
		batchTimeout:     batchTimeout,
		queue:            make(chan item, queueSize),
		log:              log,
		breakerThreshold: threshold,
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
		metrics.IngestDropped.WithLabelValues("log", "breaker_open").Inc()
		return ErrBreakerOpen
	}
	select {
	case b.queue <- item{log: &entry}:
		return nil
	default:
		metrics.IngestDropped.WithLabelValues("log", "queue_full").Inc()
		b.log.Warn("ingest queue full, dropping log entry",
			"entry_id", entry.ID.String(),
			"service", entry.Service,
		)
		return ErrQueueFull
	}
}

func (b *Batcher) SendSpan(span model.SpanEntry) error {
	if b.IsBreakerOpen() {
		metrics.IngestDropped.WithLabelValues("span", "breaker_open").Inc()
		return ErrBreakerOpen
	}
	select {
	case b.queue <- item{span: &span}:
		return nil
	default:
		metrics.IngestDropped.WithLabelValues("span", "queue_full").Inc()
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
			kind := "log"
			if it.span != nil {
				kind = "span"
			}
			metrics.IngestDropped.WithLabelValues(kind, "serialize_error").Inc()
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
			metrics.IngestDropped.WithLabelValues("log", "write_failed").Add(float64(len(logItems)))
			b.log.Error("log batch write failed", "err", err, "count", len(logItems))
		} else {
			metrics.IngestAccepted.WithLabelValues("log").Add(float64(len(logItems)))
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
			metrics.IngestDropped.WithLabelValues("span", "write_failed").Add(float64(len(spanItems)))
			b.log.Error("span batch write failed", "err", err, "count", len(spanItems))
		} else {
			metrics.IngestAccepted.WithLabelValues("span").Add(float64(len(spanItems)))
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
	activeMeta, ok := manager.ActiveSegmentMeta()
	if !ok {
		return
	}
	var minTS, maxTS int64
	for _, it := range items {
		if minTS == 0 || it.TS < minTS {
			minTS = it.TS
		}
		if it.TS > maxTS {
			maxTS = it.TS
		}
	}
	sparse.TouchRange(activeMeta.ID, activeMeta.FileName, minTS, maxTS)
}
