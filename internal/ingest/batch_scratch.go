package ingest

import (
	"fmt"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// A production batcher has one log worker and one span worker. Each worker
// owns one scratch value for its full lifetime, so the hot path can reuse
// memory without synchronization or a process-wide pool.
type logBatchScratch struct {
	payload []byte
	items   []storage.BatchItem
	index   []*model.LogEntry
	replay  []model.LogEntry
}

type spanBatchScratch struct {
	payload []byte
	items   []storage.BatchItem
	index   []*model.SpanEntry
	replay  []model.SpanEntry
}

const (
	// A single unusually large record must not permanently pin its backing
	// array in an otherwise small-batch worker.
	maxRetainedBatchPayload = 4 << 20
	maxRetainedBatchItems   = 16 << 10
	maxIntValue             = int(^uint(0) >> 1)
)

func (s *logBatchScratch) prepare(batchLen, payloadSize int, index, replay bool) {
	s.payload = resizePayload(s.payload, payloadSize)
	s.items = resetBatchSlice(s.items, batchLen)
	s.index = resetOptionalBatchSlice(s.index, batchLen, index)
	s.replay = resetOptionalBatchSlice(s.replay, batchLen, replay)
}

func (s *spanBatchScratch) prepare(batchLen, payloadSize int, index, replay bool) {
	s.payload = resizePayload(s.payload, payloadSize)
	s.items = resetBatchSlice(s.items, batchLen)
	s.index = resetOptionalBatchSlice(s.index, batchLen, index)
	s.replay = resetOptionalBatchSlice(s.replay, batchLen, replay)
}

func (s *logBatchScratch) finish() {
	clear(s.items)
	clear(s.index)
	clear(s.replay)
	s.items = retainBatchSlice(s.items)
	s.index = retainBatchSlice(s.index)
	s.replay = retainBatchSlice(s.replay)
	s.payload = retainPayload(s.payload)
}

func (s *spanBatchScratch) finish() {
	clear(s.items)
	clear(s.index)
	clear(s.replay)
	s.items = retainBatchSlice(s.items)
	s.index = retainBatchSlice(s.index)
	s.replay = retainBatchSlice(s.replay)
	s.payload = retainPayload(s.payload)
}

func resizePayload(payload []byte, size int) []byte {
	if cap(payload) < size {
		return make([]byte, 0, size)
	}
	return payload[:0]
}

func retainPayload(payload []byte) []byte {
	if cap(payload) > maxRetainedBatchPayload {
		return nil
	}
	return payload[:0]
}

func resetBatchSlice[S ~[]E, E any](items S, size int) S {
	clear(items)
	if cap(items) < size {
		return make(S, 0, size)
	}
	return items[:0]
}

func resetOptionalBatchSlice[S ~[]E, E any](items S, size int, enabled bool) S {
	clear(items)
	if !enabled {
		return items[:0]
	}
	if cap(items) < size {
		return make(S, 0, size)
	}
	return items[:0]
}

func retainBatchSlice[S ~[]E, E any](items S) S {
	if cap(items) > maxRetainedBatchItems {
		return nil
	}
	return items[:0]
}

func logBatchPayloadSize(batch []item) (int, error) {
	total := 0
	for _, it := range batch {
		if it.log == nil {
			continue
		}
		size, err := it.log.EncodedSize()
		if err != nil {
			// prepareLogBatch reports the entry-specific error while encoding.
			continue
		}
		if size > uint64(maxIntValue-total) {
			return 0, fmt.Errorf("serialize log batch: encoded size overflow")
		}
		total += int(size)
	}
	return total, nil
}

func spanBatchPayloadSize(batch []item) (int, error) {
	total := 0
	for _, it := range batch {
		if it.span == nil {
			continue
		}
		size, err := it.span.EncodedSize()
		if err != nil {
			continue
		}
		if size > uint64(maxIntValue-total) {
			return 0, fmt.Errorf("serialize span batch: encoded size overflow")
		}
		total += int(size)
	}
	return total, nil
}
