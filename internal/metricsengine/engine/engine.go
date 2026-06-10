package engine

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/head"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

type Options struct {
	WALPath string
	// WALFlushInterval bounds how long AppendBatch waits for a batched fsync.
	WALFlushInterval time.Duration
}

type Engine struct {
	mu        sync.Mutex
	registry  *index.Registry
	head      *head.Head
	wal       *wal.WAL
	committer *committer

	// walRecovery captures what RecoverReplay found at open time. A non-zero
	// TruncatedBytes means a corrupt or torn WAL tail was dropped.
	walRecovery wal.RecoverStats
}

func New() *Engine {
	e, err := Open(Options{})
	if err != nil {
		panic(err)
	}
	return e
}

func Open(opts Options) (*Engine, error) {
	return OpenWithRegistry(index.NewRegistry(), opts)
}

func OpenWithRegistry(registry *index.Registry, opts Options) (*Engine, error) {
	if registry == nil {
		registry = index.NewRegistry()
	}
	e := &Engine{
		registry: registry,
		head:     head.New(registry),
	}
	if opts.WALPath != "" {
		// RecoverReplay tolerates a corrupt or torn tail (crash mid-write):
		// it replays the valid prefix and truncates the garbage in place so
		// the WAL stays appendable and replayable. A hard error here would
		// make the store unopenable after any torn write.
		stats, err := wal.RecoverReplay(opts.WALPath, func(record wal.Record) error {
			e.head.Append(record.Labels, record.Type, record.Timestamp, record.Value)
			return nil
		})
		if err != nil {
			return nil, err
		}
		e.walRecovery = stats
		w, err := wal.Open(opts.WALPath)
		if err != nil {
			return nil, err
		}
		e.wal = w
		e.committer = newCommitter(w, opts.WALFlushInterval)
	}
	return e, nil
}

func (e *Engine) Append(labels model.LabelSet, typ model.MetricType, timestamp int64, value int64) (index.SeriesID, error) {
	ids, err := e.AppendBatch([]model.Sample{{
		Labels: labels, Type: typ, Timestamp: timestamp, Value: value,
	}})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// AppendBatch appends samples to the WAL and in-memory head.
// The WAL sync completes before the head update, so replay can recover every
// acknowledged sample after a crash.
func (e *Engine) AppendBatch(samples []model.Sample) ([]index.SeriesID, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	var records []wal.Record
	if e.committer != nil {
		records = make([]wal.Record, len(samples))
		for i, sample := range samples {
			records[i] = wal.Record{
				Labels:    sample.Labels.Canonical(),
				Type:      sample.Type,
				Timestamp: sample.Timestamp,
				Value:     sample.Value,
			}
		}
	}

	if e.committer != nil {
		if err := e.committer.Append(records); err != nil {
			return nil, err
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	ids := make([]index.SeriesID, 0, len(samples))
	for _, sample := range samples {
		ids = append(ids, e.head.Append(sample.Labels, sample.Type, sample.Timestamp, sample.Value))
	}
	return ids, nil
}

// maxScaledFloat is float64(math.MaxInt64) == 2^63. Scaled values at or
// beyond this magnitude cannot be represented in the int64 value model.
const maxScaledFloat = float64(math.MaxInt64)

// AppendScaledFloat stores a float64 as round(value*scale) in the int64
// value model. Values smaller than 1/scale collapse to 0; NaN, ±Inf, and
// values whose scaled form overflows int64 are rejected instead of being
// stored as garbage.
func (e *Engine) AppendScaledFloat(labels model.LabelSet, typ model.MetricType, timestamp int64, value float64, scale int64) (index.SeriesID, error) {
	if scale <= 0 {
		return 0, errors.New("engine: scale must be positive")
	}
	scaled := math.Round(value * float64(scale))
	if math.IsNaN(scaled) {
		return 0, errors.New("engine: cannot store NaN value")
	}
	if scaled >= maxScaledFloat || scaled <= -maxScaledFloat {
		return 0, fmt.Errorf("engine: value %v at scale %d overflows int64", value, scale)
	}
	return e.Append(labels, typ, timestamp, int64(scaled))
}

func (e *Engine) FlushBlock(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.writeBlockLocked(path); err != nil {
		return err
	}
	return e.commitFlushLocked()
}

func (e *Engine) PrepareFlushBlock(path string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.writeBlockLocked(path)
}

func (e *Engine) CommitFlush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commitFlushLocked()
}

func (e *Engine) writeBlockLocked(path string) error {
	return block.WriteFile(path, e.head.Snapshot())
}

func (e *Engine) commitFlushLocked() error {
	e.head.Reset()
	if e.wal != nil {
		return e.wal.Truncate()
	}
	return nil
}

func (e *Engine) BufferedSeries() int {
	return e.head.Len()
}

func (e *Engine) BufferedSamples() int {
	return e.head.SampleCount()
}

func (e *Engine) Snapshot() []block.Series {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.head.Snapshot()
}

func (e *Engine) Registry() *index.Registry {
	return e.registry
}

// WALRecoveryStats reports what the open-time WAL replay found. A non-zero
// TruncatedBytes means a corrupt or torn tail was dropped — worth surfacing
// as a metric or log line by the embedding layer.
func (e *Engine) WALRecoveryStats() wal.RecoverStats {
	return e.walRecovery
}

func (e *Engine) Close() error {
	if e.committer != nil {
		// Drain pending fsyncs first so callers that returned successfully
		// stay durable after Close. flushAndStop does one final tick before
		// the goroutine exits.
		if err := e.committer.flushAndStop(); err != nil {
			return err
		}
	}
	if e.wal == nil {
		return nil
	}
	return e.wal.Close()
}
