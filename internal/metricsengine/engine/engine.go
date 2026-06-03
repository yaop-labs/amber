package engine

import (
	"errors"
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
	// WALFlushInterval bounds how often the WAL committer goroutine fsyncs.
	// Concurrent AppendBatch callers all funnel into this single fsync
	// (Postgres-style group commit), so under load fsync amortises across
	// many batches. Default 5ms. Durability bound: a successful
	// AppendBatch return implies the records were fsync'd at most
	// WALFlushInterval ago.
	WALFlushInterval time.Duration
}

type Engine struct {
	mu        sync.Mutex
	registry  *index.Registry
	head      *head.Head
	wal       *wal.WAL
	committer *committer
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
		if err := wal.Replay(opts.WALPath, func(record wal.Record) error {
			e.head.Append(record.Labels, record.Type, record.Timestamp, record.Value)
			return nil
		}); err != nil {
			return nil, err
		}
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
	// Group-commit path also applies to single-record Append: it's just
	// AppendBatch with one record. The committer absorbs the overhead.
	ids, err := e.AppendBatch([]model.Sample{{
		Labels: labels, Type: typ, Timestamp: timestamp, Value: value,
	}})
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// AppendBatch is the group-commit ingest hot path. The expensive (and
// previously serialising) part — WAL fsync — is moved OUT from under the
// engine mutex into a single background committer that fsyncs a fixed
// number of times per second regardless of how many writers are calling.
// Each writer waits on a Cond until the committer's last fsync covers its
// own seq; up to that point the call holds NO locks except briefly the
// WAL's own append cursor lock (memcpy-only path, no I/O).
//
// In-memory head + registry update is still serialised by e.mu, but the
// critical section there is microseconds (slice append + map insert),
// not the previous 5-15ms of disk fsync.
//
// Ordering invariant: WAL fsync must complete BEFORE head append, so that
// a crash between the two cannot leave head with a sample whose WAL
// record was never fsync'd (replay would silently drop it on restart).
func (e *Engine) AppendBatch(samples []model.Sample) ([]index.SeriesID, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	// 1. Canonicalise labels and build WAL records OUTSIDE any lock. This
	//    is CPU-bound and embarrassingly parallel.
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

	// 2. WAL append + fsync via the group-commit committer. Concurrent
	//    callers funnel their writes into one shared fsync. e.mu is NOT
	//    held during this wait, so the head append step below remains
	//    parallel across goroutines that finished fsync at the same tick.
	if e.committer != nil {
		if err := e.committer.Append(records); err != nil {
			return nil, err
		}
	}

	// 3. In-memory state under e.mu — microseconds.
	e.mu.Lock()
	defer e.mu.Unlock()
	ids := make([]index.SeriesID, 0, len(samples))
	for _, sample := range samples {
		ids = append(ids, e.head.Append(sample.Labels, sample.Type, sample.Timestamp, sample.Value))
	}
	return ids, nil
}

func (e *Engine) AppendScaledFloat(labels model.LabelSet, typ model.MetricType, timestamp int64, value float64, scale int64) (index.SeriesID, error) {
	if scale <= 0 {
		return 0, errors.New("engine: scale must be positive")
	}
	scaled := int64(math.Round(value * float64(scale)))
	return e.Append(labels, typ, timestamp, scaled)
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
