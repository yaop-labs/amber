package engine

import (
	"errors"
	"math"
	"sync"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/head"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

type Options struct {
	WALPath string
}

type Engine struct {
	mu       sync.Mutex
	registry *index.Registry
	head     *head.Head
	wal      *wal.WAL
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
	}
	return e, nil
}

func (e *Engine) Append(labels model.LabelSet, typ model.MetricType, timestamp int64, value int64) (index.SeriesID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.wal != nil {
		if err := e.wal.Append(wal.Record{
			Labels:    labels.Canonical(),
			Type:      typ,
			Timestamp: timestamp,
			Value:     value,
		}); err != nil {
			return 0, err
		}
	}
	return e.head.Append(labels, typ, timestamp, value), nil
}

func (e *Engine) AppendBatch(samples []model.Sample) ([]index.SeriesID, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ids := make([]index.SeriesID, 0, len(samples))
	if len(samples) == 0 {
		return ids, nil
	}
	if e.wal != nil {
		records := make([]wal.Record, 0, len(samples))
		for _, sample := range samples {
			records = append(records, wal.Record{
				Labels:    sample.Labels.Canonical(),
				Type:      sample.Type,
				Timestamp: sample.Timestamp,
				Value:     sample.Value,
			})
		}
		if err := e.wal.AppendBatch(records); err != nil {
			return nil, err
		}
	}
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
	if e.wal == nil {
		return nil
	}
	return e.wal.Close()
}
