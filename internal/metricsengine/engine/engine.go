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

	// flushGate excludes flushes from in-flight appends. An append holds the
	// read side from WAL enqueue through the head update; flush holds the
	// write side from snapshot through WAL truncate. Without it a sample
	// could be acked (WAL fsynced), miss the flush snapshot, have its WAL
	// record truncated, and survive only in the head — i.e. an acked sample
	// lost on the next crash.
	flushGate sync.RWMutex
	// gateHeld tracks the Prepare→Commit/Abort pairing so releasing is
	// idempotent and a Commit without a Prepare cannot unlock an unheld gate.
	gateHeld bool
	gateMu   sync.Mutex

	// walMu orders WAL enqueues so a series record always precedes the first
	// sample referencing it, across goroutines. Fsync waits happen outside.
	walMu sync.Mutex
	// declared tracks series already written as KindSeries records in the
	// current WAL generation (since the last truncate).
	declared map[index.SeriesID]struct{}

	// walRecovery captures what RecoverReplay found at open time. A non-zero
	// TruncatedBytes means a corrupt or torn WAL tail was dropped.
	walRecovery wal.RecoverStats
	// walUnknownSeries counts replayed sample records whose series ID was
	// resolvable neither from the WAL's series records nor the registry
	// (possible only for WALs from the pre-binary format era or after
	// catalog loss); those samples are skipped.
	walUnknownSeries int
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
		declared: make(map[index.SeriesID]struct{}),
	}
	if opts.WALPath != "" {
		// RecoverReplay tolerates a corrupt or torn tail (crash mid-write):
		// it replays the valid prefix and truncates the garbage in place so
		// the WAL stays appendable and replayable. A hard error here would
		// make the store unopenable after any torn write.
		stats, err := wal.RecoverReplay(opts.WALPath, e.replayRecord)
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

// replayRecord applies one WAL record during open.
func (e *Engine) replayRecord(record wal.Record) error {
	switch record.Kind {
	case wal.KindSeries:
		id := index.SeriesID(record.ID)
		e.registry.Import(id, record.Labels)
		// The series record is still in the WAL after replay (truncate only
		// happens on flush), so it stays declared for this generation.
		e.declared[id] = struct{}{}
		return nil
	case wal.KindSample:
		id := index.SeriesID(record.ID)
		labels, ok := e.registry.Labels(id)
		if !ok {
			e.walUnknownSeries++
			return nil
		}
		e.head.AppendWithID(id, labels, record.Type, record.Timestamp, record.Value)
		return nil
	case wal.KindLegacySample:
		e.head.Append(record.Labels, record.Type, record.Timestamp, record.Value)
		return nil
	default:
		return fmt.Errorf("engine: unknown WAL record kind %d", record.Kind)
	}
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

// AppendBatch appends samples to the WAL and in-memory head. The WAL sync
// completes before the head update, so replay recovers every acknowledged
// sample after a crash. WAL records are ID-based: a series' labels are
// written once per WAL generation (KindSeries), each sample is a compact
// (id, type, ts, value) record.
func (e *Engine) AppendBatch(samples []model.Sample) ([]index.SeriesID, error) {
	if len(samples) == 0 {
		return nil, nil
	}

	if e.committer == nil {
		e.mu.Lock()
		defer e.mu.Unlock()
		ids := make([]index.SeriesID, 0, len(samples))
		for _, sample := range samples {
			ids = append(ids, e.head.Append(sample.Labels, sample.Type, sample.Timestamp, sample.Value))
		}
		return ids, nil
	}

	e.flushGate.RLock()
	defer e.flushGate.RUnlock()

	ids := make([]index.SeriesID, len(samples))
	canonical := make([]model.LabelSet, len(samples))

	// Resolve IDs and enqueue WAL records under walMu so a KindSeries record
	// lands in the file before any KindSample that references it, regardless
	// of goroutine interleaving. The fsync wait happens after the lock is
	// released, so concurrent appenders still share one group commit.
	e.walMu.Lock()
	records := make([]wal.Record, 0, len(samples))
	for i, sample := range samples {
		labels := sample.Labels.Canonical()
		id := e.registry.GetOrCreateAt(labels, sample.Timestamp)
		ids[i] = id
		canonical[i] = labels
		if _, ok := e.declared[id]; !ok {
			e.declared[id] = struct{}{}
			records = append(records, wal.Record{Kind: wal.KindSeries, ID: uint64(id), Labels: labels})
		}
		records = append(records, wal.Record{
			Kind:      wal.KindSample,
			ID:        uint64(id),
			Type:      sample.Type,
			Timestamp: sample.Timestamp,
			Value:     sample.Value,
		})
	}
	seq, err := e.committer.enqueue(records)
	e.walMu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := e.committer.waitSynced(seq); err != nil {
		return nil, err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	for i, sample := range samples {
		e.head.AppendWithID(ids[i], canonical[i], sample.Type, sample.Timestamp, sample.Value)
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

// FlushBlock snapshots the head into a block and commits in one step.
func (e *Engine) FlushBlock(path string) error {
	e.acquireGate()
	defer e.releaseGate()
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.writeBlockLocked(path); err != nil {
		return err
	}
	return e.commitFlushLocked()
}

// PrepareFlushBlock writes the head snapshot to a block file and holds the
// flush gate: appends are excluded until CommitFlush or AbortFlush releases
// it. Without the gate a sample acked between snapshot and WAL truncate
// would survive only in memory.
func (e *Engine) PrepareFlushBlock(path string) error {
	e.acquireGate()
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.writeBlockLocked(path); err != nil {
		e.releaseGate()
		return err
	}
	return nil
}

// CommitFlush resets the head and truncates the WAL, then releases the gate
// taken by PrepareFlushBlock.
func (e *Engine) CommitFlush() error {
	defer e.releaseGate()
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.commitFlushLocked()
}

// AbortFlush releases the flush gate without committing; the prepared block
// file, if any, is the caller's to discard. Safe to call when no flush is
// prepared.
func (e *Engine) AbortFlush() {
	e.releaseGate()
}

func (e *Engine) acquireGate() {
	e.flushGate.Lock()
	e.gateMu.Lock()
	e.gateHeld = true
	e.gateMu.Unlock()
}

func (e *Engine) releaseGate() {
	e.gateMu.Lock()
	held := e.gateHeld
	e.gateHeld = false
	e.gateMu.Unlock()
	if held {
		e.flushGate.Unlock()
	}
}

func (e *Engine) writeBlockLocked(path string) error {
	return block.WriteFile(path, e.head.Snapshot())
}

func (e *Engine) commitFlushLocked() error {
	e.head.Reset()
	// New WAL generation: every series must be re-declared before its next
	// sample. Caller holds the flush gate, so no append interleaves between
	// the truncate and this reset.
	e.walMu.Lock()
	e.declared = make(map[index.SeriesID]struct{})
	e.walMu.Unlock()
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

// UnknownWALSeries counts replayed samples skipped because their series ID
// could not be resolved to labels (WAL series record and catalog both
// missing). Zero in normal operation.
func (e *Engine) UnknownWALSeries() int {
	return e.walUnknownSeries
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
