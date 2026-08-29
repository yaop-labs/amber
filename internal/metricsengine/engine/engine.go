// Package engine ties the metrics WAL, the in-memory head, and the flush
// protocol together. Appends are durable on return (WAL fsync precedes the head
// update); a flush snapshots the head into a block and checkpoints the WAL under
// a gate that excludes concurrent appends so no acknowledged sample is lost.
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
	sharedwal "github.com/yaop-labs/amber/internal/wal"
)

// Options configures a metrics engine. A zero WALPath runs the engine purely
// in memory with no durability.
type Options struct {
	WALPath string
	// SharedWAL uses amber's process-wide segmented WAL instead of a private
	// metrics WAL file. It is mutually exclusive with WALPath.
	SharedWAL        *sharedwal.WAL
	WALFlushInterval time.Duration
}

// Engine is the durable metrics writer: it owns the registry, the head, the
// WAL, and the flush coordination. It is safe for concurrent appends.
type Engine struct {
	mu        sync.Mutex
	registry  *index.Registry
	head      *head.Head
	wal       *wal.WAL
	walPath   string
	sharedWAL *sharedwal.WAL
	committer *committer

	// flushGate excludes flushes from in-flight appends. An append holds the
	// read side from WAL enqueue through the head update; flush holds the
	// write side from snapshot through WAL truncate. Without it a sample
	// could be acked (WAL fsynced), miss the flush snapshot, have its WAL
	// record truncated, and survive only in the head - i.e. an acked sample
	// lost on the next crash.
	flushGate sync.RWMutex
	// gateHeld tracks the Prepare->Commit/Abort pairing so releasing is
	// idempotent and a Commit without a Prepare cannot unlock an unheld gate.
	gateHeld bool
	gateMu   sync.Mutex

	// walMu orders WAL enqueues so a series record always precedes the first
	// sample referencing it, across goroutines. Fsync waits happen outside.
	walMu sync.Mutex
	// declared tracks series already written as KindSeries records in the
	// current WAL generation (since the last truncate).
	declared map[index.SeriesID]struct{}

	// sketchHead buffers histogram ticks, the sketch analogue of head.Head.
	// Guarded by mu.
	sketchHead map[index.SeriesID]*sketchBuf

	// walRecovery captures what RecoverReplay found at open time. A non-zero
	// TruncatedBytes means a corrupt or torn WAL tail was dropped.
	walRecovery wal.RecoverStats
	// walUnknownSeries counts replayed sample records whose series ID was
	// resolvable neither from the WAL's series records nor the registry
	// (possible only for WALs from the pre-binary format era or after
	// catalog loss); those samples are skipped.
	walUnknownSeries int
}

// New returns an in-memory engine (no WAL). It panics only on an error that
// cannot occur without a WAL path.
func New() *Engine {
	e, err := Open(Options{})
	if err != nil {
		panic(err)
	}
	return e
}

// Open creates an engine with a fresh registry, recovering opts.WALPath if set.
func Open(opts Options) (*Engine, error) {
	return OpenWithRegistry(index.NewRegistry(), opts)
}

// OpenWithRegistry is Open using a caller-supplied registry (shared with the
// store catalog). When opts.WALPath is set it replays and repairs the WAL,
// rebuilding the head before returning.
func OpenWithRegistry(registry *index.Registry, opts Options) (*Engine, error) {
	if registry == nil {
		registry = index.NewRegistry()
	}
	e := &Engine{
		registry:   registry,
		head:       head.New(registry),
		declared:   make(map[index.SeriesID]struct{}),
		sketchHead: make(map[index.SeriesID]*sketchBuf),
	}
	if opts.SharedWAL != nil && opts.WALPath != "" {
		return nil, errors.New("engine: SharedWAL and WALPath are mutually exclusive")
	}
	if opts.SharedWAL != nil {
		stats, err := opts.SharedWAL.Replay(sharedwal.StreamMetrics, func(_ uint64, payload []byte) error {
			rec, err := decodeRecord(payload)
			if err != nil {
				return err
			}
			return e.replayRecord(rec)
		})
		if err != nil {
			return nil, err
		}
		e.walRecovery = wal.RecoverStats{Records: stats.Records, TruncatedBytes: stats.TruncatedBytes, CorruptRecords: stats.CorruptRecords}
		e.sharedWAL = opts.SharedWAL
		e.committer = newCommitter(func(records []record) error {
			payloads := make([][]byte, len(records))
			for i, rec := range records {
				payload, err := encodeRecord(rec)
				if err != nil {
					return err
				}
				payloads[i] = payload
			}
			_, err := e.sharedWAL.AppendBatchUnsynced(sharedwal.StreamMetrics, payloads)
			return err
		}, e.sharedWAL.Sync, opts.WALFlushInterval)
	} else if opts.WALPath != "" {
		stats, err := wal.RecoverReplay(opts.WALPath, func(legacy wal.Record) error {
			return e.replayRecord(fromLegacyRecord(legacy))
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
		e.walPath = opts.WALPath
		e.committer = newCommitter(func(records []record) error {
			legacy := make([]wal.Record, 0, len(records))
			for _, rec := range records {
				legacy = append(legacy, legacyRecord(rec))
			}
			return w.AppendBatchUnsynced(legacy)
		}, w.Sync, opts.WALFlushInterval)
	}
	return e, nil
}

func fromLegacyRecord(r wal.Record) record {
	kind := kindSample
	switch r.Kind {
	case wal.KindSeries:
		kind = kindSeries
	case wal.KindLegacySample:
		kind = kindLegacySample
	case wal.KindSketchExp:
		kind = kindSketchExp
	case wal.KindSketchExplicit:
		kind = kindSketchExplicit
	}
	return record{kind: kind, id: r.ID, labels: r.Labels, typ: r.Type, timestamp: r.Timestamp, value: r.Value, payload: r.Payload}
}

func legacyRecord(r record) wal.Record {
	kind := wal.KindSample
	switch r.kind {
	case kindSeries:
		kind = wal.KindSeries
	case kindSample:
		kind = wal.KindSample
	case kindSketchExp:
		kind = wal.KindSketchExp
	case kindSketchExplicit:
		kind = wal.KindSketchExplicit
	}
	return wal.Record{Kind: kind, ID: r.id, Labels: r.labels, Type: r.typ, Timestamp: r.timestamp, Value: r.value, Payload: r.payload}
}

// replayRecord applies one WAL record during open.
func (e *Engine) replayRecord(record record) error {
	switch record.kind {
	case kindSeries:
		id := index.SeriesID(record.id)
		e.registry.Import(id, record.labels)
		// The series record is still in the WAL after replay (truncate only
		// happens on flush), so it stays declared for this generation.
		e.declared[id] = struct{}{}
		return nil
	case kindSample:
		id := index.SeriesID(record.id)
		labels, ok := e.registry.Labels(id)
		if !ok {
			e.walUnknownSeries++
			return nil
		}
		e.head.AppendWithID(id, labels, record.typ, record.timestamp, record.value)
		return nil
	case kindLegacySample:
		e.head.Append(record.labels, record.typ, record.timestamp, record.value)
		return nil
	case kindSketchExp, kindSketchExplicit:
		return e.replaySketchRecord(record)
	default:
		return fmt.Errorf("engine: unknown WAL record kind %d", record.kind)
	}
}

// Append is AppendBatch for a single sample.
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
	records := make([]record, 0, len(samples))
	for i, sample := range samples {
		labels := sample.Labels.Canonical()
		id := e.registry.GetOrCreateAt(labels, sample.Timestamp)
		ids[i] = id
		canonical[i] = labels
		if _, ok := e.declared[id]; !ok {
			e.declared[id] = struct{}{}
			records = append(records, record{kind: kindSeries, id: uint64(id), labels: labels})
		}
		records = append(records, record{
			kind:      kindSample,
			id:        uint64(id),
			typ:       sample.Type,
			timestamp: sample.Timestamp,
			value:     sample.Value,
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
// value model. Values smaller than 1/scale collapse to 0; NaN, +/-Inf, and
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

// CommitFlush resets the head and checkpoints the WAL, then releases the gate
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
	e.sketchHead = make(map[index.SeriesID]*sketchBuf)
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

// LastWALSeq returns the highest sequence written by this metrics stream.
// For the shared WAL this is used to advance the metrics checkpoint only
// after the corresponding block and manifest have been made durable.
func (e *Engine) LastWALSeq() uint64 {
	if e.sharedWAL == nil {
		return 0
	}
	return e.sharedWAL.LastStreamSeq(sharedwal.StreamMetrics)
}

// CheckpointWAL advances the shared metrics WAL checkpoint. Legacy WALs are
// already truncated by CommitFlush and need no second checkpoint operation.
func (e *Engine) CheckpointWAL() error {
	if e.sharedWAL == nil {
		return nil
	}
	return e.sharedWAL.Checkpoint(sharedwal.StreamMetrics, e.LastWALSeq())
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

// SnapshotMatching copies only head series whose labels satisfy match
// (nil = all), so selective queries skip copying the rest of the head.
func (e *Engine) SnapshotMatching(match func(model.LabelSet) bool) []block.Series {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.head.SnapshotMatching(match)
}

func (e *Engine) Registry() *index.Registry {
	return e.registry
}

// WALRecoveryStats reports what the open-time WAL replay found. A non-zero
// TruncatedBytes means a corrupt or torn tail was dropped - worth surfacing
// as a metric or log line by the embedding layer.
func (e *Engine) WALRecoveryStats() wal.RecoverStats {
	return e.walRecovery
}

// UnknownWALSeries counts replayed samples skipped because their series ID
// could not be resolved to labels (WAL series record and catalog both
// missing). Zero in normal operation.

// HasWALRecords reports whether the engine's WAL stream currently contains records.
func (e *Engine) HasWALRecords() (bool, error) {
	if e.sharedWAL != nil {
		return e.sharedWAL.HasStreamRecords(sharedwal.StreamMetrics)
	}
	if e.wal != nil {
		return wal.HasRecords(e.walPath)
	}
	return false, nil
}

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
