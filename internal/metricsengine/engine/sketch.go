package engine

// Histogram (sketch) ingest: sketches share the engine's WAL, registry, flush
// gate, and head lifecycle with scalar samples - one durability protocol for
// every signal shape.

import (
	"errors"
	"sort"

	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

// SketchSample is one histogram tick to ingest: exactly one of Exp/Explicit
// is set.
type SketchSample struct {
	Labels    model.LabelSet
	Timestamp int64
	Exp       *histogram.ExponentialHistogram
	Explicit  *histogram.ExplicitBucketHistogram
}

// sketchBuf buffers one series' ticks in the head.
type sketchBuf struct {
	labels     model.LabelSet
	timestamps []int64
	exp        []*histogram.ExponentialHistogram
	explicit   []*histogram.ExplicitBucketHistogram
}

// AppendSketches appends histogram ticks to the WAL and the in-memory sketch
// head, with the same ack-after-fsync contract as AppendBatch.
func (e *Engine) AppendSketches(samples []SketchSample) ([]index.SeriesID, error) {
	if len(samples) == 0 {
		return nil, nil
	}
	for _, s := range samples {
		if (s.Exp == nil) == (s.Explicit == nil) {
			return nil, errors.New("engine: sketch sample must set exactly one of Exp/Explicit")
		}
	}

	if e.committer == nil {
		e.mu.Lock()
		defer e.mu.Unlock()
		ids := make([]index.SeriesID, len(samples))
		for i, s := range samples {
			labels := s.Labels.Canonical()
			ids[i] = e.registry.GetOrCreateAt(labels, s.Timestamp)
			e.appendSketchLocked(ids[i], labels, s)
		}
		return ids, nil
	}

	e.flushGate.RLock()
	defer e.flushGate.RUnlock()

	ids := make([]index.SeriesID, len(samples))
	canonical := make([]model.LabelSet, len(samples))

	e.walMu.Lock()
	records := make([]wal.Record, 0, len(samples))
	for i, s := range samples {
		labels := s.Labels.Canonical()
		id := e.registry.GetOrCreateAt(labels, s.Timestamp)
		ids[i] = id
		canonical[i] = labels
		if _, ok := e.declared[id]; !ok {
			e.declared[id] = struct{}{}
			records = append(records, wal.Record{Kind: wal.KindSeries, ID: uint64(id), Labels: labels})
		}
		rec := wal.Record{ID: uint64(id), Timestamp: s.Timestamp}
		if s.Exp != nil {
			rec.Kind = wal.KindSketchExp
			rec.Payload = histogram.AppendSketch(nil, s.Exp)
		} else {
			rec.Kind = wal.KindSketchExplicit
			rec.Payload = histogram.EncodeExplicitTick(s.Explicit)
		}
		records = append(records, rec)
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
	for i, s := range samples {
		e.appendSketchLocked(ids[i], canonical[i], s)
	}
	return ids, nil
}

// appendSketchLocked adds one tick to the sketch head. Caller holds e.mu.
func (e *Engine) appendSketchLocked(id index.SeriesID, labels model.LabelSet, s SketchSample) {
	buf := e.sketchHead[id]
	if buf == nil {
		buf = &sketchBuf{labels: labels}
		e.sketchHead[id] = buf
	}
	buf.timestamps = append(buf.timestamps, s.Timestamp)
	buf.exp = append(buf.exp, s.Exp)
	buf.explicit = append(buf.explicit, s.Explicit)
}

// replaySketchRecord applies one sketch WAL record during open.
func (e *Engine) replaySketchRecord(record wal.Record) error {
	id := index.SeriesID(record.ID)
	labels, ok := e.registry.Labels(id)
	if !ok {
		e.walUnknownSeries++
		return nil
	}
	s := SketchSample{Labels: labels, Timestamp: record.Timestamp}
	switch record.Kind {
	case wal.KindSketchExp:
		sk, _, err := histogram.DecodeSketch(record.Payload)
		if err != nil {
			return err
		}
		s.Exp = sk
	case wal.KindSketchExplicit:
		h, err := histogram.DecodeExplicitTick(record.Payload)
		if err != nil {
			return err
		}
		s.Explicit = h
	}
	e.appendSketchLocked(id, labels, s)
	return nil
}

// SketchSnapshot returns the buffered sketch series, ticks sorted by time,
// split by payload kind. IDs are the global registry IDs.
func (e *Engine) SketchSnapshot() ([]histogram.ExpSeries, []histogram.ExplicitSeries) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.sketchSnapshotLocked()
}

func (e *Engine) sketchSnapshotLocked() ([]histogram.ExpSeries, []histogram.ExplicitSeries) {
	ids := make([]int, 0, len(e.sketchHead))
	for id := range e.sketchHead {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)

	var exp []histogram.ExpSeries
	var explicit []histogram.ExplicitSeries
	for _, rawID := range ids {
		id := index.SeriesID(rawID)
		buf := e.sketchHead[id]
		es := histogram.ExpSeries{ID: uint64(id), Labels: append(model.LabelSet(nil), buf.labels...)}
		xs := histogram.ExplicitSeries{ID: uint64(id), Labels: es.Labels}
		for i, ts := range buf.timestamps {
			switch {
			case buf.exp[i] != nil:
				es.Timestamps = append(es.Timestamps, ts)
				es.Sketches = append(es.Sketches, buf.exp[i])
			case buf.explicit[i] != nil:
				xs.Timestamps = append(xs.Timestamps, ts)
				xs.Buckets = append(xs.Buckets, buf.explicit[i])
			}
		}
		if len(es.Timestamps) > 0 {
			sortExpSeries(&es)
			exp = append(exp, es)
		}
		if len(xs.Timestamps) > 0 {
			sortExplicitSeries(&xs)
			explicit = append(explicit, xs)
		}
	}
	return exp, explicit
}

func sortExpSeries(s *histogram.ExpSeries) {
	if sort.SliceIsSorted(s.Timestamps, func(i, j int) bool { return s.Timestamps[i] < s.Timestamps[j] }) {
		return
	}
	sort.Sort(&expTickSorter{s})
}

func sortExplicitSeries(s *histogram.ExplicitSeries) {
	if sort.SliceIsSorted(s.Timestamps, func(i, j int) bool { return s.Timestamps[i] < s.Timestamps[j] }) {
		return
	}
	sort.Sort(&explicitTickSorter{s})
}

type expTickSorter struct{ s *histogram.ExpSeries }

func (x *expTickSorter) Len() int           { return len(x.s.Timestamps) }
func (x *expTickSorter) Less(i, j int) bool { return x.s.Timestamps[i] < x.s.Timestamps[j] }
func (x *expTickSorter) Swap(i, j int) {
	x.s.Timestamps[i], x.s.Timestamps[j] = x.s.Timestamps[j], x.s.Timestamps[i]
	x.s.Sketches[i], x.s.Sketches[j] = x.s.Sketches[j], x.s.Sketches[i]
}

type explicitTickSorter struct{ s *histogram.ExplicitSeries }

func (x *explicitTickSorter) Len() int           { return len(x.s.Timestamps) }
func (x *explicitTickSorter) Less(i, j int) bool { return x.s.Timestamps[i] < x.s.Timestamps[j] }
func (x *explicitTickSorter) Swap(i, j int) {
	x.s.Timestamps[i], x.s.Timestamps[j] = x.s.Timestamps[j], x.s.Timestamps[i]
	x.s.Buckets[i], x.s.Buckets[j] = x.s.Buckets[j], x.s.Buckets[i]
}

// BufferedSketchSeries returns the number of buffered sketch series.
func (e *Engine) BufferedSketchSeries() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.sketchHead)
}

// BufferedSketchSamples returns the number of buffered sketch ticks.
func (e *Engine) BufferedSketchSamples() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, buf := range e.sketchHead {
		n += len(buf.timestamps)
	}
	return n
}
