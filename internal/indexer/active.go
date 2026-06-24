// Package indexer owns the in-memory bitmap indexes for currently-open
// (unsealed) segments. This is writer-side state - query.Executor reads from
// it via Lookup* but does not mutate it. Under distributed mode (Phase 1.5)
// the indexer lives on the ingest node, while Executor lives on the query
// node and reads sealed indexes from S3.
package indexer

import (
	"encoding/hex"
	"sync"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// kindSlot pairs an in-memory bitmap with the segment filename it indexes;
// when the manager rotates to a new active segment, the slot's name diverges
// from the manager's and the bitmap is replaced lazily on next ensure().
type kindSlot struct {
	bitmap *index.MultiFieldIndex
	name   string
}

// ActiveIndex holds the in-memory bitmap index of the current active log and
// span segments. It rotates its bitmap lazily when the manager seals a segment
// and opens a new one. It is safe for concurrent use.
type ActiveIndex struct {
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager

	mu   sync.RWMutex
	log  kindSlot
	span kindSlot
}

// New returns an ActiveIndex over the given log and span segment managers.
func New(logManager, spanManager *storage.SegmentManager) *ActiveIndex {
	return &ActiveIndex{
		logManager:  logManager,
		spanManager: spanManager,
	}
}

// lookup returns the bitmap if it indexes the given segment name. Read-side
// fall-through for queries hitting an unsealed segment.
func (a *ActiveIndex) lookup(slot *kindSlot, name string) (*index.MultiFieldIndex, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if slot.name == name && slot.bitmap != nil {
		return slot.bitmap, true
	}
	return nil, false
}

// ensure returns the slot's bitmap, lazily rotating to a fresh one when the
// manager has moved to a new active segment.
func (a *ActiveIndex) ensure(mgr *storage.SegmentManager, slot *kindSlot) *index.MultiFieldIndex {
	activeMeta, ok := mgr.ActiveSegmentMeta()
	if !ok {
		return nil
	}
	a.mu.RLock()
	if slot.name == activeMeta.FileName && slot.bitmap != nil {
		idx := slot.bitmap
		a.mu.RUnlock()
		return idx
	}
	a.mu.RUnlock()

	a.mu.Lock()
	defer a.mu.Unlock()
	if slot.name == activeMeta.FileName && slot.bitmap != nil {
		return slot.bitmap
	}
	fresh := index.NewMultiFieldIndex()
	slot.bitmap = fresh
	slot.name = activeMeta.FileName
	return fresh
}

// LookupLog returns the active log bitmap if it indexes segment name, letting
// queries against the unsealed segment use the index instead of a scan.
func (a *ActiveIndex) LookupLog(name string) (*index.MultiFieldIndex, bool) {
	return a.lookup(&a.log, name)
}

// LookupSpan is LookupLog for the active span segment.
func (a *ActiveIndex) LookupSpan(name string) (*index.MultiFieldIndex, bool) {
	return a.lookup(&a.span, name)
}

func (a *ActiveIndex) activeLog() *index.MultiFieldIndex {
	return a.ensure(a.logManager, &a.log)
}

func (a *ActiveIndex) activeSpan() *index.MultiFieldIndex {
	return a.ensure(a.spanManager, &a.span)
}

// IndexLogEntry adds one log entry's indexable fields to the active log bitmap.
func (a *ActiveIndex) IndexLogEntry(entry model.LogEntry) {
	idx := a.activeLog()
	if idx == nil {
		return
	}
	entryID := model.EntryIDToUint64(entry.ID)
	idx.Add("level", entry.Level.String(), entryID)
	if entry.Service != "" {
		idx.Add("service", entry.Service, entryID)
	}
	if entry.Host != "" {
		idx.Add("host", entry.Host, entryID)
	}
	if !model.IsZeroTraceID(entry.TraceID) {
		var traceHex [32]byte
		hex.Encode(traceHex[:], entry.TraceID[:])
		idx.Add("trace_id", string(traceHex[:]), entryID)
	}
}

// IndexSpanEntry adds one span's indexable fields to the active span bitmap.
func (a *ActiveIndex) IndexSpanEntry(span model.SpanEntry) {
	idx := a.activeSpan()
	if idx == nil {
		return
	}
	entryID := model.EntryIDToUint64(span.ID)
	if span.Service != "" {
		idx.Add("service", span.Service, entryID)
	}
	if span.Operation != "" {
		idx.Add("operation", span.Operation, entryID)
	}
	idx.Add("status", span.Status.String(), entryID)
	idx.Add(index.DurationBucketField, index.DurationBucket(span.Duration()), entryID)
	if !model.IsZeroTraceID(span.TraceID) {
		var traceHex [32]byte
		hex.Encode(traceHex[:], span.TraceID[:])
		idx.Add("trace_id", string(traceHex[:]), entryID)
	}
}

// IndexLogEntries adds a batch of log entries to the active log bitmap in one
// pass, the path the batcher uses after a write.
func (a *ActiveIndex) IndexLogEntries(entries []*model.LogEntry) {
	if len(entries) == 0 {
		return
	}
	idx := a.activeLog()
	if idx == nil {
		return
	}

	levelGroups := make(map[string][]uint64, 4)
	serviceGroups := make(map[string][]uint64, 4)
	hostGroups := make(map[string][]uint64, 8)
	traceGroups := make(map[string][]uint64)

	var traceHexCache map[model.TraceID]string

	for _, entry := range entries {
		entryID := model.EntryIDToUint64(entry.ID)

		levelGroups[entry.Level.String()] = append(levelGroups[entry.Level.String()], entryID)
		if entry.Service != "" {
			serviceGroups[entry.Service] = append(serviceGroups[entry.Service], entryID)
		}
		if entry.Host != "" {
			hostGroups[entry.Host] = append(hostGroups[entry.Host], entryID)
		}
		if !model.IsZeroTraceID(entry.TraceID) {
			if traceHexCache == nil {
				traceHexCache = make(map[model.TraceID]string)
			}
			th, ok := traceHexCache[entry.TraceID]
			if !ok {
				var buf [32]byte
				hex.Encode(buf[:], entry.TraceID[:])
				th = string(buf[:])
				traceHexCache[entry.TraceID] = th
			}
			traceGroups[th] = append(traceGroups[th], entryID)
		}
	}

	flush := func(field string, groups map[string][]uint64) {
		if len(groups) == 0 {
			return
		}
		bi := idx.GetOrCreate(field)
		for value, ids := range groups {
			bi.AddMany(value, ids)
		}
	}
	flush("level", levelGroups)
	flush("service", serviceGroups)
	flush("host", hostGroups)
	flush("trace_id", traceGroups)
}

// IndexSpanEntries adds a batch of spans to the active span bitmap in one pass.
func (a *ActiveIndex) IndexSpanEntries(spans []*model.SpanEntry) {
	if len(spans) == 0 {
		return
	}
	idx := a.activeSpan()
	if idx == nil {
		return
	}

	serviceGroups := make(map[string][]uint64, 4)
	operationGroups := make(map[string][]uint64, 8)
	statusGroups := make(map[string][]uint64, 4)
	durGroups := make(map[string][]uint64, 16)
	traceGroups := make(map[string][]uint64)
	var traceHexCache map[model.TraceID]string

	for _, span := range spans {
		entryID := model.EntryIDToUint64(span.ID)

		if span.Service != "" {
			serviceGroups[span.Service] = append(serviceGroups[span.Service], entryID)
		}
		if span.Operation != "" {
			operationGroups[span.Operation] = append(operationGroups[span.Operation], entryID)
		}
		statusGroups[span.Status.String()] = append(statusGroups[span.Status.String()], entryID)
		durBucket := index.DurationBucket(span.Duration())
		durGroups[durBucket] = append(durGroups[durBucket], entryID)
		if !model.IsZeroTraceID(span.TraceID) {
			if traceHexCache == nil {
				traceHexCache = make(map[model.TraceID]string)
			}
			th, ok := traceHexCache[span.TraceID]
			if !ok {
				var buf [32]byte
				hex.Encode(buf[:], span.TraceID[:])
				th = string(buf[:])
				traceHexCache[span.TraceID] = th
			}
			traceGroups[th] = append(traceGroups[th], entryID)
		}
	}

	if len(serviceGroups) > 0 {
		bi := idx.GetOrCreate("service")
		for value, ids := range serviceGroups {
			bi.AddMany(value, ids)
		}
	}
	if len(operationGroups) > 0 {
		bi := idx.GetOrCreate("operation")
		for value, ids := range operationGroups {
			bi.AddMany(value, ids)
		}
	}
	if len(statusGroups) > 0 {
		bi := idx.GetOrCreate("status")
		for value, ids := range statusGroups {
			bi.AddMany(value, ids)
		}
	}
	if len(durGroups) > 0 {
		bi := idx.GetOrCreate(index.DurationBucketField)
		for value, ids := range durGroups {
			bi.AddMany(value, ids)
		}
	}
	if len(traceGroups) > 0 {
		bi := idx.GetOrCreate("trace_id")
		for value, ids := range traceGroups {
			bi.AddMany(value, ids)
		}
	}
}
