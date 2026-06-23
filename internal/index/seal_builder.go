package index

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// LogSealIndexes is the output of the one-pass seal build. Everything except
// the posting list is returned for executor cache registration: the indexes
// already exist in memory here, and discarding them forces the first query
// against the fresh segment to re-parse the snapshot from disk (~450 ms per
// .fidx first touch).
type LogSealIndexes struct {
	Bitmap    *MultiFieldIndex
	FTS       *FTSIndex
	Ribbon    *RibbonFilter
	FTSRibbon *RibbonFilter
}

// BuildLogSealIndexes builds every log sidecar index in a single segment
// scan. The per-index builders below each re-read and re-decode the whole
// segment; running all five per seal multiplied the decode cost by five and
// the (dominant) FTS tokenize+stem cost by two, which is what made seal
// builds fall behind sustained ingest.
func BuildLogSealIndexes(segmentPath string, log *slog.Logger) (*LogSealIndexes, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	bitmap := NewMultiFieldIndex()
	fts := NewFTSIndex()
	posting := NewPostingListBuilder(16)
	var traceKeys [][]byte
	ctx := context.Background()
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var entry model.LogEntry
		if _, err := entry.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		entryID := model.EntryIDToUint64(entry.ID)

		bitmap.Add("level", entry.Level.String(), entryID)
		if entry.Service != "" {
			bitmap.Add("service", entry.Service, entryID)
		}
		if entry.Host != "" {
			bitmap.Add("host", entry.Host, entryID)
		}
		// trace_id intentionally excluded from the bitmap: high cardinality
		// makes per-value overhead dominate. Ribbon + posting cover it.

		if entry.Body != "" {
			if err := fts.Index(ctx, entryID, entry.Body); err != nil {
				return err
			}
		}

		if !model.IsZeroTraceID(entry.TraceID) {
			k := make([]byte, 16)
			copy(k, entry.TraceID[:])
			traceKeys = append(traceKeys, k)
			posting.Add(entry.TraceID[:], entryID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if skipped > 0 && log != nil {
		log.Debug("seal_builder: skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	if err := bitmap.Save(segmentPath + ".bidx"); err != nil {
		return nil, err
	}
	// The FTS ribbon's keys come straight from the FTS build state — one key
	// per unique token, no second tokenize pass, no separate token set. Must
	// be taken before Save seals the index and drops that state.
	tokenKeys := fts.TokenKeys()
	if err := fts.Save(segmentPath + ".fidx"); err != nil {
		return nil, err
	}

	// Ribbons are best-effort prefilters: a build failure (it happens on
	// high-cardinality token sets — pre-existing, see BuildRibbonFilter)
	// degrades queries to scans for this segment but must not discard the
	// core indexes built above or trigger a full re-scan retry.
	out := &LogSealIndexes{Bitmap: bitmap, FTS: fts}
	if ribbon, err := BuildRibbonFilter(traceKeys, 8); err != nil {
		if log != nil {
			log.Warn("seal_builder: trace ribbon failed, queries fall back to scan", "path", segmentPath, "err", err)
		}
	} else if err := ribbon.Save(segmentPath + ".filt"); err != nil {
		return nil, err
	} else {
		out.Ribbon = ribbon
	}

	if ftsRibbon, err := BuildRibbonFilter(tokenKeys, 8); err != nil {
		if log != nil {
			log.Warn("seal_builder: fts ribbon failed, queries fall back to scan", "path", segmentPath, "err", err)
		}
	} else if err := ftsRibbon.Save(segmentPath + ".fts.filt"); err != nil {
		return nil, err
	} else {
		out.FTSRibbon = ftsRibbon
	}

	pl := posting.Build()
	if err := pl.Save(segmentPath + ".pidx"); err != nil {
		return nil, err
	}

	return out, nil
}

// SpanSealIndexes mirrors LogSealIndexes for span segments.
type SpanSealIndexes struct {
	Ribbon *RibbonFilter
}

// BuildSpanSealIndexes builds every span sidecar index in a single scan.
func BuildSpanSealIndexes(segmentPath string, log *slog.Logger) (*SpanSealIndexes, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	bitmap := NewMultiFieldIndex()
	posting := NewPostingListBuilder(16)
	var traceKeys [][]byte
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var span model.SpanEntry
		if _, err := span.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		entryID := model.EntryIDToUint64(span.ID)

		if span.Service != "" {
			bitmap.Add("service", span.Service, entryID)
		}
		if span.Operation != "" {
			bitmap.Add("operation", span.Operation, entryID)
		}
		bitmap.Add("status", span.Status.String(), entryID)
		bitmap.Add(DurationBucketField, DurationBucket(span.Duration()), entryID)

		if !model.IsZeroTraceID(span.TraceID) {
			k := make([]byte, 16)
			copy(k, span.TraceID[:])
			traceKeys = append(traceKeys, k)
			posting.Add(span.TraceID[:], entryID)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if skipped > 0 && log != nil {
		log.Debug("seal_builder: skipped undecodable span records", "path", segmentPath, "count", skipped)
	}

	if err := bitmap.Save(segmentPath + ".bidx"); err != nil {
		return nil, err
	}
	out := &SpanSealIndexes{}
	if ribbon, err := BuildRibbonFilter(traceKeys, 8); err != nil {
		if log != nil {
			log.Warn("seal_builder: span trace ribbon failed, queries fall back to scan", "path", segmentPath, "err", err)
		}
	} else if err := ribbon.Save(segmentPath + ".filt"); err != nil {
		return nil, err
	} else {
		out.Ribbon = ribbon
	}
	pl := posting.Build()
	if err := pl.Save(segmentPath + ".pidx"); err != nil {
		return nil, err
	}

	return out, nil
}

// BuildLogBitmapIndex builds just the log bitmap sidecar by scanning the
// segment. Prefer BuildLogSealIndexes, which builds every sidecar in one scan;
// the single-index builders exist for tests and targeted rebuilds.
func BuildLogBitmapIndex(segmentPath string, log *slog.Logger) (*MultiFieldIndex, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	idx := NewMultiFieldIndex()
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var entry model.LogEntry
		if _, err := entry.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}

		entryID := model.EntryIDToUint64(entry.ID)
		idx.Add("level", entry.Level.String(), entryID)
		if entry.Service != "" {
			idx.Add("service", entry.Service, entryID)
		}
		if entry.Host != "" {
			idx.Add("host", entry.Host, entryID)
		}
		// trace_id intentionally excluded: high cardinality (~1 unique value per
		// record) makes the bitmap per-value overhead dominate index size.
		// Ribbon filter (.filt) handles segment-level trace_id pruning;
		// executor falls back to scan for intra-segment matching.
		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	if err := idx.Save(segmentPath + ".bidx"); err != nil {
		return nil, err
	}

	return idx, nil
}

// BuildLogFTSIndex builds just the log full-text sidecar by scanning the
// segment (see BuildLogBitmapIndex on preferring the one-pass build).
func BuildLogFTSIndex(segmentPath string, log *slog.Logger) (*FTSIndex, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	idx := NewFTSIndex()
	ctx := context.Background()
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var entry model.LogEntry
		if _, err := entry.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		if entry.Body == "" {
			return nil
		}
		return idx.Index(ctx, model.EntryIDToUint64(entry.ID), entry.Body)
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: fts skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	if err := idx.Save(segmentPath + ".fidx"); err != nil {
		return nil, err
	}

	return idx, nil
}

// BuildLogRibbonFilter builds just the log service-name ribbon filter by
// scanning the segment.
func BuildLogRibbonFilter(segmentPath string, log *slog.Logger) (*RibbonFilter, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	var keys [][]byte
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var entry model.LogEntry
		if _, err := entry.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		if model.IsZeroTraceID(entry.TraceID) {
			return nil
		}
		id := entry.TraceID
		k := make([]byte, 16)
		copy(k, id[:])
		keys = append(keys, k)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: ribbon skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	f, err := BuildRibbonFilter(keys, 8)
	if err != nil {
		return nil, err
	}
	if err := f.Save(segmentPath + ".filt"); err != nil {
		return nil, err
	}
	return f, nil
}

// BuildSpanRibbonFilter builds just the span service-name ribbon filter by
// scanning the segment.
func BuildSpanRibbonFilter(segmentPath string, log *slog.Logger) (*RibbonFilter, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	var keys [][]byte
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var span model.SpanEntry
		if _, err := span.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		if model.IsZeroTraceID(span.TraceID) {
			return nil
		}
		id := span.TraceID
		k := make([]byte, 16)
		copy(k, id[:])
		keys = append(keys, k)
		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: ribbon skipped undecodable span records", "path", segmentPath, "count", skipped)
	}

	f, err := BuildRibbonFilter(keys, 8)
	if err != nil {
		return nil, err
	}
	if err := f.Save(segmentPath + ".filt"); err != nil {
		return nil, err
	}
	return f, nil
}

// BuildLogFTSRibbon builds just the log FTS-token ribbon filter (used to skip
// segments for full-text queries) by scanning the segment.
func BuildLogFTSRibbon(segmentPath string, log *slog.Logger) (*RibbonFilter, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	seen := make(map[string]struct{}, 4096)
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var entry model.LogEntry
		if _, err := entry.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		if entry.Body == "" {
			return nil
		}
		for _, tok := range TokenizeFTS(entry.Body) {
			if tok == "" {
				continue
			}
			seen[tok] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: fts ribbon skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	keys := make([][]byte, 0, len(seen))
	for tok := range seen {
		keys = append(keys, []byte(tok))
	}

	f, err := BuildRibbonFilter(keys, 8)
	if err != nil {
		return nil, err
	}
	if err := f.Save(segmentPath + ".fts.filt"); err != nil {
		return nil, err
	}
	return f, nil
}

// BuildLogPostingList builds a .pidx posting-list index for log segments.
// It maps each unique trace_id to the sorted list of record IDs in the segment
// that carry it, enabling exact intra-segment lookup and bitmap intersection.
func BuildLogPostingList(segmentPath string, log *slog.Logger) (*PostingList, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	b := NewPostingListBuilder(16)
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var entry model.LogEntry
		if _, err := entry.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		if model.IsZeroTraceID(entry.TraceID) {
			return nil
		}
		b.Add(entry.TraceID[:], model.EntryIDToUint64(entry.ID))
		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: posting list skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	pl := b.Build()
	if err := pl.Save(segmentPath + ".pidx"); err != nil {
		return nil, err
	}
	return pl, nil
}

// BuildSpanPostingList builds a .pidx posting-list index for span segments.
func BuildSpanPostingList(segmentPath string, log *slog.Logger) (*PostingList, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	b := NewPostingListBuilder(16)
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var span model.SpanEntry
		if _, err := span.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}
		if model.IsZeroTraceID(span.TraceID) {
			return nil
		}
		b.Add(span.TraceID[:], model.EntryIDToUint64(span.ID))
		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: span posting list skipped undecodable records", "path", segmentPath, "count", skipped)
	}

	pl := b.Build()
	if err := pl.Save(segmentPath + ".pidx"); err != nil {
		return nil, err
	}
	return pl, nil
}

// BuildSpanBitmapIndex builds just the span bitmap sidecar by scanning the
// segment (see BuildSpanSealIndexes for the one-pass build).
func BuildSpanBitmapIndex(segmentPath string, log *slog.Logger) (*MultiFieldIndex, error) {
	sr, err := storage.OpenSegmentReader(segmentPath, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = sr.Close() }()

	idx := NewMultiFieldIndex()
	var skipped int

	err = sr.Scan(func(data []byte) error {
		var span model.SpanEntry
		if _, err := span.ReadFrom(bytes.NewReader(data)); err != nil {
			skipped++
			return nil
		}

		entryID := model.EntryIDToUint64(span.ID)
		if span.Service != "" {
			idx.Add("service", span.Service, entryID)
		}
		if span.Operation != "" {
			idx.Add("operation", span.Operation, entryID)
		}
		idx.Add("status", span.Status.String(), entryID)
		idx.Add(DurationBucketField, DurationBucket(span.Duration()), entryID)

		// trace_id excluded for same reason as log bitmap: high cardinality
		// dominates index size. Ribbon filter handles segment pruning.

		return nil
	})
	if err != nil {
		return nil, err
	}

	if skipped > 0 && log != nil {
		log.Debug("seal_builder: skipped undecodable span records", "path", segmentPath, "count", skipped)
	}

	if err := idx.Save(segmentPath + ".bidx"); err != nil {
		return nil, err
	}

	return idx, nil
}
