package index

import (
	"bytes"
	"context"
	"log/slog"

	"github.com/hnlbs/amber/internal/model"
	"github.com/hnlbs/amber/internal/storage"
)

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
