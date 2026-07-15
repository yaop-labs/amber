package otlpv4

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/histogram"
	memodel "github.com/yaop-labs/amber/internal/metricsengine/model"
	mestore "github.com/yaop-labs/amber/internal/metricsengine/store"
	metricwal "github.com/yaop-labs/amber/internal/metricsengine/wal"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// ReplayLegacyV3 reads a cleanly closed v3 database without modifying it and
// emits deterministic normalized envelopes. The signal order is logs, traces,
// then metrics; v3 did not retain a cross-signal ingest order.
func ReplayLegacyV3(ctx context.Context, dataRoot string, fn func(Envelope) error) error {
	if fn == nil {
		return errors.New("otlpv4: legacy replay callback is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := replayLegacyEventStore(ctx, filepath.Join(dataRoot, "logs"), "logs", fn); err != nil {
		return err
	}
	if err := replayLegacyEventStore(ctx, filepath.Join(dataRoot, "spans"), "traces", fn); err != nil {
		return err
	}
	if err := replayLegacyMetrics(ctx, filepath.Join(dataRoot, "metrics"), fn); err != nil {
		return err
	}
	return nil
}

func replayLegacyEventStore(ctx context.Context, dir, signal string, fn func(Envelope) error) error {
	metaPath := filepath.Join(dir, "meta.json")
	payload, err := readRegular(metaPath)
	if errors.Is(err, os.ErrNotExist) {
		return rejectUnownedLegacySegments(dir, signal)
	}
	if err != nil {
		return fmt.Errorf("otlpv4: read v3 %s metadata: %w", signal, err)
	}
	var meta storage.StoreMeta
	if err := decodeStrictJSON(payload, &meta); err != nil {
		return fmt.Errorf("otlpv4: parse v3 %s metadata: %w", signal, err)
	}
	segments := append([]storage.SegmentMeta(nil), meta.Segments...)
	sort.Slice(segments, func(i, j int) bool { return segments[i].ID < segments[j].ID })
	seen := make(map[uint32]struct{}, len(segments))
	for _, segment := range segments {
		if err := ctx.Err(); err != nil {
			return err
		}
		parsedID, ok := storage.ParseSegmentID(segment.FileName)
		if !ok || parsedID != segment.ID {
			return fmt.Errorf("otlpv4: v3 %s metadata has invalid segment %q", signal, segment.FileName)
		}
		if _, duplicate := seen[segment.ID]; duplicate {
			return fmt.Errorf("otlpv4: v3 %s metadata has duplicate segment %d", signal, segment.ID)
		}
		seen[segment.ID] = struct{}{}
		if segment.DeletePending {
			continue
		}
		if !segment.Sealed {
			return fmt.Errorf("otlpv4: v3 %s segment %s is not sealed", signal, segment.FileName)
		}
		if !segment.HasLocalCopy() {
			return fmt.Errorf("otlpv4: v3 %s segment %s is remote-only", signal, segment.FileName)
		}
		path := filepath.Join(dir, segment.FileName)
		if err := replayLegacySegment(ctx, path, signal, fn); err != nil {
			return err
		}
	}
	return nil
}

func replayLegacySegment(ctx context.Context, path, signal string, fn func(Envelope) error) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("otlpv4: inspect v3 %s segment %s: %w", signal, filepath.Base(path), err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("otlpv4: v3 %s segment %s is not regular", signal, filepath.Base(path))
	}
	reader, err := storage.OpenSegmentReader(path, nil)
	if err != nil {
		return fmt.Errorf("otlpv4: open v3 %s segment %s: %w", signal, filepath.Base(path), err)
	}
	scanErr := reader.Scan(func(record []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var envelope Envelope
		var err error
		switch signal {
		case "logs":
			var entry model.LogEntry
			if err := entry.DecodeBytes(record); err != nil {
				return fmt.Errorf("decode v3 log: %w", err)
			}
			envelope, err = NormalizedLogV3(entry)
		case "traces":
			var entry model.SpanEntry
			if err := entry.DecodeBytes(record); err != nil {
				return fmt.Errorf("decode v3 span: %w", err)
			}
			envelope, err = NormalizedSpanV3(entry)
		default:
			return fmt.Errorf("unsupported legacy signal %q", signal)
		}
		if err != nil {
			return err
		}
		return fn(envelope)
	})
	closeErr := reader.Close()
	if scanErr != nil {
		return fmt.Errorf("otlpv4: replay v3 %s segment %s: %w", signal, filepath.Base(path), scanErr)
	}
	if closeErr != nil {
		return fmt.Errorf("otlpv4: close v3 %s segment %s: %w", signal, filepath.Base(path), closeErr)
	}
	return nil
}

func rejectUnownedLegacySegments(dir, signal string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if _, ok := storage.ParseSegmentID(entry.Name()); ok {
			return fmt.Errorf("otlpv4: v3 %s has segments but no metadata", signal)
		}
	}
	return nil
}

func replayLegacyMetrics(ctx context.Context, dir string, fn func(Envelope) error) error {
	hasWALRecords, err := metricwal.HasRecords(filepath.Join(dir, "head.wal"))
	if err != nil {
		return fmt.Errorf("otlpv4: inspect v3 metrics WAL: %w", err)
	}
	if hasWALRecords {
		return errors.New("otlpv4: v3 metrics WAL is not empty; source is not cleanly closed")
	}
	markers, err := filepath.Glob(filepath.Join(dir, "*.flush-pending"))
	if err != nil {
		return fmt.Errorf("otlpv4: inspect v3 metrics flush markers: %w", err)
	}
	if len(markers) != 0 {
		return errors.New("otlpv4: v3 metrics has an incomplete flush transition")
	}
	manifestPath := filepath.Join(dir, "manifest.json")
	payload, err := readRegular(manifestPath)
	if errors.Is(err, os.ErrNotExist) {
		return rejectUnownedMetricBlocks(dir)
	}
	if err != nil {
		return fmt.Errorf("otlpv4: read v3 metrics manifest: %w", err)
	}
	var manifest mestore.Manifest
	if err := decodeStrictJSON(payload, &manifest); err != nil {
		return fmt.Errorf("otlpv4: parse v3 metrics manifest: %w", err)
	}
	if manifest.Version < 0 || manifest.Version > 1 {
		return fmt.Errorf("otlpv4: unsupported v3 metrics manifest version %d", manifest.Version)
	}
	blocks := append([]mestore.BlockMeta(nil), manifest.Blocks...)
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].MinTime != blocks[j].MinTime {
			return blocks[i].MinTime < blocks[j].MinTime
		}
		return blocks[i].Path < blocks[j].Path
	})
	seen := make(map[string]struct{}, len(blocks))
	for _, meta := range blocks {
		if err := ctx.Err(); err != nil {
			return err
		}
		if filepath.Base(meta.Path) != meta.Path || strings.Contains(meta.Path, `\`) {
			return fmt.Errorf("otlpv4: invalid v3 metrics block path %q", meta.Path)
		}
		if _, duplicate := seen[meta.Path]; duplicate {
			return fmt.Errorf("otlpv4: duplicate v3 metrics block %q", meta.Path)
		}
		seen[meta.Path] = struct{}{}
		path := filepath.Join(dir, meta.Path)
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("otlpv4: inspect v3 metrics block %s: %w", meta.Path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("otlpv4: v3 metrics block %s is not regular", meta.Path)
		}
		switch meta.Kind {
		case "":
			if !strings.HasSuffix(meta.Path, ".meb") {
				return fmt.Errorf("otlpv4: scalar block has invalid extension %q", meta.Path)
			}
			if err := replayLegacyScalarBlock(ctx, path, fn); err != nil {
				return err
			}
		case mestore.BlockKindHistogram:
			if !strings.HasSuffix(meta.Path, ".mhb") {
				return fmt.Errorf("otlpv4: histogram block has invalid extension %q", meta.Path)
			}
			if err := replayLegacyHistogramBlock(ctx, path, fn); err != nil {
				return err
			}
		default:
			return fmt.Errorf("otlpv4: unsupported v3 metrics block kind %q", meta.Kind)
		}
	}
	return nil
}

func replayLegacyScalarBlock(ctx context.Context, path string, fn func(Envelope) error) error {
	directory, err := block.ReadDirectory(path)
	if err != nil {
		return fmt.Errorf("otlpv4: read v3 scalar block %s: %w", filepath.Base(path), err)
	}
	err = block.ScanFileFilteredWithDirectoryShared(path, directory, nil, func(series block.DecodedSeries) error {
		for i, timestamp := range series.Timestamps {
			if err := ctx.Err(); err != nil {
				return err
			}
			envelope, err := NormalizedMetricSampleV3(memodel.Sample{
				Labels: series.Entry.Labels, Type: series.Entry.Type, Timestamp: timestamp, Value: series.Values[i],
			})
			if err != nil {
				return err
			}
			if err := fn(envelope); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("otlpv4: replay v3 scalar block %s: %w", filepath.Base(path), err)
	}
	return nil
}

func replayLegacyHistogramBlock(ctx context.Context, path string, fn func(Envelope) error) error {
	exponential, explicit, err := histogram.ReadBlock(path)
	if err != nil {
		return fmt.Errorf("otlpv4: read v3 histogram block %s: %w", filepath.Base(path), err)
	}
	for _, series := range exponential {
		for i, timestamp := range series.Timestamps {
			if err := emitLegacySketch(ctx, fn, engine.SketchSample{
				Labels: series.Entry.Labels, Timestamp: timestamp, Exp: series.Sketches[i],
			}); err != nil {
				return fmt.Errorf("otlpv4: replay v3 histogram block %s: %w", filepath.Base(path), err)
			}
		}
	}
	for _, series := range explicit {
		for i, timestamp := range series.Timestamps {
			if err := emitLegacySketch(ctx, fn, engine.SketchSample{
				Labels: series.Entry.Labels, Timestamp: timestamp, Explicit: series.Buckets[i],
			}); err != nil {
				return fmt.Errorf("otlpv4: replay v3 histogram block %s: %w", filepath.Base(path), err)
			}
		}
	}
	return nil
}

func emitLegacySketch(ctx context.Context, fn func(Envelope) error, sample engine.SketchSample) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	envelope, err := NormalizedMetricSketchV3(sample)
	if err != nil {
		return err
	}
	return fn(envelope)
}

func rejectUnownedMetricBlocks(dir string) error {
	for _, pattern := range []string{"*.meb", "*.mhb"} {
		matches, err := filepath.Glob(filepath.Join(dir, pattern))
		if err != nil {
			return err
		}
		if len(matches) != 0 {
			return errors.New("otlpv4: v3 metrics has blocks but no manifest")
		}
	}
	return nil
}

func readRegular(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("not a regular file")
	}
	return os.ReadFile(path) //nolint:gosec
}

func decodeStrictJSON(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("trailing data")
	}
	return nil
}
