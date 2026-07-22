package otlpv4

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	memodel "github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
	"google.golang.org/protobuf/proto"
)

const (
	// DirectoryName is the database-root directory containing the canonical
	// OTLP replay journal.
	DirectoryName = "otlp_v4"
	// FormatFileName identifies the journal format independently from the
	// database identity and the reusable segment container version.
	FormatFileName = "FORMAT.json"

	journalFormatVersion = 1
)

// Journal appends canonical OTLP envelopes through Amber's WAL-backed segment
// durability protocol. The segment format remains a container detail; every
// record is independently identified and verified as AOT4.
type Journal struct {
	manager   *storage.SegmentManager
	closeOnce sync.Once
	closeErr  error

	retentionMu    sync.RWMutex
	retentionStats RetentionStats
	pruneMu        sync.Mutex
}

// Stats is a cheap operational snapshot of the canonical replay journal.
type Stats struct {
	SealedSegments    int
	ActiveSegment     bool
	ActiveRecords     uint64
	TotalRecords      uint64
	SegmentBytes      int64
	WALBytes          int64
	WALCorruptRecords uint64
	Retention         RetentionStats
}

// RetentionPolicy bounds the physical canonical replay journal. Age is
// measured from the time Amber accepted a journal record, not from timestamps
// inside the telemetry payload. A zero field disables that limit.
type RetentionPolicy struct {
	MaxAge      time.Duration
	MaxBytes    int64
	MaxSegments int
}

// RetentionStats is the cumulative operational state of journal pruning.
// OldestRetainedAt is zero when no retained journal record exists.
type RetentionStats struct {
	Runs             uint64
	Failures         uint64
	DeletedSegments  uint64
	DeletedRecords   uint64
	DeletedBytes     uint64
	LastSuccessAt    time.Time
	OldestRetainedAt time.Time
}

// RetentionResult describes one completed prune attempt.
type RetentionResult struct {
	DeletedSegments  uint64
	DeletedRecords   uint64
	DeletedBytes     uint64
	OldestRetainedAt time.Time
}

type formatManifest struct {
	JournalFormatVersion int    `json:"journal_format_version"`
	EnvelopeMagic        string `json:"envelope_magic"`
	EnvelopeVersion      uint16 `json:"envelope_version"`
}

// OpenJournal opens or creates the canonical journal below dataRoot.
func OpenJournal(dataRoot string, policy storage.RotationPolicy) (*Journal, error) {
	dir := filepath.Join(dataRoot, DirectoryName)
	if err := os.MkdirAll(dir, 0o750); err != nil { //nolint:gosec
		return nil, fmt.Errorf("otlpv4: create journal directory: %w", err)
	}
	if err := ensureFormatManifest(dir); err != nil {
		return nil, err
	}
	manager, err := storage.OpenSegmentManager(dir, policy)
	if err != nil {
		return nil, fmt.Errorf("otlpv4: open journal storage: %w", err)
	}
	return &Journal{manager: manager}, nil
}

// Append durably records one envelope before the caller acknowledges it.
func (j *Journal) Append(envelope Envelope, acceptedAt time.Time) error {
	if acceptedAt.IsZero() {
		return errors.New("otlpv4: accepted time is required")
	}
	record, err := envelope.MarshalBinary()
	if err != nil {
		return err
	}
	if err := j.manager.Write(record, acceptedAt.UnixNano()); err != nil {
		return fmt.Errorf("otlpv4: append journal: %w", err)
	}
	return nil
}

// AppendRequest wraps and appends one accepted original OTLP request.
func (j *Journal) AppendRequest(signal Signal, request proto.Message, acceptedAt time.Time) error {
	envelope, err := New(signal, FidelityOTLP, request)
	if err != nil {
		return err
	}
	return j.Append(envelope, acceptedAt)
}

// AppendNormalizedLogs records native logs as one envelope per database entry.
func (j *Journal) AppendNormalizedLogs(entries []model.LogEntry) error {
	acceptedAt := time.Now().UnixNano()
	items := make([]storage.BatchItem, 0, len(entries))
	for _, entry := range entries {
		envelope, err := NormalizedLogNative(entry)
		if err != nil {
			return err
		}
		record, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		items = append(items, storage.BatchItem{Data: record, TS: acceptedAt})
	}
	if err := j.manager.WriteBatch(items); err != nil {
		return fmt.Errorf("otlpv4: append normalized logs: %w", err)
	}
	return nil
}

// AppendNormalizedSpans records native spans as one envelope per database entry.
func (j *Journal) AppendNormalizedSpans(entries []model.SpanEntry) error {
	acceptedAt := time.Now().UnixNano()
	items := make([]storage.BatchItem, 0, len(entries))
	for _, entry := range entries {
		envelope, err := NormalizedSpanNative(entry)
		if err != nil {
			return err
		}
		record, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		items = append(items, storage.BatchItem{Data: record, TS: acceptedAt})
	}
	if err := j.manager.WriteBatch(items); err != nil {
		return fmt.Errorf("otlpv4: append normalized spans: %w", err)
	}
	return nil
}

// AppendNormalizedMetricSamples records native scalar samples.
func (j *Journal) AppendNormalizedMetricSamples(samples []memodel.Sample) error {
	acceptedAt := time.Now().UnixNano()
	items := make([]storage.BatchItem, 0, len(samples))
	for _, sample := range samples {
		envelope, err := NormalizedMetricSampleNative(sample)
		if err != nil {
			return err
		}
		record, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		items = append(items, storage.BatchItem{Data: record, TS: acceptedAt})
	}
	if err := j.manager.WriteBatch(items); err != nil {
		return fmt.Errorf("otlpv4: append normalized metric samples: %w", err)
	}
	return nil
}

// AppendNormalizedMetricFloat records the original scaled-float native call.
func (j *Journal) AppendNormalizedMetricFloat(labels memodel.LabelSet, typ memodel.MetricType, timestamp int64, value float64, scale int64) error {
	stored := int64(math.Round(value * float64(scale)))
	normalizedLabels := make(memodel.LabelSet, 0, len(labels)+1)
	for _, label := range labels {
		if label.Name != scaleLabel {
			normalizedLabels = append(normalizedLabels, label)
		}
	}
	normalizedLabels = append(normalizedLabels, memodel.Label{Name: scaleLabel, Value: strconv.FormatInt(scale, 10)})
	return j.AppendNormalizedMetricSamples([]memodel.Sample{{Labels: normalizedLabels, Type: typ, Timestamp: timestamp, Value: stored}})
}

// AppendNormalizedMetricSketches records native histogram ticks.
func (j *Journal) AppendNormalizedMetricSketches(samples []engine.SketchSample) error {
	acceptedAt := time.Now().UnixNano()
	items := make([]storage.BatchItem, 0, len(samples))
	for _, sample := range samples {
		envelope, err := NormalizedMetricSketchNative(sample)
		if err != nil {
			return err
		}
		record, err := envelope.MarshalBinary()
		if err != nil {
			return err
		}
		items = append(items, storage.BatchItem{Data: record, TS: acceptedAt})
	}
	if err := j.manager.WriteBatch(items); err != nil {
		return fmt.Errorf("otlpv4: append normalized metric sketches: %w", err)
	}
	return nil
}

// Stats reports journal growth and WAL repair state without replaying records.
func (j *Journal) Stats() (Stats, error) {
	stats, err := j.manager.Stats()
	if err != nil {
		return Stats{}, fmt.Errorf("otlpv4: journal stats: %w", err)
	}
	j.retentionMu.RLock()
	retentionStats := j.retentionStats
	j.retentionMu.RUnlock()
	return Stats{
		SealedSegments:    stats.SealedSegments,
		ActiveSegment:     stats.ActiveSegment,
		ActiveRecords:     stats.ActiveRecords,
		TotalRecords:      stats.TotalRecords,
		SegmentBytes:      stats.SegmentBytes,
		WALBytes:          stats.WALBytes,
		WALCorruptRecords: stats.WALCorruptRecords,
		Retention:         retentionStats,
	}, nil
}

// Prune removes sealed journal segments selected by policy. Deletion is
// crash-retryable: the segment is first marked DeletePending in the durable
// manifest, then its files are removed, and finally the manifest entry is
// dropped. The active segment is rotated only when it must become eligible for
// an age, byte, or segment limit.
func (j *Journal) Prune(now time.Time, policy RetentionPolicy) (result RetentionResult, err error) {
	j.pruneMu.Lock()
	defer j.pruneMu.Unlock()
	defer func() { j.recordRetention(result, err) }()
	if now.IsZero() {
		return result, errors.New("otlpv4: retention time is required")
	}
	if policy.MaxAge < 0 {
		return result, errors.New("otlpv4: retention max age cannot be negative")
	}
	if policy.MaxBytes < 0 {
		return result, errors.New("otlpv4: retention max bytes cannot be negative")
	}
	if policy.MaxSegments < 0 {
		return result, errors.New("otlpv4: retention max segments cannot be negative")
	}

	if rotate, rotateErr := j.shouldRotateForRetention(now, policy); rotateErr != nil {
		return result, rotateErr
	} else if rotate {
		if rotateErr := j.manager.Rotate(); rotateErr != nil {
			return result, fmt.Errorf("otlpv4: rotate journal for retention: %w", rotateErr)
		}
	}

	candidates := selectRetentionSegments(j.manager.SegmentsForRetention(), now, policy)
	var pruneErr error
	for _, segment := range candidates {
		if deleteErr := j.deleteSegment(segment); deleteErr != nil {
			pruneErr = errors.Join(pruneErr, deleteErr)
			continue
		}
		result.DeletedSegments++
		result.DeletedRecords += segment.RecordCount
		if segment.SizeBytes > 0 {
			result.DeletedBytes += uint64(segment.SizeBytes)
		}
	}
	result.OldestRetainedAt = j.oldestRetainedAt()
	return result, pruneErr
}

func (j *Journal) shouldRotateForRetention(now time.Time, policy RetentionPolicy) (bool, error) {
	active, ok := j.manager.ActiveSegmentMeta()
	if !ok || active.RecordCount == 0 {
		return false, nil
	}
	if policy.MaxAge > 0 && active.MaxTS < now.Add(-policy.MaxAge).UnixNano() {
		return true, nil
	}
	sealed := j.manager.SegmentsForRetention()
	if policy.MaxSegments > 0 && len(sealed)+1 > policy.MaxSegments {
		return true, nil
	}
	if policy.MaxBytes > 0 {
		stats, err := j.manager.Stats()
		if err != nil {
			return false, fmt.Errorf("otlpv4: read journal stats for retention: %w", err)
		}
		if stats.SegmentBytes+stats.WALBytes > policy.MaxBytes {
			return true, nil
		}
	}
	return false, nil
}

func selectRetentionSegments(segments []storage.SegmentMeta, now time.Time, policy RetentionPolicy) []storage.SegmentMeta {
	selected := make(map[uint32]storage.SegmentMeta)
	for _, segment := range segments {
		if segment.DeletePending {
			selected[segment.ID] = segment
		}
	}
	if policy.MaxAge > 0 {
		cutoff := now.Add(-policy.MaxAge).UnixNano()
		for _, segment := range segments {
			if segment.MaxTS < cutoff {
				selected[segment.ID] = segment
			}
		}
	}

	remaining := make([]storage.SegmentMeta, 0, len(segments))
	for _, segment := range segments {
		if _, found := selected[segment.ID]; !found {
			remaining = append(remaining, segment)
		}
	}
	sort.SliceStable(remaining, func(i, k int) bool {
		if remaining[i].MaxTS == remaining[k].MaxTS {
			return remaining[i].ID < remaining[k].ID
		}
		return remaining[i].MaxTS < remaining[k].MaxTS
	})

	if policy.MaxSegments > 0 && len(remaining) > policy.MaxSegments {
		excess := len(remaining) - policy.MaxSegments
		for _, segment := range remaining[:excess] {
			selected[segment.ID] = segment
		}
		remaining = remaining[excess:]
	}
	if policy.MaxBytes > 0 {
		var total int64
		for _, segment := range remaining {
			total += segment.SizeBytes
		}
		for _, segment := range remaining {
			if total <= policy.MaxBytes {
				break
			}
			selected[segment.ID] = segment
			total -= segment.SizeBytes
		}
	}

	out := make([]storage.SegmentMeta, 0, len(selected))
	for _, segment := range segments {
		if candidate, found := selected[segment.ID]; found {
			out = append(out, candidate)
		}
	}
	sort.SliceStable(out, func(i, k int) bool { return out[i].ID < out[k].ID })
	return out
}

func (j *Journal) deleteSegment(segment storage.SegmentMeta) error {
	if err := j.manager.BeginDeleteSegment(segment.ID); err != nil {
		return fmt.Errorf("otlpv4: mark journal segment %d for deletion: %w", segment.ID, err)
	}
	if err := j.manager.DeleteSegmentFiles(segment); err != nil {
		return fmt.Errorf("otlpv4: delete journal segment %d files: %w", segment.ID, err)
	}
	if err := j.manager.RemoveSegment(segment.ID); err != nil {
		return fmt.Errorf("otlpv4: remove journal segment %d metadata: %w", segment.ID, err)
	}
	return nil
}

func (j *Journal) oldestRetainedAt() time.Time {
	var (
		oldest int64
		have   bool
	)
	for _, segment := range j.manager.SegmentsForRetention() {
		if segment.DeletePending || segment.RecordCount == 0 {
			continue
		}
		if !have || segment.MinTS < oldest {
			oldest = segment.MinTS
			have = true
		}
	}
	if active, ok := j.manager.ActiveSegmentMeta(); ok && active.RecordCount > 0 && (!have || active.MinTS < oldest) {
		oldest = active.MinTS
		have = true
	}
	if !have {
		return time.Time{}
	}
	return time.Unix(0, oldest).UTC()
}

func (j *Journal) recordRetention(result RetentionResult, err error) {
	j.retentionMu.Lock()
	defer j.retentionMu.Unlock()
	j.retentionStats.Runs++
	j.retentionStats.DeletedSegments += result.DeletedSegments
	j.retentionStats.DeletedRecords += result.DeletedRecords
	j.retentionStats.DeletedBytes += result.DeletedBytes
	if err != nil {
		j.retentionStats.Failures++
		return
	}
	j.retentionStats.OldestRetainedAt = result.OldestRetainedAt
	j.retentionStats.LastSuccessAt = time.Now().UTC()
}

// Close seals the current journal segment and closes its WAL.
func (j *Journal) Close() error {
	j.closeOnce.Do(func() {
		j.closeErr = j.manager.Close()
	})
	return j.closeErr
}

// Replay scans a closed journal in append order. It validates the journal
// manifest, segment container, AOT4 checksum, and protobuf payload before fn is
// called. Replay is intentionally offline so its end boundary cannot race
// ingest or segment rotation.
func Replay(ctx context.Context, dataRoot string, fn func(Envelope) error) error {
	if fn == nil {
		return errors.New("otlpv4: replay callback is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Join(dataRoot, DirectoryName)
	if err := loadFormatManifest(dir); err != nil {
		return err
	}
	paths, err := closedJournalSegmentPaths(dir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := storage.OpenSegmentReader(path, nil)
		if err != nil {
			return fmt.Errorf("otlpv4: open journal segment %s: %w", filepath.Base(path), err)
		}
		scanErr := reader.Scan(func(record []byte) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			envelope, err := Parse(record)
			if err != nil {
				return fmt.Errorf("otlpv4: parse journal record: %w", err)
			}
			return fn(envelope)
		})
		closeErr := reader.Close()
		if scanErr != nil {
			return fmt.Errorf("otlpv4: replay journal segment %s: %w", filepath.Base(path), scanErr)
		}
		if closeErr != nil {
			return fmt.Errorf("otlpv4: close journal segment %s: %w", filepath.Base(path), closeErr)
		}
	}
	return nil
}

func ensureFormatManifest(dir string) error {
	path := filepath.Join(dir, FormatFileName)
	if _, err := os.Lstat(path); err == nil {
		return loadFormatManifest(dir)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("otlpv4: inspect format manifest: %w", err)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("otlpv4: inspect journal directory: %w", err)
	}
	tmp := path + ".tmp"
	for _, entry := range entries {
		if entry.Name() != FormatFileName+".tmp" || entry.IsDir() {
			return errors.New("otlpv4: journal has data but no format manifest")
		}
	}
	if err := os.Remove(tmp); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("otlpv4: remove stale format manifest: %w", err)
	}
	manifest := formatManifest{
		JournalFormatVersion: journalFormatVersion,
		EnvelopeMagic:        magic,
		EnvelopeVersion:      FormatVersion,
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("otlpv4: encode format manifest: %w", err)
	}
	payload = append(payload, '\n')
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("otlpv4: create format manifest: %w", err)
	}
	removeTmp := true
	defer func() {
		if removeTmp {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := file.Write(payload); err != nil {
		_ = file.Close()
		return fmt.Errorf("otlpv4: write format manifest: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("otlpv4: sync format manifest: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("otlpv4: close format manifest: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("otlpv4: publish format manifest: %w", err)
	}
	removeTmp = false
	directory, err := os.Open(dir) //nolint:gosec
	if err != nil {
		return fmt.Errorf("otlpv4: open journal directory for sync: %w", err)
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return fmt.Errorf("otlpv4: sync journal directory: %w", err)
	}
	if err := directory.Close(); err != nil {
		return fmt.Errorf("otlpv4: close journal directory: %w", err)
	}
	return nil
}

func loadFormatManifest(dir string) error {
	path := filepath.Join(dir, FormatFileName)
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("otlpv4: inspect format manifest: %w", err)
	}
	if !info.Mode().IsRegular() {
		return errors.New("otlpv4: format manifest is not a regular file")
	}
	payload, err := os.ReadFile(path) //nolint:gosec
	if err != nil {
		return fmt.Errorf("otlpv4: read format manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var manifest formatManifest
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("otlpv4: parse format manifest: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("otlpv4: parse format manifest: trailing data")
	}
	if manifest.JournalFormatVersion != journalFormatVersion ||
		manifest.EnvelopeMagic != magic || manifest.EnvelopeVersion != FormatVersion {
		return fmt.Errorf(
			"otlpv4: unsupported journal format %d/%q/%d",
			manifest.JournalFormatVersion, manifest.EnvelopeMagic, manifest.EnvelopeVersion,
		)
	}
	return nil
}

func closedJournalSegmentPaths(dir string) ([]string, error) {
	walInfo, err := os.Lstat(filepath.Join(dir, "amber.wal"))
	if err != nil {
		return nil, fmt.Errorf("otlpv4: inspect journal WAL: %w", err)
	}
	if !walInfo.Mode().IsRegular() || walInfo.Size() != 0 {
		return nil, errors.New("otlpv4: journal WAL is not cleanly closed")
	}
	payload, err := readRegular(filepath.Join(dir, "meta.json"))
	if err != nil {
		return nil, fmt.Errorf("otlpv4: read journal storage metadata: %w", err)
	}
	var meta storage.StoreMeta
	if err := decodeStrictJSON(payload, &meta); err != nil {
		return nil, fmt.Errorf("otlpv4: parse journal storage metadata: %w", err)
	}
	segments := append([]storage.SegmentMeta(nil), meta.Segments...)
	sort.Slice(segments, func(i, j int) bool { return segments[i].ID < segments[j].ID })
	paths := make([]string, 0, len(segments))
	seen := make(map[uint32]struct{}, len(segments))
	for _, segment := range segments {
		parsedID, ok := storage.ParseSegmentID(segment.FileName)
		if !ok || parsedID != segment.ID || !segment.Sealed || segment.DeletePending || !segment.HasLocalCopy() {
			return nil, fmt.Errorf("otlpv4: invalid closed journal segment %q", segment.FileName)
		}
		if _, duplicate := seen[segment.ID]; duplicate {
			return nil, fmt.Errorf("otlpv4: duplicate journal segment %d", segment.ID)
		}
		seen[segment.ID] = struct{}{}
		path := filepath.Join(dir, segment.FileName)
		info, err := os.Lstat(path)
		if err != nil {
			return nil, fmt.Errorf("otlpv4: inspect journal segment %s: %w", segment.FileName, err)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("otlpv4: journal segment %s is not regular", segment.FileName)
		}
		paths = append(paths, path)
	}
	return paths, nil
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
