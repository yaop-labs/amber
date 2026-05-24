package storage

// Storage primitive benchmarks. Three things matter for Phase 1 (S3):
//
//  1. WriteBatch throughput — sets the ceiling on ingest.
//  2. OpenSegmentReader cold (no hint, has footer) — first read after S3 GET.
//  3. scanBlockOffsets — what we pay when footer is missing/corrupt and we
//     must rebuild the block index by streaming through the segment. S3
//     latency on top of this is the worst-case cold-segment cost.

import (
	"bytes"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
)

func benchEntryBytes() []byte {
	entry := model.LogEntry{
		ID:        model.MustNewEntryID(),
		Timestamp: time.Now(),
		Level:     model.LevelInfo,
		Service:   "api-gateway",
		Host:      "host-001",
		Body:      "GET /api/v1/users 200 success latency 45ms",
		Attrs: []model.Attr{
			{Key: "env", Value: "prod"},
			{Key: "region", Value: "us-east-1"},
		},
	}
	var buf bytes.Buffer
	entry.WriteTo(&buf)
	return buf.Bytes()
}

func benchSetupBatch(b *testing.B, batchSize int) []BatchItem {
	b.Helper()
	data := benchEntryBytes()
	batch := make([]BatchItem, batchSize)
	now := time.Now().UnixNano()
	for i := range batch {
		// Each item gets its own slice so WriteBatch's append paths see
		// realistic per-item buffers, not shared backing memory.
		buf := make([]byte, len(data))
		copy(buf, data)
		batch[i] = BatchItem{Data: buf, TS: now + int64(i)}
	}
	return batch
}

func benchRunWriteBatch(b *testing.B, batchSize int) {
	b.Helper()
	dir := b.TempDir()
	// Huge thresholds so rotation never trips during the bench — we want
	// pure WriteBatch cost, not rotation overhead.
	policy := RotationPolicy{MaxRecords: 1_000_000_000, MaxBytes: 64 << 30}
	mgr, err := OpenSegmentManager(dir, policy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}
	defer mgr.Close()

	batch := benchSetupBatch(b, batchSize)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := mgr.WriteBatch(batch); err != nil {
			b.Fatalf("WriteBatch: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)*float64(batchSize)/b.Elapsed().Seconds(), "records/sec")
}

func BenchmarkWriteBatch_10(b *testing.B)   { benchRunWriteBatch(b, 10) }
func BenchmarkWriteBatch_100(b *testing.B)  { benchRunWriteBatch(b, 100) }
func BenchmarkWriteBatch_1000(b *testing.B) { benchRunWriteBatch(b, 1000) }

// prepareSealedSegment writes recordCount records to a fresh segment, rotates
// (which writes the footer), and returns the on-disk path.
func prepareSealedSegment(b *testing.B, recordCount int) string {
	b.Helper()
	dir := b.TempDir()
	policy := RotationPolicy{MaxRecords: 1_000_000_000, MaxBytes: 64 << 30}
	mgr, err := OpenSegmentManager(dir, policy)
	if err != nil {
		b.Fatalf("OpenSegmentManager: %v", err)
	}

	const chunk = 1000
	full := benchSetupBatch(b, chunk)
	for written := 0; written < recordCount; written += chunk {
		n := min(recordCount-written, chunk)
		if err := mgr.WriteBatch(full[:n]); err != nil {
			b.Fatalf("WriteBatch: %v", err)
		}
	}
	active, ok := mgr.ActiveSegmentMeta()
	if !ok {
		b.Fatal("ActiveSegmentMeta: not found")
	}
	path := mgr.SegmentPath(active)
	if err := mgr.Rotate(); err != nil {
		b.Fatalf("Rotate: %v", err)
	}
	mgr.Close()
	return path
}

// BenchmarkSegmentReader_ScanFiltered_100k measures the R1 query path:
// full-segment scan with a 4% allowedIDs bitmap (service+level selectivity).
// This is the hot path that determines R1 latency in loadbench.
func BenchmarkSegmentReader_ScanFiltered_100k(b *testing.B) {
	path := prepareSealedSegment(b, 100_000)

	// Build a bitmap with every 25th ID to simulate 4% selectivity
	// (service=svc-00 AND level=ERROR with 5 services × 5 levels).
	allowed := make(map[uint64]struct{}, 4000)
	for i := uint64(15); i < 100_000; i += 25 {
		allowed[i] = struct{}{}
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			b.Fatalf("OpenSegmentReader: %v", err)
		}
		matched := 0
		_ = sr.Scan(func(data []byte) error {
			if len(data) < 10 {
				return nil
			}
			// Peek ID (bytes 2-9, big-endian, matching model.EntryIDToUint64)
			id := uint64(data[2])<<56 | uint64(data[3])<<48 | uint64(data[4])<<40 |
				uint64(data[5])<<32 | uint64(data[6])<<24 | uint64(data[7])<<16 |
				uint64(data[8])<<8 | uint64(data[9])
			if _, ok := allowed[id]; ok {
				matched++
			}
			return nil
		})
		sr.Close()
		_ = matched
	}
	b.ReportMetric(float64(b.N)*100_000/b.Elapsed().Seconds(), "records/sec")
}

// BenchmarkOpenSegmentReader_WithFooter_10k measures the happy path: footer
// is present, no need to scan blocks. This is what we pay on every cold open
// of a properly-sealed segment.
func BenchmarkOpenSegmentReader_WithFooter_10k(b *testing.B) {
	path := prepareSealedSegment(b, 10_000)

	b.ReportAllocs()
	for b.Loop() {
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			b.Fatalf("OpenSegmentReader: %v", err)
		}
		sr.Close()
	}
}

// BenchmarkScanBlockOffsets_10k forces the no-footer recovery path: we open
// the segment, then directly invoke scanBlockOffsets. This is the cost we'd
// pay every cold open if footers were lost — and what S3-cold reads look like
// if we ever cache only data, not footers.
func BenchmarkScanBlockOffsets_10k(b *testing.B) {
	path := prepareSealedSegment(b, 10_000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// Re-open each iteration: scanBlockOffsets mutates SegmentReader
		// state and seeks the underlying file. Sharing one reader across
		// iterations would measure the second-call no-op.
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			b.Fatalf("OpenSegmentReader: %v", err)
		}
		if err := sr.scanBlockOffsets(); err != nil {
			b.Fatalf("scanBlockOffsets: %v", err)
		}
		sr.Close()
	}
	b.ReportMetric(10_000*float64(b.N)/b.Elapsed().Seconds(), "records_scanned/sec")
}

// BenchmarkSegmentReader_ScanAll_10k measures full sequential scan over a
// sealed segment — what a no-filter query pays per segment.
func BenchmarkSegmentReader_ScanAll_10k(b *testing.B) {
	path := prepareSealedSegment(b, 10_000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			b.Fatalf("OpenSegmentReader: %v", err)
		}
		count := 0
		if err := sr.Scan(func([]byte) error {
			count++
			return nil
		}); err != nil {
			b.Fatalf("Scan: %v", err)
		}
		sr.Close()
		if count != 10_000 {
			b.Fatalf("scanned %d, want 10000", count)
		}
	}
	b.ReportMetric(10_000*float64(b.N)/b.Elapsed().Seconds(), "records/sec")
}
