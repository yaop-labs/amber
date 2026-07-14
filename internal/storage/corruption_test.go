package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Corruption-injection tests pin the per-subsystem corruption contract:
//
//   - WAL: a structurally incomplete final record is repaired, but checksum or
//     middle corruption fails open rather than guessing a safe append point.
//   - Sealed segment blocks: the store opens; scanning the damaged segment
//     reports an error (zstd CRC / magic), it does not return garbage.
//   - Segment footer: deliberately survivable - the reader falls back to the
//     footerless block scan and every record stays readable.
//   - meta.json: fail-stop. Meta is the durability source of truth and is
//     written atomically, so a corrupt meta means disk-level damage; opening
//     would mean guessing about data. Open refuses.

// writeAbandonedFixture writes 120 records rotating every 50, then abandons
// the manager crash-style (no Close): two sealed segments hold 1..100, the
// WAL holds 101..120 destined for the unsealed active segment.
func writeAbandonedFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 50})
	if err != nil {
		t.Fatalf("open fixture manager: %v", err)
	}
	writeFixtureRecords(t, sm)
	return dir
}

// writeClosedFixture writes the same 120 records and closes cleanly: three
// sealed segments (50+50+20), empty WAL.
func writeClosedFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	sm, err := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 50})
	if err != nil {
		t.Fatalf("open fixture manager: %v", err)
	}
	writeFixtureRecords(t, sm)
	if err := sm.Close(); err != nil {
		t.Fatalf("close fixture manager: %v", err)
	}
	return dir
}

func writeFixtureRecords(t *testing.T, sm *SegmentManager) {
	t.Helper()
	base := time.Now().UnixNano()
	for i := 1; i <= 120; i++ {
		data := fmt.Appendf(nil, "corr-rec-%05d", i)
		if err := sm.Write(data, base+int64(i)*int64(time.Microsecond)); err != nil {
			t.Fatalf("fixture write %d: %v", i, err)
		}
	}
}

// walRecordStart walks the WAL record chain and returns the file offset
// where the n-th (0-based) record's header begins.
func walRecordStart(t *testing.T, walPath string, n int) int64 {
	t.Helper()
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatalf("read wal: %v", err)
	}
	off := int64(0)
	for i := 0; ; i++ {
		if off+walHeaderSize > int64(len(data)) {
			t.Fatalf("wal ended before record %d (have %d)", n, i)
		}
		if binary.LittleEndian.Uint32(data[off:off+4]) != walMagic {
			t.Fatalf("bad wal magic at offset %d", off)
		}
		if i == n {
			return off
		}
		off += walHeaderSize + int64(binary.LittleEndian.Uint32(data[off+8:off+12]))
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open %s for corruption: %v", path, err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatalf("read byte at %d: %v", off, err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatalf("write byte at %d: %v", off, err)
	}
}

func TestCorruption_WALRecord(t *testing.T) {
	dir := writeAbandonedFixture(t)

	// Corrupt the 10th unreplayed WAL record (= record 110): replay must
	// keep 101..109, stop there, and count the corruption.
	walPath := filepath.Join(dir, walFileName)
	// header | ts(8) | data - flip a byte inside the data region.
	flipByte(t, walPath, walRecordStart(t, walPath, 9)+walHeaderSize+8+2)

	sm, err := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 50})
	if sm != nil {
		_ = sm.Close()
	}
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("open error = %v, want ErrWALCorrupted", err)
	}
	// Refusal must be stable: a failed open may replay valid prefix bytes into
	// the active fd, but the next attempt rolls back to the durable watermark
	// and must still reject the same corrupt WAL.
	if sm, err = OpenSegmentManager(dir, RotationPolicy{MaxRecords: 50}); sm != nil {
		_ = sm.Close()
	}
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("second open error = %v, want ErrWALCorrupted", err)
	}
}

// TestCorruption_WALSeqField pins that the record CRC covers the header's
// seq field. An unprotected flipped bit in seq made replay silently treat
// the record as already durable (seq <= synced watermark) - undetected loss.
func TestCorruption_WALSeqField(t *testing.T) {
	dir := writeAbandonedFixture(t)

	walPath := filepath.Join(dir, walFileName)
	// seq lives at header bytes 12..20; flip its low byte in record 10.
	flipByte(t, walPath, walRecordStart(t, walPath, 9)+12)

	sm, err := OpenSegmentManager(dir, RotationPolicy{MaxRecords: 50})
	if sm != nil {
		_ = sm.Close()
	}
	if !errors.Is(err, ErrWALCorrupted) {
		t.Fatalf("open error = %v, want ErrWALCorrupted", err)
	}
}

func TestCorruption_SealedSegmentBlock(t *testing.T) {
	// Two flip positions: the zstd frame header (structural damage) and the
	// middle of the compressed payload (caught by the zstd content CRC).
	cases := []struct {
		name string
		off  func(compressedSize int64) int64
	}{
		{"frame_header", func(int64) int64 { return 4 }},
		{"payload_crc", func(cs int64) int64 { return cs / 2 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := writeClosedFixture(t)
			segPath := filepath.Join(dir, segmentFileName(1))

			// Read the first block header to size the corruption target.
			f, err := os.Open(segPath)
			if err != nil {
				t.Fatalf("open segment: %v", err)
			}
			var blockHeader [blockHeaderSize]byte
			if _, err := f.ReadAt(blockHeader[:], segHeaderSize); err != nil {
				t.Fatalf("read block header: %v", err)
			}
			_ = f.Close()
			compressedSize := int64(binary.LittleEndian.Uint32(blockHeader[8:12]))

			flipByte(t, segPath, segHeaderSize+blockHeaderSize+tc.off(compressedSize))

			// The store opens - sealed segments are not scanned at open.
			sm, err := OpenSegmentManager(dir, DefaultRotationPolicy)
			if err != nil {
				t.Fatalf("store must open with a damaged sealed segment, got: %v", err)
			}
			defer sm.Close()

			// Scanning the damaged segment must error, not return garbage.
			sr, err := OpenSegmentReader(segPath, nil)
			if err != nil {
				// Failing at open is an equally honest report.
				t.Logf("reader open reports: %v", err)
				return
			}
			defer sr.Close()
			scanErr := sr.Scan(func([]byte) error { return nil })
			if scanErr == nil {
				t.Error("scan of corrupted block returned no error")
			} else {
				t.Logf("scan reports: %v", scanErr)
			}
		})
	}
}

func TestCorruption_SegmentFooter(t *testing.T) {
	dir := writeClosedFixture(t)

	// Chop into the footer: the reader must fall back to the footerless
	// block scan and still return every record.
	segPath := filepath.Join(dir, segmentFileName(1))
	info, err := os.Stat(segPath)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if err := os.Truncate(segPath, info.Size()-5); err != nil {
		t.Fatalf("truncate footer: %v", err)
	}

	sr, err := OpenSegmentReader(segPath, nil)
	if err != nil {
		t.Fatalf("reader must survive a truncated footer, got: %v", err)
	}
	defer sr.Close()

	count := 0
	if err := sr.Scan(func([]byte) error { count++; return nil }); err != nil {
		t.Fatalf("footerless scan: %v", err)
	}
	if count != 50 {
		t.Errorf("footerless scan returned %d records, want 50", count)
	}
}

func TestCorruption_Meta(t *testing.T) {
	dir := writeClosedFixture(t)

	// Structural damage to meta.json (first byte of the JSON object).
	flipByte(t, filepath.Join(dir, metaFileName), 0)

	if _, err := OpenSegmentManager(dir, DefaultRotationPolicy); err == nil {
		t.Fatal("open over corrupt meta.json must fail-stop, got nil error")
	} else {
		t.Logf("open reports: %v", err)
	}
}
