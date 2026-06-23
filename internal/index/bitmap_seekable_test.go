package index

import (
	"encoding/binary"
	"hash/crc32"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// buildSampleIndex makes a multi-field index with skewed, overlapping postings.
func buildSampleIndex(rng *rand.Rand) *MultiFieldIndex {
	m := NewMultiFieldIndex()
	services := []string{"api", "db", "cache", "auth"}
	ops := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	for id := uint64(1); id <= 4000; id++ {
		m.Add("service", services[rng.Intn(len(services))], id)
		m.Add("operation", ops[rng.Intn(len(ops))], id)
		if id%7 == 0 {
			m.Add("status", "error", id)
		} else {
			m.Add("status", "ok", id)
		}
	}
	return m
}

// TestSeekableMatchesResident checks that the lazy BID3 reader answers HasField,
// FieldValues, FilterAny, and FilterMulti identically to the fully-loaded index.
func TestSeekableMatchesResident(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	src := buildSampleIndex(rng)
	path := filepath.Join(t.TempDir(), "seg.bidx")
	if err := src.Save(path); err != nil {
		t.Fatal(err)
	}

	resident, err := LoadMultiFieldIndex(path) // BID3 full path
	if err != nil {
		t.Fatalf("full load: %v", err)
	}
	seek, err := OpenSeekableBitmapIndex(path)
	if err != nil {
		t.Fatalf("seek open: %v", err)
	}

	for _, field := range []string{"service", "operation", "status", "missing"} {
		if resident.HasField(field) != seek.HasField(field) {
			t.Fatalf("HasField(%q) mismatch", field)
		}
		// FieldValues order is unspecified for the resident index (map order);
		// compare as sets.
		rv, sv := resident.FieldValues(field), seek.FieldValues(field)
		slices.Sort(rv)
		if !slices.Equal(rv, sv) {
			t.Fatalf("FieldValues(%q): %v vs %v", field, rv, sv)
		}
	}

	conds := []map[string][]string{
		{"service": {"api"}},
		{"service": {"api", "db"}},
		{"operation": {"GET", "POST"}},
		{"service": {"api"}, "operation": {"GET"}},
		{"service": {"api", "cache"}, "operation": {"GET", "PUT"}, "status": {"error"}},
		{"service": {"nonexistent"}},
		{"service": {"api"}, "operation": {"nonexistent"}},
	}
	for _, c := range conds {
		want := resident.FilterMulti(c)
		got := seek.FilterMulti(c)
		if !slices.Equal(want, got) {
			t.Fatalf("FilterMulti(%v): seek=%v resident=%v", c, got, want)
		}
	}

	for _, vals := range [][]string{{"api"}, {"api", "db"}, {"none"}} {
		if !slices.Equal(resident.FilterAny("service", vals), seek.FilterAny("service", vals)) {
			t.Fatalf("FilterAny(service,%v) mismatch", vals)
		}
	}
}

// TestSeekableRoundTripValues exhaustively compares each value's posting.
func TestSeekableRoundTripValues(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	src := buildSampleIndex(rng)
	path := filepath.Join(t.TempDir(), "seg.bidx")
	if err := src.Save(path); err != nil {
		t.Fatal(err)
	}
	seek, err := OpenSeekableBitmapIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"service", "operation", "status"} {
		for _, value := range src.FieldValues(field) {
			want := src.FilterAny(field, []string{value})
			got := seek.FilterAny(field, []string{value})
			if !slices.Equal(want, got) {
				t.Fatalf("posting %s=%s: seek=%d ids, resident=%d ids", field, value, len(got), len(want))
			}
		}
	}
}

// TestOpenSeekableRejectsLegacyBID2 confirms a BID2 file is reported as
// not-seekable so the caller falls back to a full load (which still works).
func TestOpenSeekableRejectsLegacyBID2(t *testing.T) {
	// Hand-build a minimal valid BID2 file: magic + uvarint(0 fields) + CRC.
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], 0)
	body := make([]byte, 0, len(bitmapIndexMagic)+n)
	body = append(body, bitmapIndexMagic[:]...)
	body = append(body, scratch[:n]...)
	var sum [4]byte
	binary.LittleEndian.PutUint32(sum[:], crc32.ChecksumIEEE(body))
	data := make([]byte, 0, len(body)+len(sum))
	data = append(data, body...)
	data = append(data, sum[:]...)

	path := filepath.Join(t.TempDir(), "legacy.bidx")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenSeekableBitmapIndex(path); err != ErrBitmapNotSeekable {
		t.Fatalf("expected ErrBitmapNotSeekable, got %v", err)
	}
	if _, err := LoadMultiFieldIndex(path); err != nil {
		t.Fatalf("legacy BID2 should still load fully: %v", err)
	}
}

// TestSeekableDetectsPostingCorruption flips a byte in the postings region and
// checks the posting CRC catches it (a corrupt prune must error, not silently
// drop candidates).
func TestSeekableDetectsPostingCorruption(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	src := buildSampleIndex(rng)
	path := filepath.Join(t.TempDir(), "seg.bidx")
	if err := src.Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte well inside the postings region (after magic, before dir).
	data[64] ^= 0xFF
	corrupt := filepath.Join(t.TempDir(), "corrupt.bidx")
	if err := os.WriteFile(corrupt, data, 0o644); err != nil {
		t.Fatal(err)
	}
	// The whole-file CRC must reject the corruption on the full-load path.
	if _, err := LoadMultiFieldIndex(corrupt); err == nil {
		t.Fatal("full load should fail the whole-file CRC")
	}
	seek, err := OpenSeekableBitmapIndex(corrupt)
	if err != nil {
		t.Fatalf("directory still opens: %v", err)
	}
	// Some posting read should now fail its CRC; FilterAny returns nil so the
	// caller falls back to the exact scan. Walk every field's values to hit it.
	hitNil := false
	for _, field := range []string{"service", "operation", "status"} {
		for _, v := range seek.FieldValues(field) {
			if seek.FilterAny(field, []string{v}) == nil {
				hitNil = true
			}
		}
	}
	if !hitNil {
		t.Fatal("expected a CRC failure to surface as a nil posting result")
	}
}
