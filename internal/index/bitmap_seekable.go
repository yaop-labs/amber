package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
)

// SpanBitmap is the read API the span query path needs from a segment's bitmap
// index. Both the fully-resident *MultiFieldIndex (active/unsealed segments,
// legacy BID2) and the lazy *SeekableBitmapIndex (sealed BID3) satisfy it, so
// the executor can hold either behind one cache.
type SpanBitmap interface {
	HasField(field string) bool
	FieldValues(field string) []string
	FilterMulti(conditions map[string][]string) []uint64
	FilterAny(field string, values []string) []uint64
}

var (
	_ SpanBitmap = (*MultiFieldIndex)(nil)
	_ SpanBitmap = (*SeekableBitmapIndex)(nil)
)

// ErrBitmapNotSeekable signals that a file is a valid but non-seekable bitmap
// (legacy BID2): the caller should fall back to a full LoadMultiFieldIndex.
var ErrBitmapNotSeekable = errors.New("bitmap: not a seekable (BID3) index")

type postingLoc struct {
	off    int64
	length int64
}

// SeekableBitmapIndex reads individual postings from a BID3 file on demand. Only
// the directory (field -> value -> blob location) is held resident; postings are
// pread and decoded per query, so a service+operation filter touches a few KB
// per segment instead of decoding the whole index.
type SeekableBitmapIndex struct {
	path string
	dir  map[string]map[string]postingLoc
}

// OpenSeekableBitmapIndex reads just the directory of a BID3 .bidx at path.
// Returns ErrBitmapNotSeekable for a legacy (BID2) file so the caller can fall
// back to a full load.
func OpenSeekableBitmapIndex(path string) (*SeekableBitmapIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("bitmap: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < 16 { // magic(4) + footer(12)
		return nil, ErrBitmapNotSeekable
	}

	var magic [4]byte
	if _, err := f.ReadAt(magic[:], 0); err != nil {
		return nil, err
	}
	if !bytes.Equal(magic[:], bitmapIndexMagicV3[:]) {
		return nil, ErrBitmapNotSeekable
	}

	var footer [12]byte
	if _, err := f.ReadAt(footer[:], size-12); err != nil {
		return nil, err
	}
	dirOff := int64(binary.LittleEndian.Uint64(footer[:8]))
	postingsEnd := size - 12
	if dirOff < 4 || dirOff > postingsEnd {
		return nil, errors.New("bitmap: bad directory offset")
	}

	dirBytes := make([]byte, postingsEnd-dirOff)
	if _, err := f.ReadAt(dirBytes, dirOff); err != nil {
		return nil, err
	}
	dir, err := parseBitmapDirectory(dirBytes, postingsEnd)
	if err != nil {
		return nil, err
	}
	return &SeekableBitmapIndex{path: path, dir: dir}, nil
}

// parseBitmapDirectory decodes the BID3 directory and bounds-checks every blob
// location against the postings region [4, postingsEnd).
func parseBitmapDirectory(b []byte, postingsEnd int64) (map[string]map[string]postingLoc, error) {
	r := b
	take := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errors.New("bitmap: truncated directory varint")
		}
		r = r[n:]
		return v, nil
	}
	takeStr := func() (string, error) {
		l, err := take()
		if err != nil {
			return "", err
		}
		if l > uint64(len(r)) {
			return "", errors.New("bitmap: directory string out of range")
		}
		s := string(r[:l])
		r = r[l:]
		return s, nil
	}

	fieldCount, err := take()
	if err != nil {
		return nil, err
	}
	dir := make(map[string]map[string]postingLoc, fieldCount)
	for range fieldCount {
		field, err := takeStr()
		if err != nil {
			return nil, err
		}
		valueCount, err := take()
		if err != nil {
			return nil, err
		}
		values := make(map[string]postingLoc, valueCount)
		for range valueCount {
			value, err := takeStr()
			if err != nil {
				return nil, err
			}
			off, err := take()
			if err != nil {
				return nil, err
			}
			length, err := take()
			if err != nil {
				return nil, err
			}
			end := int64(off) + int64(length)
			if int64(off) < 4 || length < 4 || end > postingsEnd {
				return nil, errors.New("bitmap: directory blob out of range")
			}
			values[value] = postingLoc{off: int64(off), length: int64(length)}
		}
		dir[field] = values
	}
	return dir, nil
}

// readPostingAt reads and verifies one CRC-prefixed posting blob.
func readPostingAt(f *os.File, loc postingLoc) ([]uint64, error) {
	buf := make([]byte, loc.length)
	if _, err := f.ReadAt(buf, loc.off); err != nil {
		return nil, err
	}
	want := binary.LittleEndian.Uint32(buf[:4])
	if crc32.ChecksumIEEE(buf[4:]) != want {
		return nil, errors.New("bitmap: posting crc mismatch")
	}
	return decodePosting(buf[4:])
}

func (s *SeekableBitmapIndex) HasField(field string) bool {
	_, ok := s.dir[field]
	return ok
}

func (s *SeekableBitmapIndex) FieldValues(field string) []string {
	values, ok := s.dir[field]
	if !ok {
		return nil
	}
	out := make([]string, 0, len(values))
	for v := range values {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

// filterAnyFrom unions the postings of one field's requested values, reading
// from an already-open file. Mirrors MultiFieldIndex.FilterAny.
func (s *SeekableBitmapIndex) filterAnyFrom(f *os.File, field string, values []string) ([]uint64, error) {
	locs, ok := s.dir[field]
	if !ok {
		return nil, nil
	}
	var result []uint64
	for _, v := range values {
		loc, ok := locs[v]
		if !ok {
			continue
		}
		ids, err := readPostingAt(f, loc)
		if err != nil {
			return nil, err
		}
		if result == nil {
			result = ids
		} else {
			result = UnionSorted(result, ids)
		}
	}
	return result, nil
}

func (s *SeekableBitmapIndex) FilterAny(field string, values []string) []uint64 {
	f, err := os.Open(s.path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()
	ids, err := s.filterAnyFrom(f, field, values)
	if err != nil {
		return nil
	}
	return ids
}

// FilterMulti intersects per-field unions (OR within a field, AND across
// fields), opening the file once for the whole query. On a read error it
// returns nil, letting the caller fall back to the exact scan filter.
func (s *SeekableBitmapIndex) FilterMulti(conditions map[string][]string) []uint64 {
	f, err := os.Open(s.path)
	if err != nil {
		return nil
	}
	defer func() { _ = f.Close() }()

	var result []uint64
	first := true
	for field, values := range conditions {
		fieldIDs, err := s.filterAnyFrom(f, field, values)
		if err != nil {
			return nil
		}
		if len(fieldIDs) == 0 {
			return nil
		}
		if first {
			result = fieldIDs
			first = false
			continue
		}
		result = IntersectSorted(result, fieldIDs)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

// loadMultiFieldIndexV3 fully decodes a BID3 file into a resident
// MultiFieldIndex (used by the log path and rebuilds, which want the whole
// index in memory). It verifies the whole-file CRC and every posting CRC.
func loadMultiFieldIndexV3(data []byte) (*MultiFieldIndex, error) {
	if len(data) < 16 {
		return nil, errors.New("bitmap: truncated BID3 file")
	}
	body := data[:len(data)-12]
	footer := data[len(data)-12:]
	dirOff := int64(binary.LittleEndian.Uint64(footer[:8]))
	wantCRC := binary.LittleEndian.Uint32(footer[8:])
	if crc32.ChecksumIEEE(body) != wantCRC {
		return nil, errors.New("bitmap: crc mismatch")
	}
	postingsEnd := int64(len(body))
	if dirOff < 4 || dirOff > postingsEnd {
		return nil, errors.New("bitmap: bad directory offset")
	}
	dir, err := parseBitmapDirectory(body[dirOff:], postingsEnd)
	if err != nil {
		return nil, err
	}

	m := NewMultiFieldIndex()
	for field, values := range dir {
		bi := NewBitmapIndex()
		for value, loc := range values {
			blob := body[loc.off : loc.off+loc.length]
			if crc32.ChecksumIEEE(blob[4:]) != binary.LittleEndian.Uint32(blob[:4]) {
				return nil, errors.New("bitmap: posting crc mismatch")
			}
			ids, err := decodePosting(blob[4:])
			if err != nil {
				return nil, err
			}
			bi.values[value] = &valueBucket{ids: ids, sorted: true, normalized: true}
		}
		m.fields[field] = bi
	}
	return m, nil
}
