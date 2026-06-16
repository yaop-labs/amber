package block

import (
	"encoding/binary"
	"errors"

	"github.com/yaop-labs/amber/internal/metricsengine/codec"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// directory_binary.go is the binary on-disk encoding of a block Directory.
// It replaces the JSON directory that dominated the metrics-campaign query
// wall: json.Unmarshal of a 100k-entry directory cost ~1.77s and ~262MB /
// 1.5M allocs per block, and the query path re-decodes every in-window block
// (see store/rate_query_bench_test.go). Binary decode interns label strings
// within the block (5 names + a few thousand values stand in for 500k label
// strings) and pours all Labels / AggregateBuckets into one backing slice
// each, so decode is a near-flat scan with O(strings + blocks) allocations.
//
// The format is self-describing via a leading magic that cannot collide with
// the legacy JSON directory (which always starts with '{'), so a new reader
// transparently handles both and old JSON blocks still load.

const directoryBinaryMagic = "MDB2"

// hasBinaryDirectoryMagic reports whether a directory payload is the binary
// form. Legacy JSON directories begin with '{' and never match.
func hasBinaryDirectoryMagic(payload []byte) bool {
	return len(payload) >= len(directoryBinaryMagic) &&
		string(payload[:len(directoryBinaryMagic)]) == directoryBinaryMagic
}

// encodeDirectoryBinary serialises a Directory into the MDB2 binary format.
func encodeDirectoryBinary(dir Directory) []byte {
	// Build the per-block string table from label names and values. Label
	// names are a tiny fixed set; values repeat heavily across series.
	strIndex := make(map[string]uint64)
	var strTable []string
	intern := func(s string) {
		if _, ok := strIndex[s]; ok {
			return
		}
		strIndex[s] = uint64(len(strTable))
		strTable = append(strTable, s)
	}
	// Pre-walk to populate the table so it is written before the entries.
	for _, e := range dir.Series {
		for _, l := range e.Labels {
			intern(l.Name)
			intern(l.Value)
		}
	}

	dst := make([]byte, 0, len(dir.Series)*48)
	dst = append(dst, directoryBinaryMagic...)
	dst = binary.AppendUvarint(dst, uint64(dir.Version))

	dst = binary.AppendUvarint(dst, uint64(len(strTable)))
	for _, s := range strTable {
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		dst = append(dst, s...)
	}

	dst = binary.AppendUvarint(dst, uint64(len(dir.Series)))
	for _, e := range dir.Series {
		dst = binary.AppendUvarint(dst, e.SeriesID)
		dst = append(dst, byte(e.Type), byte(e.TimestampKind), byte(e.ValueStrategy), zoneFlags(e.ZoneMap))

		dst = binary.AppendVarint(dst, e.TimeMin)
		dst = binary.AppendVarint(dst, e.TimeMax)
		dst = binary.AppendUvarint(dst, uint64(e.TimestampOff))
		dst = binary.AppendUvarint(dst, uint64(e.TimestampLen))
		dst = binary.AppendUvarint(dst, uint64(e.TimestampN))
		dst = binary.AppendVarint(dst, e.TimestampBase)
		dst = binary.AppendVarint(dst, e.TimestampStep)
		dst = binary.AppendUvarint(dst, uint64(e.ValueOff))
		dst = binary.AppendUvarint(dst, uint64(e.ValueLen))
		dst = binary.AppendUvarint(dst, uint64(e.ValueN))
		dst = binary.AppendVarint(dst, e.ValueBase)

		dst = binary.AppendVarint(dst, e.ZoneMap.Min)
		dst = binary.AppendVarint(dst, e.ZoneMap.Max)
		dst = binary.AppendVarint(dst, e.ZoneMap.Sum)
		dst = binary.AppendUvarint(dst, uint64(e.ZoneMap.Count))
		dst = binary.AppendVarint(dst, e.ZoneMap.First)
		dst = binary.AppendVarint(dst, e.ZoneMap.Last)

		dst = binary.AppendUvarint(dst, uint64(len(e.Labels)))
		for _, l := range e.Labels {
			dst = binary.AppendUvarint(dst, strIndex[l.Name])
			dst = binary.AppendUvarint(dst, strIndex[l.Value])
		}

		dst = binary.AppendUvarint(dst, uint64(len(e.AggregateBuckets)))
		for _, b := range e.AggregateBuckets {
			dst = binary.AppendVarint(dst, b.TimeMin)
			dst = binary.AppendVarint(dst, b.TimeMax)
			dst = binary.AppendVarint(dst, b.Min)
			dst = binary.AppendVarint(dst, b.Max)
			dst = binary.AppendVarint(dst, b.Sum)
			dst = binary.AppendUvarint(dst, uint64(b.Count))
		}
	}
	return dst
}

func zoneFlags(z ZoneMap) byte {
	var f byte
	if z.Monotonic {
		f |= 1
	}
	if z.HasReset {
		f |= 2
	}
	return f
}

// binReader is a cursor over a binary directory payload. It records the first
// decode error and turns every subsequent read into a no-op so the caller can
// check err once after the scan.
type binReader struct {
	buf []byte
	pos int
	err error
}

func (r *binReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf[r.pos:])
	if n <= 0 {
		r.err = errors.New("block: truncated directory uvarint")
		return 0
	}
	r.pos += n
	return v
}

func (r *binReader) varint() int64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Varint(r.buf[r.pos:])
	if n <= 0 {
		r.err = errors.New("block: truncated directory varint")
		return 0
	}
	r.pos += n
	return v
}

func (r *binReader) byte() byte {
	if r.err != nil {
		return 0
	}
	if r.pos >= len(r.buf) {
		r.err = errors.New("block: truncated directory byte")
		return 0
	}
	b := r.buf[r.pos]
	r.pos++
	return b
}

// bytes returns a sub-slice of length n. The result aliases the payload, so
// callers that retain it past the payload's lifetime must copy. Label strings
// are converted with string(...) at decode and so are independent.
func (r *binReader) bytes(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || r.pos+n > len(r.buf) {
		r.err = errors.New("block: truncated directory bytes")
		return nil
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b
}

// decodeDirectoryBinary parses an MDB2 directory payload. The CRC is verified
// by the caller (decodeDirectoryPayload) before this runs.
func decodeDirectoryBinary(payload []byte) (Directory, error) {
	if !hasBinaryDirectoryMagic(payload) {
		return Directory{}, errors.New("block: not a binary directory")
	}
	r := &binReader{buf: payload, pos: len(directoryBinaryMagic)}

	ver := uint16(r.uvarint())
	if r.err != nil {
		return Directory{}, r.err
	}
	if ver != version {
		return Directory{}, errors.New("block: unsupported version")
	}

	strCount := int(r.uvarint())
	if r.err != nil {
		return Directory{}, r.err
	}
	if strCount < 0 || strCount > len(payload) {
		return Directory{}, errors.New("block: invalid directory string count")
	}
	strTable := make([]string, strCount)
	for i := range strTable {
		n := int(r.uvarint())
		strTable[i] = string(r.bytes(n))
	}
	if r.err != nil {
		return Directory{}, r.err
	}
	lookup := func(idx uint64) (string, bool) {
		if idx >= uint64(len(strTable)) {
			return "", false
		}
		return strTable[idx], true
	}

	entryCount := int(r.uvarint())
	if r.err != nil {
		return Directory{}, r.err
	}
	if entryCount < 0 || entryCount > len(payload) {
		return Directory{}, errors.New("block: invalid directory entry count")
	}

	dir := Directory{Version: ver, Series: make([]DirectoryEntry, entryCount)}
	// First pass would be needed to size the shared backing slices exactly;
	// instead grow them lazily but in one arena each by appending. Pre-size
	// with a heuristic (5 labels/series is the common shape) to avoid most
	// regrowth while keeping a single backing allocation.
	labelArena := make([]model.Label, 0, entryCount*5)
	var bucketArena []AggregateBucket

	for i := range dir.Series {
		e := &dir.Series[i]
		e.SeriesID = r.uvarint()
		e.Type = model.MetricType(r.byte())
		e.TimestampKind = codec.TimestampStrategy(r.byte())
		e.ValueStrategy = codec.ValueStrategy(r.byte())
		flags := r.byte()

		e.TimeMin = r.varint()
		e.TimeMax = r.varint()
		e.TimestampOff = int64(r.uvarint())
		e.TimestampLen = int64(r.uvarint())
		e.TimestampN = int(r.uvarint())
		e.TimestampBase = r.varint()
		e.TimestampStep = r.varint()
		e.ValueOff = int64(r.uvarint())
		e.ValueLen = int64(r.uvarint())
		e.ValueN = int(r.uvarint())
		e.ValueBase = r.varint()

		e.ZoneMap.Min = r.varint()
		e.ZoneMap.Max = r.varint()
		e.ZoneMap.Sum = r.varint()
		e.ZoneMap.Count = int(r.uvarint())
		e.ZoneMap.First = r.varint()
		e.ZoneMap.Last = r.varint()
		e.ZoneMap.Monotonic = flags&1 != 0
		e.ZoneMap.HasReset = flags&2 != 0

		labelCount := int(r.uvarint())
		if r.err != nil {
			return Directory{}, r.err
		}
		if labelCount < 0 || labelCount > len(payload) {
			return Directory{}, errors.New("block: invalid label count")
		}
		start := len(labelArena)
		for j := 0; j < labelCount; j++ {
			name, okN := lookup(r.uvarint())
			value, okV := lookup(r.uvarint())
			if !okN || !okV {
				if r.err == nil {
					r.err = errors.New("block: directory string index out of range")
				}
				return Directory{}, r.err
			}
			labelArena = append(labelArena, model.Label{Name: name, Value: value})
		}
		e.Labels = labelArena[start:len(labelArena):len(labelArena)]

		bucketCount := int(r.uvarint())
		if r.err != nil {
			return Directory{}, r.err
		}
		if bucketCount < 0 || bucketCount > len(payload) {
			return Directory{}, errors.New("block: invalid bucket count")
		}
		if bucketCount > 0 {
			bstart := len(bucketArena)
			for j := 0; j < bucketCount; j++ {
				bucketArena = append(bucketArena, AggregateBucket{
					TimeMin: r.varint(),
					TimeMax: r.varint(),
					Min:     r.varint(),
					Max:     r.varint(),
					Sum:     r.varint(),
					Count:   int(r.uvarint()),
				})
			}
			e.AggregateBuckets = bucketArena[bstart:len(bucketArena):len(bucketArena)]
		}
	}
	if r.err != nil {
		return Directory{}, r.err
	}
	return dir, nil
}
