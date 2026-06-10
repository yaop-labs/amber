package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// Catalog-log binary record format.
// File lifecycle is handled in catalog_log.go; this file only defines framing
// and record encoding.
//
//	record   = total_len[4] | crc32[4] | type[1] | body[total_len-9]
//	type=0x01 REGISTER:  series_id[8] | labels_len[4] | labels_blob[labels_len]
//	type=0x03 EVICT:     series_id[8] | ts_unix_ms[8]
//
// CRC covers total_len|type|body, excluding the crc field. The length prefix is
// included so recovery never trusts an unverified next-record boundary.
//
// All integers are fixed-width little-endian. Labels are encoded as
// (name_len[2]|name|value_len[2]|value) pairs.
//
// TOUCH is reserved for a future coalesced-touch record. v0 rebuilds
// last-touch state from blocks during startup reconciliation.

const (
	catalogRecordRegister byte = 0x01
	catalogRecordTouch    byte = 0x02 // reserved, not written by v0
	catalogRecordEvict    byte = 0x03

	// Header = total_len[4] + crc[4]. Body lives after.
	catalogHeaderLen = 8
	// Minimum record = header + type[1]. Any record shorter is malformed.
	catalogMinRecordLen = catalogHeaderLen + 1
	// Cap a single record to refuse absurd allocations on corrupt input.
	catalogMaxRecordLen = 1 << 20 // 1 MiB

	// Body layouts (the bytes after the 8-byte header).
	//   REGISTER body = type[1] | series_id[8] | labels_len[4] | labels_blob
	//   EVICT    body = type[1] | series_id[8] | ts_unix_ms[8]
	catalogRegisterHeader = 1 + 8 + 4
	catalogEvictBody      = 1 + 8 + 8
)

var catalogCRCTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCatalogLogCorrupt is returned when a record fails integrity checks.
var ErrCatalogLogCorrupt = errors.New("catalog log: record CRC mismatch")

// ErrCatalogLogTorn is returned for a partial record at EOF.
var ErrCatalogLogTorn = errors.New("catalog log: torn record at EOF")

type catalogRecord struct {
	typ      byte
	seriesID uint64
	labels   model.LabelSet // REGISTER only
	ts       int64          // EVICT only
}

// encodeRegister returns the on-disk bytes for a REGISTER record.
func encodeRegister(seriesID uint64, labels model.LabelSet) []byte {
	body := encodeLabels(labels)
	total := catalogHeaderLen + 1 + 8 + 4 + len(body)
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(total))
	// crc filled at end
	out[8] = catalogRecordRegister
	binary.LittleEndian.PutUint64(out[9:17], seriesID)
	binary.LittleEndian.PutUint32(out[17:21], uint32(len(body)))
	copy(out[21:], body)
	crc := crc32.Checksum(out[0:4], catalogCRCTable)
	crc = crc32.Update(crc, catalogCRCTable, out[8:])
	binary.LittleEndian.PutUint32(out[4:8], crc)
	return out
}

// encodeEvict returns the on-disk bytes for an EVICT record.
// ts is the wall-clock time at which the sweep evicted the series.
func encodeEvict(seriesID uint64, ts int64) []byte {
	total := catalogHeaderLen + 1 + 8 + 8
	out := make([]byte, total)
	binary.LittleEndian.PutUint32(out[0:4], uint32(total))
	out[8] = catalogRecordEvict
	binary.LittleEndian.PutUint64(out[9:17], seriesID)
	binary.LittleEndian.PutUint64(out[17:25], uint64(ts))
	crc := crc32.Checksum(out[0:4], catalogCRCTable)
	crc = crc32.Update(crc, catalogCRCTable, out[8:])
	binary.LittleEndian.PutUint32(out[4:8], crc)
	return out
}

// encodeLabels serializes a canonical LabelSet.
func encodeLabels(labels model.LabelSet) []byte {
	size := 0
	for _, l := range labels {
		size += 2 + len(l.Name) + 2 + len(l.Value)
	}
	out := make([]byte, size)
	off := 0
	for _, l := range labels {
		binary.LittleEndian.PutUint16(out[off:off+2], uint16(len(l.Name)))
		off += 2
		copy(out[off:off+len(l.Name)], l.Name)
		off += len(l.Name)
		binary.LittleEndian.PutUint16(out[off:off+2], uint16(len(l.Value)))
		off += 2
		copy(out[off:off+len(l.Value)], l.Value)
		off += len(l.Value)
	}
	return out
}

func decodeLabels(in []byte) (model.LabelSet, error) {
	var out model.LabelSet
	for off := 0; off < len(in); {
		if off+2 > len(in) {
			return nil, fmt.Errorf("catalog log: truncated name length at %d", off)
		}
		nl := int(binary.LittleEndian.Uint16(in[off : off+2]))
		off += 2
		if off+nl > len(in) {
			return nil, fmt.Errorf("catalog log: truncated name at %d (want %d bytes)", off, nl)
		}
		name := string(in[off : off+nl])
		off += nl
		if off+2 > len(in) {
			return nil, fmt.Errorf("catalog log: truncated value length at %d", off)
		}
		vl := int(binary.LittleEndian.Uint16(in[off : off+2]))
		off += 2
		if off+vl > len(in) {
			return nil, fmt.Errorf("catalog log: truncated value at %d (want %d bytes)", off, vl)
		}
		out = append(out, model.Label{Name: name, Value: string(in[off : off+vl])})
		off += vl
	}
	return out, nil
}

// readRecord reads the next record from r. Torn records are reported without
// advancing recovery state; callers truncate at the last known-good offset.
func readRecord(r io.Reader) (catalogRecord, error) {
	header := make([]byte, catalogHeaderLen)
	_, err := io.ReadFull(r, header)
	if err == io.EOF {
		return catalogRecord{}, io.EOF
	}
	if err == io.ErrUnexpectedEOF {
		return catalogRecord{}, ErrCatalogLogTorn
	}
	if err != nil {
		return catalogRecord{}, err
	}
	total := int(binary.LittleEndian.Uint32(header[0:4]))
	wantCRC := binary.LittleEndian.Uint32(header[4:8])
	if total < catalogMinRecordLen || total > catalogMaxRecordLen {
		return catalogRecord{}, fmt.Errorf("%w: invalid record length %d", ErrCatalogLogCorrupt, total)
	}
	body := make([]byte, total-catalogHeaderLen)
	_, err = io.ReadFull(r, body)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return catalogRecord{}, ErrCatalogLogTorn
	}
	if err != nil {
		return catalogRecord{}, err
	}
	gotCRC := crc32.Checksum(header[0:4], catalogCRCTable)
	gotCRC = crc32.Update(gotCRC, catalogCRCTable, body)
	if gotCRC != wantCRC {
		return catalogRecord{}, fmt.Errorf("%w (offset-relative)", ErrCatalogLogCorrupt)
	}
	rec, err := decodeBody(body)
	if err != nil {
		return catalogRecord{}, fmt.Errorf("%w: %v", ErrCatalogLogCorrupt, err)
	}
	return rec, nil
}

func decodeBody(body []byte) (catalogRecord, error) {
	if len(body) < 1 {
		return catalogRecord{}, errors.New("empty body")
	}
	switch body[0] {
	case catalogRecordRegister:
		if len(body) < catalogRegisterHeader {
			return catalogRecord{}, errors.New("register body too short")
		}
		seriesID := binary.LittleEndian.Uint64(body[1:9])
		labelsLen := int(binary.LittleEndian.Uint32(body[9:13]))
		if catalogRegisterHeader+labelsLen != len(body) {
			return catalogRecord{}, fmt.Errorf("register labels_len %d mismatches body size %d", labelsLen, len(body)-catalogRegisterHeader)
		}
		labels, err := decodeLabels(body[catalogRegisterHeader:])
		if err != nil {
			return catalogRecord{}, err
		}
		return catalogRecord{typ: catalogRecordRegister, seriesID: seriesID, labels: labels}, nil
	case catalogRecordEvict:
		if len(body) != catalogEvictBody {
			return catalogRecord{}, fmt.Errorf("evict body length %d != %d", len(body), catalogEvictBody)
		}
		seriesID := binary.LittleEndian.Uint64(body[1:9])
		ts := int64(binary.LittleEndian.Uint64(body[9:17]))
		return catalogRecord{typ: catalogRecordEvict, seriesID: seriesID, ts: ts}, nil
	case catalogRecordTouch:
		return catalogRecord{}, errors.New("touch record type not implemented in v0")
	default:
		return catalogRecord{}, fmt.Errorf("unknown record type 0x%02x", body[0])
	}
}
