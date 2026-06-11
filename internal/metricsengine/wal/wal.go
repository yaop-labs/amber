package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

const maxRecordSize = 16 << 20

// RecordKind discriminates the binary WAL record types.
type RecordKind uint8

const (
	// KindSeries declares id → labels. Written once per series per WAL
	// generation (between truncates), so samples don't repeat the label set.
	KindSeries RecordKind = 1
	// KindSample is one (id, type, timestamp, value) point.
	KindSample RecordKind = 2
	// KindLegacySample is produced only by the decoder for records written
	// by the pre-binary JSON format: a sample carrying its own labels.
	KindLegacySample RecordKind = 3
	// KindSketchExp / KindSketchExplicit carry one histogram tick:
	// (id, ts, opaque sketch payload). The payload codec lives in the
	// histogram package; the WAL treats it as bytes.
	KindSketchExp      RecordKind = 4
	KindSketchExplicit RecordKind = 5
)

// Record is the logical WAL record. Which fields are meaningful depends on
// Kind: Series uses ID+Labels; Sample uses ID+Type+Timestamp+Value;
// LegacySample uses Labels+Type+Timestamp+Value.
type Record struct {
	Kind      RecordKind
	ID        uint64
	Labels    model.LabelSet
	Type      model.MetricType
	Timestamp int64
	Value     int64
	// Payload carries the encoded sketch for KindSketchExp/KindSketchExplicit.
	Payload []byte
}

// legacyRecord mirrors the retired JSON format for decoding old WAL files.
type legacyRecord struct {
	Labels    model.LabelSet   `json:"labels"`
	Type      model.MetricType `json:"type"`
	Timestamp int64            `json:"timestamp"`
	Value     int64            `json:"value"`
}

type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File

	// failed fail-stops the writer. After a failed fsync the kernel may have
	// dropped the dirty pages without retrying (PostgreSQL's "fsyncgate"), and
	// after a partial write the tail is torn — replay stops there, so records
	// appended past it would be silently lost. Either way the durable state
	// is unknowable; no further append may be acknowledged. Guarded by mu.
	failed error
}

// failStop records a fatal writer error; every subsequent append returns it.
// Caller holds w.mu.
func (w *WAL) failStop(err error) error {
	if w.failed == nil {
		w.failed = err
	}
	return err
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &WAL{path: path, file: file}, nil
}

func (w *WAL) Append(record Record) error {
	return w.AppendBatch([]Record{record})
}

func (w *WAL) AppendBatch(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	var batch []byte
	for _, record := range records {
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		batch = append(batch, encoded...)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	if _, err := w.file.Write(batch); err != nil {
		return w.failStop(err)
	}
	if err := w.file.Sync(); err != nil {
		return w.failStop(err)
	}
	return nil
}

// AppendBatchUnsynced writes records without fsync.
// The caller must call Sync before treating the records as durable.
func (w *WAL) AppendBatchUnsynced(records []Record) error {
	if len(records) == 0 {
		return nil
	}
	var batch []byte
	for _, record := range records {
		encoded, err := encodeRecord(record)
		if err != nil {
			return err
		}
		batch = append(batch, encoded...)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	if _, err := w.file.Write(batch); err != nil {
		return w.failStop(err)
	}
	return nil
}

// Sync flushes any pending writes to disk. Paired with AppendBatchUnsynced
// for group commit: many unsynced writes followed by one Sync.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	if w.file == nil {
		return nil
	}
	if err := w.file.Sync(); err != nil {
		return w.failStop(err)
	}
	return nil
}

// Binary payload layouts (framing — len[4]|crc[4] — is unchanged, so the
// tolerant replay/repair logic applies to both formats):
//
//	series: kind[1] | uvarint id | uvarint nLabels | (uvarint len|name, uvarint len|value)*
//	sample: kind[1] | uvarint id | type[1] | zigzag-varint ts | zigzag-varint value
//
// Legacy JSON payloads start with '{' (0x7B), which no RecordKind uses, so
// the decoder routes on the first byte and mixed-format files replay fine.
func encodeRecord(record Record) ([]byte, error) {
	var payload []byte
	switch record.Kind {
	case KindSeries:
		payload = appendUvarint([]byte{byte(KindSeries)}, record.ID)
		payload = appendUvarint(payload, uint64(len(record.Labels)))
		for _, label := range record.Labels {
			payload = appendString(payload, label.Name)
			payload = appendString(payload, label.Value)
		}
	case KindSample:
		payload = appendUvarint([]byte{byte(KindSample)}, record.ID)
		payload = append(payload, byte(record.Type))
		payload = appendZigZag(payload, record.Timestamp)
		payload = appendZigZag(payload, record.Value)
	case KindSketchExp, KindSketchExplicit:
		payload = appendUvarint([]byte{byte(record.Kind)}, record.ID)
		payload = appendZigZag(payload, record.Timestamp)
		payload = append(payload, record.Payload...)
	default:
		return nil, errors.New("wal: unknown record kind")
	}
	if len(payload) > maxRecordSize {
		return nil, errors.New("wal: record too large")
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	return append(header[:], payload...), nil
}

func decodeRecord(payload []byte) (Record, error) {
	if len(payload) == 0 {
		return Record{}, errors.New("wal: empty record payload")
	}
	if payload[0] == '{' {
		var legacy legacyRecord
		if err := json.Unmarshal(payload, &legacy); err != nil {
			return Record{}, err
		}
		return Record{
			Kind:      KindLegacySample,
			Labels:    legacy.Labels,
			Type:      legacy.Type,
			Timestamp: legacy.Timestamp,
			Value:     legacy.Value,
		}, nil
	}
	r := byteReader{buf: payload[1:]}
	switch RecordKind(payload[0]) {
	case KindSeries:
		rec := Record{Kind: KindSeries}
		rec.ID = r.uvarint()
		n := r.uvarint()
		if n > uint64(maxRecordSize) {
			return Record{}, errors.New("wal: series record label count out of range")
		}
		labels := make(model.LabelSet, 0, n)
		for i := uint64(0); i < n; i++ {
			labels = append(labels, model.Label{Name: r.str(), Value: r.str()})
		}
		rec.Labels = labels
		if r.err != nil || r.remaining() != 0 {
			return Record{}, errors.New("wal: malformed series record")
		}
		return rec, nil
	case KindSample:
		rec := Record{Kind: KindSample}
		rec.ID = r.uvarint()
		rec.Type = model.MetricType(r.byte())
		rec.Timestamp = r.zigzag()
		rec.Value = r.zigzag()
		if r.err != nil || r.remaining() != 0 {
			return Record{}, errors.New("wal: malformed sample record")
		}
		return rec, nil
	case KindSketchExp, KindSketchExplicit:
		rec := Record{Kind: RecordKind(payload[0])}
		rec.ID = r.uvarint()
		rec.Timestamp = r.zigzag()
		if r.err != nil {
			return Record{}, errors.New("wal: malformed sketch record")
		}
		rec.Payload = append([]byte(nil), r.buf...)
		return rec, nil
	default:
		return Record{}, errors.New("wal: unknown record kind")
	}
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendZigZag(buf []byte, v int64) []byte {
	return appendUvarint(buf, uint64(v<<1)^uint64(v>>63))
}

func appendString(buf []byte, s string) []byte {
	buf = appendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

type byteReader struct {
	buf []byte
	err error
}

func (r *byteReader) remaining() int { return len(r.buf) }

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.err = errors.New("wal: bad uvarint")
		return 0
	}
	r.buf = r.buf[n:]
	return v
}

func (r *byteReader) zigzag() int64 {
	v := r.uvarint()
	return int64(v>>1) ^ -int64(v&1)
}

func (r *byteReader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.buf) == 0 {
		r.err = errors.New("wal: short record")
		return 0
	}
	b := r.buf[0]
	r.buf = r.buf[1:]
	return b
}

func (r *byteReader) str() string {
	n := r.uvarint()
	if r.err != nil {
		return ""
	}
	if n > uint64(len(r.buf)) {
		r.err = errors.New("wal: string length out of range")
		return ""
	}
	s := string(r.buf[:n])
	r.buf = r.buf[n:]
	return s
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// A failed writer may hold acked records the disk never got; truncating
	// would destroy the only copy. Refuse and keep the evidence on disk.
	if w.failed != nil {
		return w.failed
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	file, err := os.OpenFile(w.path, os.O_CREATE|os.O_TRUNC|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w.file = file
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	err := w.file.Close()
	w.file = nil
	return err
}

// RecoverStats reports what a replay found on disk.
type RecoverStats struct {
	// Records is the number of valid records replayed.
	Records int
	// TruncatedBytes is the size of the corrupt or torn tail that was
	// dropped (and, for RecoverReplay, physically truncated). 0 means the
	// file ended cleanly.
	TruncatedBytes int64
}

// Replay calls fn for every valid record in order. A corrupted or torn tail
// (short header, short payload, oversized length, CRC mismatch, undecodable
// payload) stops the replay without error: everything before it is the
// durable prefix, everything after it is unreachable garbage from a crash
// mid-write. Only handler errors and I/O errors are returned.
func Replay(path string, fn func(Record) error) error {
	_, err := replayValid(path, fn, false)
	return err
}

// RecoverReplay is Replay plus in-place repair: when a corrupt or torn tail
// is detected, the file is truncated back to the end of the last valid
// record and fsynced, so records appended afterwards stay reachable by
// future replays. Use this (not Replay) before opening the WAL for append.
func RecoverReplay(path string, fn func(Record) error) (RecoverStats, error) {
	return replayValid(path, fn, true)
}

func replayValid(path string, fn func(Record) error, repair bool) (RecoverStats, error) {
	var stats RecoverStats

	flag := os.O_RDONLY
	if repair {
		flag = os.O_RDWR
	}
	file, err := os.OpenFile(path, flag, 0o644)
	if errors.Is(err, os.ErrNotExist) {
		return stats, nil
	}
	if err != nil {
		return stats, err
	}
	defer file.Close()

	// validEnd is the offset just past the last successfully replayed record.
	var validEnd int64
	corrupt := false

	var header [8]byte
	for {
		_, err := io.ReadFull(file, header[:])
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			corrupt = true
			break
		}
		if err != nil {
			return stats, err
		}

		n := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])
		if n > maxRecordSize {
			// Garbage length: the header itself is corrupt.
			corrupt = true
			break
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(file, payload); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			corrupt = true
			break
		} else if err != nil {
			return stats, err
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			corrupt = true
			break
		}
		record, err := decodeRecord(payload)
		if err != nil {
			// CRC matched but the payload does not decode: still treat as
			// corruption rather than failing open — the alternative is a
			// store that cannot start without manual surgery.
			corrupt = true
			break
		}
		if err := fn(record); err != nil {
			return stats, err
		}
		stats.Records++
		validEnd += int64(len(header)) + int64(n)
	}

	if !corrupt {
		return stats, nil
	}

	info, err := file.Stat()
	if err != nil {
		return stats, err
	}
	stats.TruncatedBytes = info.Size() - validEnd

	if repair && stats.TruncatedBytes > 0 {
		if err := file.Truncate(validEnd); err != nil {
			return stats, errors.Join(errors.New("wal: truncate corrupt tail"), err)
		}
		if err := file.Sync(); err != nil {
			return stats, errors.Join(errors.New("wal: sync after truncate"), err)
		}
	}
	return stats, nil
}

func HasRecords(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Size() > 0, nil
}
