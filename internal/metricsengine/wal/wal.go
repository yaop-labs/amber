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

type Record struct {
	Labels    model.LabelSet   `json:"labels"`
	Type      model.MetricType `json:"type"`
	Timestamp int64            `json:"timestamp"`
	Value     int64            `json:"value"`
}

type WAL struct {
	mu   sync.Mutex
	path string
	file *os.File
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
	if _, err := w.file.Write(batch); err != nil {
		return err
	}
	return w.file.Sync()
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
	if _, err := w.file.Write(batch); err != nil {
		return err
	}
	return nil
}

// Sync flushes any pending writes to disk. Paired with AppendBatchUnsynced
// for group commit: many unsynced writes followed by one Sync.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return nil
	}
	return w.file.Sync()
}

func encodeRecord(record Record) ([]byte, error) {
	payload, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRecordSize {
		return nil, errors.New("wal: record too large")
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	return append(header[:], payload...), nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
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
		var record Record
		if err := json.Unmarshal(payload, &record); err != nil {
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
