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

func Replay(path string, fn func(Record) error) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer file.Close()

	var header [8]byte
	for {
		_, err := io.ReadFull(file, header[:])
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		}
		if err != nil {
			return err
		}

		n := binary.LittleEndian.Uint32(header[0:4])
		wantCRC := binary.LittleEndian.Uint32(header[4:8])
		if n > maxRecordSize {
			return errors.New("wal: record too large")
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(file, payload); errors.Is(err, io.ErrUnexpectedEOF) {
			return nil
		} else if err != nil {
			return err
		}
		if crc32.ChecksumIEEE(payload) != wantCRC {
			return errors.New("wal: checksum mismatch")
		}
		var record Record
		if err := json.Unmarshal(payload, &record); err != nil {
			return err
		}
		if err := fn(record); err != nil {
			return err
		}
	}
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
