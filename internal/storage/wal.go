package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	walMagic    = uint32(0xABCD1234)
	walFileName = "amber.wal"

	walHeaderSize = 12

	// maxWALRecordBytes caps the per-record payload size accepted by Replay.
	// Guards against OOM on a corrupt WAL whose length field decodes to garbage
	// before we have a chance to verify the CRC.
	maxWALRecordBytes = 64 << 20
)

var (
	ErrWALCorrupted = errors.New("wal: corrupted record")
	ErrWALBadMagic  = errors.New("wal: bad magic bytes")
	ErrWALBadCRC    = errors.New("wal: crc32 mismatch")
)

type WALRecord struct {
	Payload []byte
}

type WAL struct {
	mu           sync.Mutex
	file         *os.File
	buf          *bufio.Writer
	path         string
	log          *slog.Logger
	corruptCount atomic.Uint64
}

// SetLogger attaches a logger used to surface WAL replay corruption events.
// Safe to call once after construction; not safe for concurrent use with replay.
func (w *WAL) SetLogger(log *slog.Logger) {
	if log != nil {
		w.log = log
	}
}

// CorruptRecords returns the number of malformed records observed during the
// most recent (or any prior) Replay. Useful for surfacing as a metric.
func (w *WAL) CorruptRecords() uint64 {
	return w.corruptCount.Load()
}

func OpenWAL(dir string) (*WAL, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("wal: mkdir %s: %w", dir, err)
	}

	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	return &WAL{
		file: f,
		buf:  bufio.NewWriterSize(f, 64*1024),
		path: path,
		log:  slog.Default(),
	}, nil
}

func (w *WAL) Write(payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writeRecord(payload); err != nil {
		return err
	}

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}

	return nil
}

func (w *WAL) WriteBatch(payloads [][]byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, payload := range payloads {
		if err := w.writeRecord(payload); err != nil {
			return err
		}
	}

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: batch flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: batch sync: %w", err)
	}

	return nil
}

func (w *WAL) writeRecord(payload []byte) error {
	crc := crc32.ChecksumIEEE(payload)
	length := uint32(len(payload))

	var header [walHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], walMagic)
	binary.LittleEndian.PutUint32(header[4:8], crc)
	binary.LittleEndian.PutUint32(header[8:12], length)

	if _, err := w.buf.Write(header[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}

	if _, err := w.buf.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}

	return nil
}

func (w *WAL) Replay(fn func(payload []byte) error) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("wal: replay seek: %w", err)
	}

	r := bufio.NewReader(w.file)
	count := 0

	for {
		var header [walHeaderSize]byte
		_, err := io.ReadFull(r, header[:])
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return count, fmt.Errorf("wal: replay read header: %w", err)
		}

		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != walMagic {
			w.corruptCount.Add(1)
			w.log.Warn("wal: bad magic, stopping replay", "offset_records", count)
			break
		}

		expectedCRC := binary.LittleEndian.Uint32(header[4:8])
		length := binary.LittleEndian.Uint32(header[8:12])

		if length > maxWALRecordBytes {
			w.corruptCount.Add(1)
			w.log.Warn("wal: record length exceeds cap, stopping replay",
				"length", length, "cap", uint32(maxWALRecordBytes), "offset_records", count)
			break
		}

		payload := make([]byte, length)
		_, err = io.ReadFull(r, payload)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		}
		if err != nil {
			return count, fmt.Errorf("wal: replay read payload: %w", err)
		}

		actualCRC := crc32.ChecksumIEEE(payload)
		if actualCRC != expectedCRC {
			w.corruptCount.Add(1)
			w.log.Warn("wal: crc mismatch, stopping replay", "offset_records", count)
			break
		}

		if err := fn(payload); err != nil {
			return count, fmt.Errorf("wal: replay handler: %w", err)
		}

		count++
	}

	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return count, fmt.Errorf("wal: replay seek to end: %w", err)
	}

	return count, nil
}

func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: truncate flush: %w", err)
	}

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}

	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: truncate seek: %w", err)
	}

	w.buf.Reset(w.file)

	return nil
}

func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("wal: stat: %w", err)
	}
	return info.Size(), nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("wal: close flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: close sync: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: close: %w", err)
	}

	return nil
}
