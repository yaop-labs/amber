// Package wal implements amber's shared segmented write-ahead log.
// The WAL is payload-agnostic: storage consumers provide a stream ID and an
// opaque payload. Sequence allocation, framing, sync, recovery, checkpoints,
// and segment reclamation live here so logs, spans, and metrics share exactly
// one durability implementation.
package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

const (
	segmentMagic        = uint32(0x41574C32) // "AWL2"
	segmentVersion      = uint16(1)
	recordMagic         = uint32(0x41575231) // "AWR1"
	recordVersion       = uint8(1)
	headerSize          = 28 // magic(4) version(1) stream(1) flags(2) seq(8) len(4) crc(4) reserved(4)
	segmentHeaderSize   = 16 // magic(4) version(2) reserved(2) segmentID(8)
	maxRecordSize       = 64 << 20
	defaultSegmentBytes = 64 << 20
	streamCount         = 3
	checkpointMagic     = uint32(0x41434b31) // "ACK1"
	checkpointVersion   = uint16(1)
)

type Stream uint8

const (
	StreamLog Stream = iota + 1
	StreamSpan
	StreamMetrics
)

func (s Stream) String() string {
	switch s {
	case StreamLog:
		return "log"
	case StreamSpan:
		return "span"
	case StreamMetrics:
		return "metrics"
	default:
		return "unknown(" + strconv.Itoa(int(s)) + ")"
	}
}

func validStream(s Stream) bool { return s >= StreamLog && s <= StreamMetrics }

var (
	ErrCorrupt        = errors.New("wal: corrupted record")
	ErrBadMagic       = errors.New("wal: bad magic")
	ErrBadCRC         = errors.New("wal: bad crc")
	ErrRecordTooLarge = errors.New("wal: record too large")
	ErrClosed         = errors.New("wal: closed")
)

type RecoverStats struct {
	Records        int
	TruncatedBytes int64
	CorruptRecords uint64
}

type Checkpoints struct {
	Log     uint64
	Span    uint64
	Metrics uint64
}

func (c Checkpoints) Get(s Stream) uint64 {
	switch s {
	case StreamLog:
		return c.Log
	case StreamSpan:
		return c.Span
	case StreamMetrics:
		return c.Metrics
	default:
		return 0
	}
}

func (c *Checkpoints) Set(s Stream, seq uint64) {
	switch s {
	case StreamLog:
		c.Log = seq
	case StreamSpan:
		c.Span = seq
	case StreamMetrics:
		c.Metrics = seq
	}
}

type Options struct {
	SegmentBytes int64
}

type WAL struct {
	mu             sync.Mutex
	dir            string
	segmentBytes   int64
	file           *os.File
	buf            *bufio.Writer
	segmentID      uint64
	segmentSize    int64
	nextSeq        uint64
	lastWritten    uint64
	failed         error
	closed         bool
	checkpoints    Checkpoints
	maxSeq         [streamCount + 1]uint64
	segmentStartID uint64
	segmentMax     [streamCount + 1]uint64
	corrupt        atomic.Uint64
}

func Open(dir string, opts Options) (*WAL, error) {
	if dir == "" {
		return nil, errors.New("wal: dir required")
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = defaultSegmentBytes
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	w := &WAL{dir: dir, segmentBytes: opts.SegmentBytes, nextSeq: 1}
	if err := w.loadCheckpoints(); err != nil {
		return nil, err
	}
	if err := w.recoverSegments(); err != nil {
		return nil, err
	}
	if err := w.openAppend(); err != nil {
		return nil, err
	}
	return w, nil
}

func (w *WAL) failStop(err error) error {
	if w.failed == nil {
		w.failed = err
	}
	return w.failed
}

func (w *WAL) Checkpoints() Checkpoints { w.mu.Lock(); defer w.mu.Unlock(); return w.checkpoints }
func (w *WAL) LastWrittenSeq() uint64   { return atomic.LoadUint64(&w.lastWritten) }

// LastStreamSeq returns the highest sequence written for stream. Unlike
// LastWrittenSeq, it does not include records belonging to other streams.
func (w *WAL) LastStreamSeq(stream Stream) uint64 {
	if !validStream(stream) {
		return 0
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.maxSeq[stream]
}

func (w *WAL) CorruptRecords() uint64 { return w.corrupt.Load() }

// HasStreamRecords reports whether at least one valid record exists for stream.
func (w *WAL) HasStreamRecords(stream Stream) (bool, error) {
	if !validStream(stream) {
		return false, fmt.Errorf("wal: invalid stream %d", stream)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	files, err := w.segmentFiles()
	if err != nil {
		return false, err
	}
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			return false, err
		}
		if err := validateSegmentHeader(f); err != nil {
			f.Close()
			return false, err
		}
		for {
			var h [headerSize]byte
			_, err := io.ReadFull(f, h[:])
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				f.Close()
				return false, err
			}
			rs := Stream(h[5])
			n := binary.LittleEndian.Uint32(h[16:20])
			if n > maxRecordSize {
				f.Close()
				return false, ErrRecordTooLarge
			}
			payload := make([]byte, int(n))
			if _, err := io.ReadFull(f, payload); err != nil {
				f.Close()
				return false, err
			}
			if rs == stream {
				f.Close()
				return true, nil
			}
		}
		_ = f.Close()
	}
	return false, nil
}

func (w *WAL) Bytes() (int64, error) {
	ents, err := os.ReadDir(w.dir)
	if err != nil {
		return 0, err
	}
	var n int64
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".awl") {
			info, err := e.Info()
			if err != nil {
				return 0, err
			}
			n += info.Size()
		}
	}
	return n, nil
}

func (w *WAL) Append(stream Stream, payload []byte) (uint64, error) {
	return w.AppendBatch(stream, [][]byte{payload})
}

func (w *WAL) AppendBatch(stream Stream, payloads [][]byte) (uint64, error) {
	return w.appendBatch(stream, payloads, true)
}

func (w *WAL) AppendBatchUnsynced(stream Stream, payloads [][]byte) (uint64, error) {
	return w.appendBatch(stream, payloads, false)
}

func (w *WAL) appendBatch(stream Stream, payloads [][]byte, syncNow bool) (uint64, error) {
	if !validStream(stream) {
		return 0, fmt.Errorf("wal: invalid stream %d", stream)
	}
	if len(payloads) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	if w.failed != nil {
		return 0, w.failed
	}
	for _, p := range payloads {
		if len(p) > maxRecordSize {
			return 0, ErrRecordTooLarge
		}
	}
	first := w.nextSeq
	for _, p := range payloads {
		if w.segmentSize > 0 && w.segmentSize+int64(headerSize)+int64(len(p)) > w.segmentBytes && w.segmentSize > segmentHeaderSize {
			if err := w.rotateLocked(); err != nil {
				return 0, w.failStop(err)
			}
		}
		seq := w.nextSeq
		if err := w.writeFrameLocked(stream, seq, p); err != nil {
			return 0, w.failStop(err)
		}
		w.nextSeq++
		w.lastWritten = seq
		if seq > w.maxSeq[stream] {
			w.maxSeq[stream] = seq
		}
		w.segmentMax[stream] = seq
	}
	if syncNow {
		if err := w.syncLocked(); err != nil {
			return 0, err
		}
	}
	return first, nil
}

func (w *WAL) writeFrameLocked(stream Stream, seq uint64, payload []byte) error {
	var h [headerSize]byte
	binary.LittleEndian.PutUint32(h[0:4], recordMagic)
	h[4] = recordVersion
	h[5] = byte(stream)
	binary.LittleEndian.PutUint16(h[6:8], 0)
	binary.LittleEndian.PutUint64(h[8:16], seq)
	binary.LittleEndian.PutUint32(h[16:20], uint32(len(payload)))
	crc := crc32.Update(0, crc32.IEEETable, h[4:20])
	crc = crc32.Update(crc, crc32.IEEETable, payload)
	binary.LittleEndian.PutUint32(h[20:24], crc)
	if _, err := w.buf.Write(h[:]); err != nil {
		return fmt.Errorf("wal: write header: %w", err)
	}
	if _, err := w.buf.Write(payload); err != nil {
		return fmt.Errorf("wal: write payload: %w", err)
	}
	w.segmentSize += int64(headerSize) + int64(len(payload))
	return nil
}

func (w *WAL) syncLocked() error {
	if err := w.buf.Flush(); err != nil {
		return w.failStop(fmt.Errorf("wal: flush: %w", err))
	}
	if err := w.file.Sync(); err != nil {
		return w.failStop(fmt.Errorf("wal: sync: %w", err))
	}
	return nil
}

func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.failed != nil {
		return w.failed
	}
	return w.syncLocked()
}

func (w *WAL) Replay(stream Stream, fn func(seq uint64, payload []byte) error) (RecoverStats, error) {
	if !validStream(stream) {
		return RecoverStats{}, fmt.Errorf("wal: invalid stream %d", stream)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	var stats RecoverStats
	files, err := w.segmentFiles()
	if err != nil {
		return stats, err
	}
	var maxSeq uint64
	for _, path := range files {
		f, err := os.OpenFile(path, os.O_RDWR, 0o600)
		if err != nil {
			return stats, err
		}
		good := int64(0)
		corrupt := false
		if info, err := f.Stat(); err != nil {
			f.Close()
			return stats, err
		} else {
			_ = info
		}
		// validate segment header first
		if err := validateSegmentHeader(f); err != nil {
			f.Close()
			return stats, err
		}
		good = segmentHeaderSize
		for {
			var h [headerSize]byte
			_, err := io.ReadFull(f, h[:])
			if errors.Is(err, io.EOF) {
				break
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				corrupt = true
				break
			}
			if err != nil {
				f.Close()
				return stats, err
			}
			if binary.LittleEndian.Uint32(h[0:4]) != recordMagic || h[4] != recordVersion {
				f.Close()
				return stats, fmt.Errorf("%w at %s+%d", ErrBadMagic, filepath.Base(path), good)
			}
			rs := Stream(h[5])
			if !validStream(rs) {
				f.Close()
				return stats, fmt.Errorf("%w: stream=%d", ErrCorrupt, h[5])
			}
			seq := binary.LittleEndian.Uint64(h[8:16])
			n := binary.LittleEndian.Uint32(h[16:20])
			want := binary.LittleEndian.Uint32(h[20:24])
			if n > maxRecordSize {
				f.Close()
				return stats, fmt.Errorf("%w: len=%d", ErrRecordTooLarge, n)
			}
			p := make([]byte, int(n))
			_, err = io.ReadFull(f, p)
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				corrupt = true
				break
			}
			if err != nil {
				f.Close()
				return stats, err
			}
			crc := crc32.Update(0, crc32.IEEETable, h[4:20])
			crc = crc32.Update(crc, crc32.IEEETable, p)
			if crc != want {
				f.Close()
				return stats, fmt.Errorf("%w at %s+%d", ErrBadCRC, filepath.Base(path), good)
			}
			if seq > maxSeq {
				maxSeq = seq
			}
			if rs == stream {
				if err := fn(seq, p); err != nil {
					f.Close()
					return stats, err
				}
				stats.Records++
			}
			good += int64(headerSize) + int64(n)
		}
		if corrupt {
			info, err := f.Stat()
			if err != nil {
				f.Close()
				return stats, err
			}
			stats.TruncatedBytes += info.Size() - good
			stats.CorruptRecords++
			w.corrupt.Add(1)
			if err := f.Truncate(good); err != nil {
				f.Close()
				return stats, err
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return stats, err
			}
		}
		f.Close()
	}
	if maxSeq >= w.nextSeq {
		w.nextSeq = maxSeq + 1
	}
	return stats, nil
}

func validateSegmentHeader(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var h [segmentHeaderSize]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(h[0:4]) != segmentMagic || binary.LittleEndian.Uint16(h[4:6]) != segmentVersion {
		return fmt.Errorf("%w: segment", ErrBadMagic)
	}
	return nil
}

func (w *WAL) Checkpoint(stream Stream, seq uint64) error {
	if !validStream(stream) {
		return fmt.Errorf("wal: invalid stream %d", stream)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.failed != nil {
		return w.failed
	}
	if seq > w.maxSeq[stream] {
		return fmt.Errorf("wal: checkpoint %d for %s beyond last stream seq %d", seq, stream, w.maxSeq[stream])
	}
	if seq <= w.checkpoints.Get(stream) {
		return nil
	}
	w.checkpoints.Set(stream, seq)
	if err := w.persistCheckpointsLocked(); err != nil {
		return w.failStop(err)
	}
	return w.reclaimLocked()
}

func (w *WAL) reclaimLocked() error {
	files, err := w.segmentFiles()
	if err != nil {
		return err
	}
	for _, path := range files {
		id, err := parseSegmentID(filepath.Base(path))
		if err != nil {
			return err
		}
		if id == w.segmentID {
			continue
		}
		maxs, err := segmentMaxSeqs(path)
		if err != nil {
			return err
		}
		safe := true
		for s := StreamLog; s <= StreamMetrics; s++ {
			if maxs[s] != 0 && w.checkpoints.Get(s) < maxs[s] {
				safe = false
				break
			}
		}
		if safe {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

func segmentMaxSeqs(path string) ([streamCount + 1]uint64, error) {
	var out [streamCount + 1]uint64
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	if err := validateSegmentHeader(f); err != nil {
		return out, err
	}
	for {
		var h [headerSize]byte
		_, err := io.ReadFull(f, h[:])
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		if binary.LittleEndian.Uint32(h[0:4]) != recordMagic || h[4] != recordVersion {
			return out, ErrCorrupt
		}
		rs := Stream(h[5])
		if !validStream(rs) {
			return out, fmt.Errorf("wal: invalid stream %d", rs)
		}
		if binary.LittleEndian.Uint16(h[6:8]) != 0 || binary.LittleEndian.Uint32(h[24:28]) != 0 {
			return out, ErrCorrupt
		}
		n := binary.LittleEndian.Uint32(h[16:20])
		if n > maxRecordSize {
			return out, ErrRecordTooLarge
		}
		p := make([]byte, int(n))
		if _, err := io.ReadFull(f, p); err != nil {
			return out, err
		}
		crc := crc32.New(crc32.IEEETable)
		_, _ = crc.Write(h[4:20])
		_, _ = crc.Write(p)
		if binary.LittleEndian.Uint32(h[20:24]) != crc.Sum32() {
			return out, ErrCorrupt
		}
		seq := binary.LittleEndian.Uint64(h[8:16])
		if seq > out[rs] {
			out[rs] = seq
		}
	}
}

func (w *WAL) Rotate() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrClosed
	}
	if w.failed != nil {
		return w.failed
	}
	if w.segmentSize <= segmentHeaderSize {
		return nil
	}
	if err := w.syncLocked(); err != nil {
		return err
	}
	return w.rotateLocked()
}

func (w *WAL) rotateLocked() error {
	if err := w.syncLocked(); err != nil {
		return err
	}
	if err := w.file.Close(); err != nil {
		return err
	}
	w.segmentID++
	path := filepath.Join(w.dir, segmentFileName(w.segmentID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	var h [segmentHeaderSize]byte
	binary.LittleEndian.PutUint32(h[0:4], segmentMagic)
	binary.LittleEndian.PutUint16(h[4:6], segmentVersion)
	binary.LittleEndian.PutUint64(h[8:16], w.segmentID)
	if _, err := f.Write(h[:]); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, 64*1024)
	w.segmentSize = segmentHeaderSize
	w.segmentMax = [streamCount + 1]uint64{}
	w.segmentStartID = w.segmentID
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	if w.file == nil {
		w.closed = true
		return nil
	}
	if w.failed == nil {
		if err := w.syncLocked(); err != nil {
			w.failed = err
		}
	}
	err := w.file.Close()
	w.closed = true
	return err
}

func (w *WAL) segmentFiles() ([]string, error) {
	ents, err := os.ReadDir(w.dir)
	if err != nil {
		return nil, err
	}
	paths := make([]string, 0)
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".awl") {
			paths = append(paths, filepath.Join(w.dir, e.Name()))
		}
	}
	sort.Slice(paths, func(i, j int) bool {
		ai, _ := parseSegmentID(filepath.Base(paths[i]))
		aj, _ := parseSegmentID(filepath.Base(paths[j]))
		return ai < aj
	})
	return paths, nil
}
func segmentFileName(id uint64) string { return fmt.Sprintf("wal-%08d.awl", id) }
func parseSegmentID(name string) (uint64, error) {
	var id uint64
	if _, err := fmt.Sscanf(name, "wal-%08d.awl", &id); err != nil {
		return 0, fmt.Errorf("wal: bad segment name %q", name)
	}
	return id, nil
}

func mustLastSegmentID(files []string) uint64 {
	if len(files) == 0 {
		return 0
	}
	id, _ := parseSegmentID(filepath.Base(files[len(files)-1]))
	return id
}

func (w *WAL) recoverSegments() error {
	files, err := w.segmentFiles()
	if err != nil {
		return err
	}
	if len(files) == 0 {
		w.segmentID = 1
		return nil
	}
	var maxSeq uint64
	var prevSeq uint64
	lastID := uint64(0)
	for _, path := range files {
		id, err := parseSegmentID(filepath.Base(path))
		if err != nil {
			return err
		}
		lastID = id
		infoBefore, err := os.Stat(path)
		if err != nil {
			return err
		}
		last, segMax, lastSeq, err := scanSegment(path, id == mustLastSegmentID(files), prevSeq)
		if infoBefore.Size() > last {
			w.corrupt.Add(1)
		}
		if lastSeq > prevSeq {
			prevSeq = lastSeq
		}
		if err != nil {
			return err
		}
		for st := StreamLog; st <= StreamMetrics; st++ {
			if segMax[st] > w.maxSeq[st] {
				w.maxSeq[st] = segMax[st]
			}
			if segMax[st] > maxSeq {
				maxSeq = segMax[st]
			}
		}
	}
	w.segmentID = lastID
	if maxSeq >= w.nextSeq {
		w.nextSeq = maxSeq + 1
	}
	return nil
}

func scanSegment(path string, isLast bool, prevSeq uint64) (int64, [streamCount + 1]uint64, uint64, error) {
	var out [streamCount + 1]uint64
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		return 0, out, 0, err
	}
	defer f.Close()
	if err := validateSegmentHeader(f); err != nil {
		return 0, out, 0, err
	}
	good := int64(segmentHeaderSize)
	for {
		var h [headerSize]byte
		_, err := io.ReadFull(f, h[:])
		if errors.Is(err, io.EOF) {
			return good, out, prevSeq, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			if !isLast {
				return good, out, prevSeq, fmt.Errorf("wal: truncated non-last segment %s", filepath.Base(path))
			}
			if err := f.Truncate(good); err != nil {
				return good, out, prevSeq, err
			}
			if err := f.Sync(); err != nil {
				return good, out, prevSeq, err
			}
			return good, out, prevSeq, nil
		}
		if err != nil {
			return good, out, prevSeq, err
		}
		if binary.LittleEndian.Uint32(h[0:4]) != recordMagic || h[4] != recordVersion {
			return good, out, prevSeq, fmt.Errorf("%w at %s+%d", ErrBadMagic, filepath.Base(path), good)
		}
		rs := Stream(h[5])
		if !validStream(rs) {
			return good, out, prevSeq, fmt.Errorf("%w: stream=%d", ErrCorrupt, h[5])
		}
		seq := binary.LittleEndian.Uint64(h[8:16])
		n := binary.LittleEndian.Uint32(h[16:20])
		want := binary.LittleEndian.Uint32(h[20:24])
		if n > maxRecordSize {
			return good, out, prevSeq, fmt.Errorf("%w: len=%d", ErrRecordTooLarge, n)
		}
		p := make([]byte, int(n))
		_, err = io.ReadFull(f, p)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			if !isLast {
				return good, out, prevSeq, fmt.Errorf("wal: truncated non-last segment %s", filepath.Base(path))
			}
			if err := f.Truncate(good); err != nil {
				return good, out, prevSeq, err
			}
			if err := f.Sync(); err != nil {
				return good, out, prevSeq, err
			}
			return good, out, prevSeq, nil
		}
		if err != nil {
			return good, out, prevSeq, err
		}
		crc := crc32.Update(0, crc32.IEEETable, h[4:20])
		crc = crc32.Update(crc, crc32.IEEETable, p)
		if crc != want {
			return good, out, prevSeq, fmt.Errorf("%w at %s+%d", ErrBadCRC, filepath.Base(path), good)
		}
		if prevSeq != 0 && seq <= prevSeq {
			return good, out, prevSeq, fmt.Errorf("wal: sequence regression at %s+%d: seq=%d previous=%d", filepath.Base(path), good, seq, prevSeq)
		}
		if seq > out[rs] {
			out[rs] = seq
		}
		prevSeq = seq
		good += int64(headerSize) + int64(n)
	}
}

func (w *WAL) openAppend() error {
	path := filepath.Join(w.dir, segmentFileName(w.segmentID))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return err
	}
	if info.Size() == 0 {
		var h [segmentHeaderSize]byte
		binary.LittleEndian.PutUint32(h[0:4], segmentMagic)
		binary.LittleEndian.PutUint16(h[4:6], segmentVersion)
		binary.LittleEndian.PutUint64(h[8:16], w.segmentID)
		if _, err := f.Write(h[:]); err != nil {
			f.Close()
			return err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		infoSize := int64(segmentHeaderSize)
		w.segmentSize = infoSize
	} else {
		if err := validateSegmentHeader(f); err != nil {
			f.Close()
			return err
		}
		w.segmentSize = info.Size()
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		f.Close()
		return err
	}
	w.file = f
	w.buf = bufio.NewWriterSize(f, 64*1024)
	return nil
}

func checkpointPath(dir string) string { return filepath.Join(dir, "checkpoints.bin") }
func (w *WAL) loadCheckpoints() error {
	data, err := os.ReadFile(checkpointPath(w.dir))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if len(data) != 4+2+6*8 {
		return fmt.Errorf("wal: invalid checkpoints file")
	}
	if binary.LittleEndian.Uint32(data[0:4]) != checkpointMagic || binary.LittleEndian.Uint16(data[4:6]) != checkpointVersion {
		return fmt.Errorf("wal: invalid checkpoints header")
	}
	w.checkpoints.Log = binary.LittleEndian.Uint64(data[6:14])
	w.checkpoints.Span = binary.LittleEndian.Uint64(data[14:22])
	w.checkpoints.Metrics = binary.LittleEndian.Uint64(data[22:30])
	return nil
}

func (w *WAL) persistCheckpointsLocked() error {
	var data [30]byte
	binary.LittleEndian.PutUint32(data[0:4], checkpointMagic)
	binary.LittleEndian.PutUint16(data[4:6], checkpointVersion)
	binary.LittleEndian.PutUint64(data[6:14], w.checkpoints.Log)
	binary.LittleEndian.PutUint64(data[14:22], w.checkpoints.Span)
	binary.LittleEndian.PutUint64(data[22:30], w.checkpoints.Metrics)
	tmp := checkpointPath(w.dir) + ".tmp"
	if err := os.WriteFile(tmp, data[:], 0o600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, checkpointPath(w.dir)); err != nil {
		return err
	}
	d, err := os.Open(w.dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
