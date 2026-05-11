package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

var ErrStopScan = errors.New("stop scan")

const (
	segMagic      = uint32(0x414D4252)
	segVersion    = uint16(2)
	segVersionMin = uint16(1)
	segHeaderSize = 16

	blockMagic      = uint32(0x424C4F4B)
	blockHeaderSize = 16

	footerMagic = uint32(0x464F4F54)

	DefaultBlockSize = 4 * 1024 * 1024
)

var (
	ErrSegmentCorrupted = errors.New("segment: corrupted file")
	ErrSegmentBadMagic  = errors.New("segment: bad magic bytes")
	ErrNoFooter         = errors.New("segment: no footer found")
)

type BlockStat struct {
	MinID uint64
	MaxID uint64
}

type SegmentFooter struct {
	MinTS        int64
	MaxTS        int64
	RecordCount  uint64
	BlockCount   uint32
	BlockOffsets []int64
	BlockStats   []BlockStat
}

type SegmentWriter struct {
	mu              sync.Mutex
	file            *os.File
	bw              *bufio.Writer
	encoder         *zstd.Encoder
	blockBuf        bytes.Buffer
	compressedBuf   []byte // reused dst for zstd.EncodeAll across flushes
	blockRecords    uint32
	blockMinID      uint64
	blockMaxID      uint64
	blockHasRecords bool
	minTS           int64
	maxTS           int64
	recordCount     uint64
	blockOffsets    []int64
	blockStats      []BlockStat
	fileOffset      int64

	blockSize int
	closed    bool
}

func OpenSegmentWriter(path string) (*SegmentWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("segment: create %s: %w", path, err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: create zstd encoder: %w", err)
	}

	sw := &SegmentWriter{
		file:      f,
		bw:        bufio.NewWriterSize(f, 256*1024),
		encoder:   enc,
		blockSize: DefaultBlockSize,
		minTS:     0,
		maxTS:     0,
	}

	if err := sw.writeHeader(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: write header: %w", err)
	}

	return sw, nil
}

func (sw *SegmentWriter) writeHeader() error {
	var header [segHeaderSize]byte
	binary.LittleEndian.PutUint32(header[0:4], segMagic)
	binary.LittleEndian.PutUint16(header[4:6], segVersion)
	binary.LittleEndian.PutUint64(header[6:14], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint16(header[14:16], 0)

	n, err := sw.bw.Write(header[:])
	sw.fileOffset += int64(n)
	return err
}

func (sw *SegmentWriter) WriteRecord(data []byte, ts int64) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return fmt.Errorf("segment: writer is closed")
	}

	if sw.recordCount == 0 || ts < sw.minTS {
		sw.minTS = ts
	}
	if ts > sw.maxTS {
		sw.maxTS = ts
	}

	if len(data) >= 10 {
		id := binary.BigEndian.Uint64(data[2:10])
		if !sw.blockHasRecords {
			sw.blockMinID = id
			sw.blockMaxID = id
			sw.blockHasRecords = true
		} else {
			if id < sw.blockMinID {
				sw.blockMinID = id
			}
			if id > sw.blockMaxID {
				sw.blockMaxID = id
			}
		}
	}

	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(data)))
	sw.blockBuf.Write(lenBuf[:])
	sw.blockBuf.Write(data)
	sw.blockRecords++
	sw.recordCount++

	if sw.blockBuf.Len() >= sw.blockSize {
		return sw.flushBlock()
	}

	return nil
}

func (sw *SegmentWriter) flushBlock() error {
	if sw.blockBuf.Len() == 0 {
		return nil
	}

	uncompressed := sw.blockBuf.Bytes()
	uncompressedSize := uint32(len(uncompressed))

	// Reuse compressedBuf capacity across flushes. First flush allocates,
	// subsequent ones append into existing storage. zstd's EncodeAll
	// appends, so [:0] keeps capacity, drops length.
	sw.compressedBuf = sw.encoder.EncodeAll(uncompressed, sw.compressedBuf[:0])
	compressed := sw.compressedBuf
	compressedSize := uint32(len(compressed))

	sw.blockOffsets = append(sw.blockOffsets, sw.fileOffset)
	sw.blockStats = append(sw.blockStats, BlockStat{
		MinID: sw.blockMinID,
		MaxID: sw.blockMaxID,
	})

	var blockHeader [blockHeaderSize]byte
	binary.LittleEndian.PutUint32(blockHeader[0:4], blockMagic)
	binary.LittleEndian.PutUint32(blockHeader[4:8], uncompressedSize)
	binary.LittleEndian.PutUint32(blockHeader[8:12], compressedSize)
	binary.LittleEndian.PutUint32(blockHeader[12:16], sw.blockRecords)

	n, err := sw.bw.Write(blockHeader[:])
	sw.fileOffset += int64(n)
	if err != nil {
		return fmt.Errorf("segment: write block header: %w", err)
	}

	n, err = sw.bw.Write(compressed)
	sw.fileOffset += int64(n)
	if err != nil {
		return fmt.Errorf("segment: write block data: %w", err)
	}

	sw.blockBuf.Reset()
	sw.blockRecords = 0
	sw.blockMinID = 0
	sw.blockMaxID = 0
	sw.blockHasRecords = false

	return nil
}

func (sw *SegmentWriter) Flush() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return nil
	}
	if sw.blockBuf.Len() > 0 {
		if err := sw.flushBlock(); err != nil {
			return err
		}
	}
	return sw.bw.Flush()
}

func (sw *SegmentWriter) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return nil
	}
	sw.closed = true

	if err := sw.flushBlock(); err != nil {
		return err
	}

	if err := sw.writeFooter(); err != nil {
		return fmt.Errorf("segment: write footer: %w", err)
	}

	if err := sw.bw.Flush(); err != nil {
		return fmt.Errorf("segment: flush: %w", err)
	}

	if err := sw.file.Sync(); err != nil {
		return fmt.Errorf("segment: sync: %w", err)
	}

	return sw.file.Close()
}

func (sw *SegmentWriter) writeFooter() error {
	blockCount := uint32(len(sw.blockOffsets))

	footerSize := 8 + 8 + 8 + 4 + blockCount*8 + blockCount*16

	var buf bytes.Buffer

	writeUint64(&buf, uint64(sw.minTS))
	writeUint64(&buf, uint64(sw.maxTS))
	writeUint64(&buf, sw.recordCount)
	writeUint32(&buf, blockCount)
	for _, offset := range sw.blockOffsets {
		writeUint64(&buf, uint64(offset))
	}
	for _, stat := range sw.blockStats {
		writeUint64(&buf, stat.MinID)
		writeUint64(&buf, stat.MaxID)
	}
	writeUint32(&buf, footerSize)
	writeUint32(&buf, footerMagic)

	n, err := sw.bw.Write(buf.Bytes())
	sw.fileOffset += int64(n)
	return err
}

func (sw *SegmentWriter) RecordCount() uint64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.recordCount
}

func (sw *SegmentWriter) TimeRange() (int64, int64) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.minTS, sw.maxTS
}

// BlockCount returns the number of blocks already flushed (compressed and
// handed off to the bufio writer). Used by SegmentManager to detect whether
// the latest WriteRecord triggered a block flush and therefore needs a sync.
func (sw *SegmentWriter) BlockCount() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.blockOffsets)
}

// Sync flushes the bufio writer and fsyncs the underlying file, returning
// the durable file offset. Records still buffered in blockBuf (i.e. not yet
// part of a flushed block) are NOT covered by this sync; only blocks already
// handed off to the bufio writer become durable.
func (sw *SegmentWriter) Sync() (int64, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return sw.fileOffset, nil
	}
	if err := sw.bw.Flush(); err != nil {
		return 0, fmt.Errorf("segment: sync flush: %w", err)
	}
	if err := sw.file.Sync(); err != nil {
		return 0, fmt.Errorf("segment: sync fsync: %w", err)
	}
	return sw.fileOffset, nil
}

type BlockIndexHint struct {
	Offsets     []int64
	Stats       []BlockStat
	MinTS       int64
	MaxTS       int64
	RecordCount uint64
}

func (sw *SegmentWriter) SnapshotBlockIndex() (*BlockIndexHint, bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if len(sw.blockOffsets) == 0 {
		return nil, false
	}

	if err := sw.bw.Flush(); err != nil {
		return nil, false
	}

	hint := &BlockIndexHint{
		Offsets:     append([]int64(nil), sw.blockOffsets...),
		Stats:       append([]BlockStat(nil), sw.blockStats...),
		MinTS:       sw.minTS,
		MaxTS:       sw.maxTS,
		RecordCount: sw.recordCount,
	}
	return hint, true
}

type SegmentReader struct {
	file    *os.File
	decoder *zstd.Decoder
	footer  SegmentFooter
	version uint16
}

func (sr *SegmentReader) ensureDecoder() error {
	if sr.decoder != nil {
		return nil
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("segment: create zstd decoder: %w", err)
	}
	sr.decoder = dec
	return nil
}

func OpenSegmentReader(path string, hint *BlockIndexHint) (*SegmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}

	sr := &SegmentReader{
		file: f,
	}

	if err := sr.readHeader(); err != nil {
		_ = f.Close()
		return nil, err
	}

	if hint != nil {
		sr.footer = SegmentFooter{
			MinTS:        hint.MinTS,
			MaxTS:        hint.MaxTS,
			RecordCount:  hint.RecordCount,
			BlockCount:   uint32(len(hint.Offsets)),
			BlockOffsets: hint.Offsets,
			BlockStats:   hint.Stats,
		}
		return sr, nil
	}

	if err := sr.readFooter(); err != nil {
		if err != ErrNoFooter {
			_ = f.Close()
			return nil, err
		}

		if err2 := sr.scanBlockOffsets(); err2 != nil {
			_ = f.Close()
			return nil, err2
		}
	}

	return sr, nil
}

func (sr *SegmentReader) scanBlockOffsets() error {
	if err := sr.ensureDecoder(); err != nil {
		return err
	}
	if _, err := sr.file.Seek(int64(segHeaderSize), io.SeekStart); err != nil {
		return fmt.Errorf("segment: seek to blocks: %w", err)
	}

	var offsets []int64
	var stats []BlockStat
	pos := int64(segHeaderSize)
	var minTS, maxTS int64
	var seenTS bool
	var totalRecords uint64

	for {
		var header [blockHeaderSize]byte
		_, err := io.ReadFull(sr.file, header[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return fmt.Errorf("segment: scan block header: %w", err)
		}

		magic := binary.LittleEndian.Uint32(header[0:4])
		if magic != blockMagic {
			break
		}

		compressedSize := int64(binary.LittleEndian.Uint32(header[8:12]))
		blockRecords := uint64(binary.LittleEndian.Uint32(header[12:16]))

		offsets = append(offsets, pos)
		totalRecords += blockRecords

		compressed := make([]byte, compressedSize)
		if _, err := io.ReadFull(sr.file, compressed); err != nil {
			break
		}
		uncompressedSize := binary.LittleEndian.Uint32(header[4:8])
		decompressed, err := sr.decoder.DecodeAll(compressed, make([]byte, 0, uncompressedSize))
		var blockStat BlockStat
		var blockHasRecs bool
		if err == nil {
			r := bytes.NewReader(decompressed)
			for r.Len() > 0 {
				var lenBuf [4]byte
				if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
					break
				}
				recLen := binary.LittleEndian.Uint32(lenBuf[:])
				recData := make([]byte, recLen)
				if _, err := io.ReadFull(r, recData); err != nil {
					break
				}
				if len(recData) >= 8 {
					ts := int64(binary.LittleEndian.Uint64(recData[:8]))
					if !seenTS {
						minTS = ts
						maxTS = ts
						seenTS = true
					} else {
						if ts < minTS {
							minTS = ts
						}
						if ts > maxTS {
							maxTS = ts
						}
					}
				}
				if len(recData) >= 10 {
					id := binary.BigEndian.Uint64(recData[2:10])
					if !blockHasRecs {
						blockStat.MinID = id
						blockStat.MaxID = id
						blockHasRecs = true
					} else {
						if id < blockStat.MinID {
							blockStat.MinID = id
						}
						if id > blockStat.MaxID {
							blockStat.MaxID = id
						}
					}
				}
			}
		}
		stats = append(stats, blockStat)

		pos += int64(blockHeaderSize) + compressedSize
	}

	sr.footer = SegmentFooter{
		MinTS:        minTS,
		MaxTS:        maxTS,
		RecordCount:  totalRecords,
		BlockCount:   uint32(len(offsets)),
		BlockOffsets: offsets,
		BlockStats:   stats,
	}
	return nil
}

func (sr *SegmentReader) readHeader() error {
	var header [segHeaderSize]byte
	if _, err := io.ReadFull(sr.file, header[:]); err != nil {
		return fmt.Errorf("segment: read header: %w", err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != segMagic {
		return ErrSegmentBadMagic
	}

	version := binary.LittleEndian.Uint16(header[4:6])
	if version < segVersionMin || version > segVersion {
		return fmt.Errorf("segment: unsupported version %d", version)
	}
	sr.version = version

	return nil
}

func (sr *SegmentReader) readFooter() error {
	if _, err := sr.file.Seek(-8, io.SeekEnd); err != nil {
		return fmt.Errorf("segment: seek to footer tail: %w", err)
	}

	var tail [8]byte
	if _, err := io.ReadFull(sr.file, tail[:]); err != nil {
		return fmt.Errorf("segment: read footer tail: %w", err)
	}

	footerSize := binary.LittleEndian.Uint32(tail[0:4])
	magic := binary.LittleEndian.Uint32(tail[4:8])
	if magic != footerMagic {
		return ErrNoFooter
	}

	totalFooter := int64(footerSize) + 8
	if _, err := sr.file.Seek(-totalFooter, io.SeekEnd); err != nil {
		return fmt.Errorf("segment: seek to footer start: %w", err)
	}

	footerData := make([]byte, footerSize)
	if _, err := io.ReadFull(sr.file, footerData); err != nil {
		return fmt.Errorf("segment: read footer: %w", err)
	}

	r := bytes.NewReader(footerData)
	sr.footer.MinTS = int64(readUint64(r))
	sr.footer.MaxTS = int64(readUint64(r))
	sr.footer.RecordCount = readUint64(r)
	blockCount := readUint32(r)
	sr.footer.BlockCount = blockCount
	sr.footer.BlockOffsets = make([]int64, blockCount)
	for i := uint32(0); i < blockCount; i++ {
		sr.footer.BlockOffsets[i] = int64(readUint64(r))
	}

	if sr.version >= 2 && blockCount > 0 {
		expected := int(blockCount) * 16
		if r.Len() >= expected {
			sr.footer.BlockStats = make([]BlockStat, blockCount)
			for i := uint32(0); i < blockCount; i++ {
				sr.footer.BlockStats[i].MinID = readUint64(r)
				sr.footer.BlockStats[i].MaxID = readUint64(r)
			}
		}
	}

	return nil
}

func (sr *SegmentReader) Footer() SegmentFooter {
	return sr.footer
}

func (sr *SegmentReader) Scan(fn func(data []byte) error) error {
	return sr.scanBlocks(sr.footer.BlockOffsets, fn)
}

func (sr *SegmentReader) ScanWithBlockSkip(
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	stats := sr.footer.BlockStats
	for i, offset := range sr.footer.BlockOffsets {
		if stats != nil && i < len(stats) {
			s := stats[i]
			// {0,0} means the block had no valid ID range (no parseable
			// records or legacy footer). Skip the optimization — fall
			// through to a full block scan rather than risk dropping data.
			if s.MinID != 0 || s.MaxID != 0 {
				if skip(s.MinID, s.MaxID) {
					continue
				}
			}
		}
		if err := sr.scanBlock(offset, fn); err != nil {
			if errors.Is(err, ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (sr *SegmentReader) ScanReverseWithBlockSkip(
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	stats := sr.footer.BlockStats
	offsets := sr.footer.BlockOffsets
	for i := len(offsets) - 1; i >= 0; i-- {
		offset := offsets[i]
		if stats != nil && i < len(stats) {
			s := stats[i]
			// {0,0} means the block had no valid ID range (no parseable
			// records or legacy footer). Skip the optimization — fall
			// through to a full block scan rather than risk dropping data.
			if s.MinID != 0 || s.MaxID != 0 {
				if skip(s.MinID, s.MaxID) {
					continue
				}
			}
		}
		if err := sr.scanBlock(offset, fn); err != nil {
			if errors.Is(err, ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (sr *SegmentReader) ScanTimeRangeWithBlockSkip(
	from, to int64,
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	if sr.footer.MaxTS < from || sr.footer.MinTS > to {
		return nil
	}
	return sr.ScanWithBlockSkip(skip, fn)
}

func (sr *SegmentReader) ScanTimeRangeReverseWithBlockSkip(
	from, to int64,
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	if sr.footer.MaxTS < from || sr.footer.MinTS > to {
		return nil
	}
	return sr.ScanReverseWithBlockSkip(skip, fn)
}

func (sr *SegmentReader) ScanTimeRange(from, to int64, fn func(data []byte) error) error {
	if sr.footer.MaxTS < from || sr.footer.MinTS > to {
		return nil
	}
	return sr.scanBlocks(sr.footer.BlockOffsets, fn)
}

func (sr *SegmentReader) scanBlocks(offsets []int64, fn func(data []byte) error) error {
	for _, offset := range offsets {
		if err := sr.scanBlock(offset, fn); err != nil {
			return err
		}
	}
	return nil
}

func (sr *SegmentReader) scanBlock(offset int64, fn func(data []byte) error) error {
	if err := sr.ensureDecoder(); err != nil {
		return err
	}
	if _, err := sr.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("segment: seek to block %d: %w", offset, err)
	}

	var blockHeader [blockHeaderSize]byte
	if _, err := io.ReadFull(sr.file, blockHeader[:]); err != nil {
		return fmt.Errorf("segment: read block header at %d: %w", offset, err)
	}

	magic := binary.LittleEndian.Uint32(blockHeader[0:4])
	if magic != blockMagic {
		return fmt.Errorf("segment: bad block magic at offset %d", offset)
	}

	uncompressedSize := binary.LittleEndian.Uint32(blockHeader[4:8])
	compressedSize := binary.LittleEndian.Uint32(blockHeader[8:12])

	compressed := make([]byte, compressedSize)
	if _, err := io.ReadFull(sr.file, compressed); err != nil {
		return fmt.Errorf("segment: read block data at %d: %w", offset, err)
	}

	uncompressed := make([]byte, 0, uncompressedSize)
	uncompressed, err := sr.decoder.DecodeAll(compressed, uncompressed)
	if err != nil {
		return fmt.Errorf("segment: decompress block at %d: %w", offset, err)
	}

	r := bytes.NewReader(uncompressed)
	for r.Len() > 0 {
		var lenBuf [4]byte
		if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
			return fmt.Errorf("segment: read record length: %w", err)
		}
		length := binary.LittleEndian.Uint32(lenBuf[:])

		data := make([]byte, length)
		if _, err := io.ReadFull(r, data); err != nil {
			return fmt.Errorf("segment: read record data: %w", err)
		}

		if err := fn(data); err != nil {
			return err
		}
	}

	return nil
}

func (sr *SegmentReader) Close() error {
	if sr.decoder != nil {
		sr.decoder.Close()
	}
	return sr.file.Close()
}

func writeUint64(w *bytes.Buffer, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	w.Write(b[:])
}

func writeUint32(w *bytes.Buffer, v uint32) {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	w.Write(b[:])
}

func readUint64(r *bytes.Reader) uint64 {
	var b [8]byte
	_, _ = io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint64(b[:])
}

func readUint32(r *bytes.Reader) uint32 {
	var b [4]byte
	_, _ = io.ReadFull(r, b[:])
	return binary.LittleEndian.Uint32(b[:])
}
