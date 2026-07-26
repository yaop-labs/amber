package storage

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
)

var ErrStopScan = errors.New("stop scan")

const (
	segMagic = uint32(0x414D4252)
	// v3 widened footer BlockStats with per-block event-time bounds.
	segVersion    = uint16(3)
	segVersionMin = uint16(1)
	segHeaderSize = 16

	blockMagic      = uint32(0x424C4F4B)
	blockHeaderSize = 16

	footerMagic = uint32(0x464F4F54)

	// DefaultBlockSize is the uncompressed block flush threshold. 512 KiB
	// keeps ~30 blocks per 100k-record segment - fine enough granularity for
	// the heap-threshold block skip in reverse scans. The previous 4 MiB
	// made a segment 2-4 blocks, so a limit-100 windowed query (q2) had to
	// decompress whole megabytes to surface a handful of records: measured
	// 3.2 ms -> 190 us per query at 512 KiB on the same data, with no
	// compression-ratio loss (zstd ratio is flat down to 256 KiB on log
	// bodies) and no write-throughput change.
	DefaultBlockSize = 512 * 1024
)

// Block buffer pools are reused across scanBlock calls.
// Oversized blocks allocate fresh buffers instead of resizing pooled buffers.
var (
	scanCompressedPool   = sync.Pool{New: func() any { b := make([]byte, 0, 64<<10); return &b }}
	scanUncompressedPool = sync.Pool{New: func() any { b := make([]byte, 0, DefaultBlockSize); return &b }}
)

// blockPoolMaxSize caps buffers returned to scanUncompressedPool.
const blockPoolMaxSize = 2 * DefaultBlockSize

// newSegmentEncoder returns the encoder owned by one SegmentWriter.
// SegmentWriter serializes flushBlock under its mutex, and zstd EncodeAll uses
// only one engine per call. The library default allocates GOMAXPROCS engines
// for concurrent EncodeAll callers, which only multiplies tables and history
// buffers here without adding compression parallelism. The window matches the
// normal uncompressed block target, so one engine doesn't retain a default
// multi-megabyte history that cannot improve compression across block frames.
func newSegmentEncoder() (*zstd.Encoder, error) {
	return zstd.NewWriter(
		nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(DefaultBlockSize),
	)
}

var (
	ErrSegmentCorrupted = errors.New("segment: corrupted file")
	ErrSegmentBadMagic  = errors.New("segment: bad magic bytes")
	ErrNoFooter         = errors.New("segment: no footer found")
)

// BlockStat carries per-block pruning ranges: ULID-derived entry IDs (v2+)
// and event-time bounds (v3+). MinTS==MaxTS==0 means "no time stats" - v2
// footers and footerless recovery - and disables time pruning for the block.
type BlockStat struct {
	MinID uint64
	MaxID uint64
	MinTS int64
	MaxTS int64
}

// SegmentFooter is the trailer of a sealed segment: the file-wide time span and
// record count plus per-block offsets and stats that let scans seek and prune
// blocks without decompressing them.
type SegmentFooter struct {
	MinTS        int64
	MaxTS        int64
	RecordCount  uint64
	BlockCount   uint32
	BlockOffsets []int64
	BlockStats   []BlockStat
}

// SegmentWriter appends records into a segment file as a sequence of
// zstd-compressed blocks, tracking per-block id/time bounds for the footer. It
// is safe for concurrent use and fail-stops after a write or fsync error.
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
	blockMinTS      int64
	blockMaxTS      int64
	blockHasRecords bool
	minTS           int64
	maxTS           int64
	recordCount     uint64
	blockOffsets    []int64
	blockStats      []BlockStat
	fileOffset      int64

	blockSize int
	closed    bool

	// failed fail-stops the writer. After a failed fsync the kernel may have
	// dropped the dirty pages (PostgreSQL's "fsyncgate"), and after a partial
	// buffered write fileOffset no longer matches the file - later block
	// offsets would be wrong. Either way the segment can't accept more
	// records; the manager recovers from WAL + last synced size on restart.
	// Guarded by mu.
	failed error
}

// failStop records a fatal writer error; subsequent writes return it.
// Caller holds sw.mu.
func (sw *SegmentWriter) failStop(err error) error {
	if sw.failed == nil {
		sw.failed = err
	}
	return err
}

// OpenSegmentWriter creates a new segment file at path (failing if it exists)
// and returns a writer positioned at its start.
func OpenSegmentWriter(path string) (*SegmentWriter, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644) //nolint:gosec
	if err != nil {
		return nil, fmt.Errorf("segment: create %s: %w", path, err)
	}

	enc, err := newSegmentEncoder()
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

// WriteRecord appends one record with event time ts, flushing the current
// block to the file once it reaches the block size.
func (sw *SegmentWriter) WriteRecord(data []byte, ts int64) error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return fmt.Errorf("segment: writer is closed")
	}
	if sw.failed != nil {
		return sw.failed
	}

	if sw.recordCount == 0 || ts < sw.minTS {
		sw.minTS = ts
	}
	if sw.recordCount == 0 || ts > sw.maxTS {
		sw.maxTS = ts
	}

	if sw.blockRecords == 0 || ts < sw.blockMinTS {
		sw.blockMinTS = ts
	}
	if sw.blockRecords == 0 || ts > sw.blockMaxTS {
		sw.blockMaxTS = ts
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
		MinTS: sw.blockMinTS,
		MaxTS: sw.blockMaxTS,
	})

	var blockHeader [blockHeaderSize]byte
	binary.LittleEndian.PutUint32(blockHeader[0:4], blockMagic)
	binary.LittleEndian.PutUint32(blockHeader[4:8], uncompressedSize)
	binary.LittleEndian.PutUint32(blockHeader[8:12], compressedSize)
	binary.LittleEndian.PutUint32(blockHeader[12:16], sw.blockRecords)

	n, err := sw.bw.Write(blockHeader[:])
	sw.fileOffset += int64(n)
	if err != nil {
		return sw.failStop(fmt.Errorf("segment: write block header: %w", err))
	}

	n, err = sw.bw.Write(compressed)
	sw.fileOffset += int64(n)
	if err != nil {
		return sw.failStop(fmt.Errorf("segment: write block data: %w", err))
	}

	sw.blockBuf.Reset()
	sw.blockRecords = 0
	sw.blockMinID = 0
	sw.blockMaxID = 0
	sw.blockMinTS = 0
	sw.blockMaxTS = 0
	sw.blockHasRecords = false

	return nil
}

// Flush compresses and writes any buffered block and flushes the file buffer,
// so the bytes reach the OS. Callers fsync separately via the manager.
func (sw *SegmentWriter) Flush() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return nil
	}
	if sw.failed != nil {
		return sw.failed
	}
	if sw.blockBuf.Len() > 0 {
		if err := sw.flushBlock(); err != nil {
			return err
		}
	}
	if err := sw.bw.Flush(); err != nil {
		return sw.failStop(err)
	}
	return nil
}

// Close seals the segment: it flushes the last block, writes the footer,
// fsyncs, and closes the file. It refuses to write a footer over an unknown
// state on a failed writer.
func (sw *SegmentWriter) Close() error {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	if sw.closed {
		return nil
	}
	sw.closed = true

	// A failed writer must not append a footer: fileOffset may not match the
	// file, and the pages on disk are suspect. Close the handle and let
	// footerless recovery deal with whatever actually landed.
	if sw.failed != nil {
		_ = sw.file.Close()
		return sw.failed
	}

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

// writeFooter appends the offsets/stats footer. Deliberately no footer CRC:
// the payload blocks are integrity-checked by their zstd frames, and a
// corrupt or truncated footer is survivable - OpenSegmentReader falls back
// to the footerless block scan (scanBlockOffsets). A CRC would only convert
// "slow open" into "fail open" for the same corruption.
func (sw *SegmentWriter) writeFooter() error {
	blockCount := uint32(len(sw.blockOffsets))

	footerSize := 8 + 8 + 8 + 4 + blockCount*8 + blockCount*32

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
		writeUint64(&buf, uint64(stat.MinTS))
		writeUint64(&buf, uint64(stat.MaxTS))
	}
	writeUint32(&buf, footerSize)
	writeUint32(&buf, footerMagic)

	n, err := sw.bw.Write(buf.Bytes())
	sw.fileOffset += int64(n)
	return err
}

// RecordCount returns the number of records written so far.
func (sw *SegmentWriter) RecordCount() uint64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return sw.recordCount
}

// TimeRange returns the min and max event time written so far.
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

// Sync flushes the writer, fsyncs the file, and returns the durable offset.
// Records still buffered in blockBuf are not covered.
func (sw *SegmentWriter) Sync() (int64, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if sw.closed {
		return sw.fileOffset, nil
	}
	if sw.failed != nil {
		return 0, sw.failed
	}
	if err := sw.bw.Flush(); err != nil {
		return 0, sw.failStop(fmt.Errorf("segment: sync flush: %w", err))
	}
	if err := sw.file.Sync(); err != nil {
		return 0, sw.failStop(fmt.Errorf("segment: sync fsync: %w", err))
	}
	return sw.fileOffset, nil
}

// BlockIndexHint is a snapshot of an active (unsealed) segment's block layout,
// passed to OpenSegmentReader so reads of the still-being-written segment can
// seek and prune blocks without a footer (which only sealed segments have).
type BlockIndexHint struct {
	Offsets     []int64
	Stats       []BlockStat
	MinTS       int64
	MaxTS       int64
	RecordCount uint64
}

// SnapshotBlockIndex returns the current block index of the active segment for
// warm reads, or false when no block has been flushed yet.
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

// SegmentReader is safe for concurrent scans once OpenSegmentReader returns:
// block reads go through ReadAt (no shared file position) and zstd DecodeAll
// is concurrency-safe. Only Close mutates state.
type SegmentReader struct {
	file    *os.File
	decoder *zstd.Decoder
	footer  SegmentFooter
	version uint16
}

// OpenSegmentReader opens a segment for reading. For a sealed segment pass a
// nil hint and the footer is read; for the active segment pass the writer's
// SnapshotBlockIndex so reads work before a footer exists.
func OpenSegmentReader(path string, hint *BlockIndexHint) (*SegmentReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("segment: open %s: %w", path, err)
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("segment: create zstd decoder: %w", err)
	}

	sr := &SegmentReader{
		file:    f,
		decoder: dec,
	}
	closeAll := func() {
		dec.Close()
		_ = f.Close()
	}

	if err := sr.readHeader(); err != nil {
		closeAll()
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
			closeAll()
			return nil, err
		}

		if err2 := sr.scanBlockOffsets(); err2 != nil {
			closeAll()
			return nil, err2
		}
	}

	return sr, nil
}

func (sr *SegmentReader) scanBlockOffsets() error {
	if _, err := sr.file.Seek(int64(segHeaderSize), io.SeekStart); err != nil {
		return fmt.Errorf("segment: seek to blocks: %w", err)
	}

	var offsets []int64
	var stats []BlockStat
	pos := int64(segHeaderSize)
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
				// Only the record ID (a ULID, always the first 16 bytes in both
				// the log and span layouts) is recoverable here. The event
				// timestamp is not: logs carry it at a fixed offset, spans after
				// variable-length strings, and a footerless reader cannot tell
				// the two apart. See the footer assignment below.
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

	// Footerless recovery cannot infer the segment time range from blocks, so
	// use the widest range and avoid incorrect time pruning.
	sr.footer = SegmentFooter{
		MinTS:        math.MinInt64,
		MaxTS:        math.MaxInt64,
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
		return fmt.Errorf("%w: read header: %v", ErrSegmentCorrupted, err)
	}

	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != segMagic {
		return fmt.Errorf("%w: %v", ErrSegmentCorrupted, ErrSegmentBadMagic)
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
	for i := range blockCount {
		sr.footer.BlockOffsets[i] = int64(readUint64(r))
	}

	if sr.version >= 2 && blockCount > 0 {
		statSize := 16
		if sr.version >= 3 {
			statSize = 32
		}
		if r.Len() >= int(blockCount)*statSize {
			sr.footer.BlockStats = make([]BlockStat, blockCount)
			for i := range blockCount {
				sr.footer.BlockStats[i].MinID = readUint64(r)
				sr.footer.BlockStats[i].MaxID = readUint64(r)
				if sr.version >= 3 {
					sr.footer.BlockStats[i].MinTS = int64(readUint64(r))
					sr.footer.BlockStats[i].MaxTS = int64(readUint64(r))
				}
			}
		}
	}

	return nil
}

// Footer returns the segment's footer (or the hint-derived equivalent).
func (sr *SegmentReader) Footer() SegmentFooter {
	return sr.footer
}

// Scan visits every record in the segment in write order.
func (sr *SegmentReader) Scan(fn func(data []byte) error) error {
	return sr.scanBlocks(sr.footer.BlockOffsets, fn)
}

// ScanWithBlockSkip scans forward, skipping any block whose ID range the skip
// predicate rejects (e.g. the top-k heap threshold).
func (sr *SegmentReader) ScanWithBlockSkip(
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	return sr.scanWithBlockSkip(false, nil, skip, fn)
}

// ScanReverseWithBlockSkip is ScanWithBlockSkip in reverse (newest first), the
// order used for reverse-paginated top-k log queries.
func (sr *SegmentReader) ScanReverseWithBlockSkip(
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	return sr.scanWithBlockSkip(true, nil, skip, fn)
}

// blockOutsideRange prunes a block by its event-time bounds (footer v3).
// Zero bounds mean "no time stats" (v2 footer or footerless recovery) and
// never prune.
func blockOutsideRange(s BlockStat, from, to int64) bool {
	if s.MinTS == 0 && s.MaxTS == 0 {
		return false
	}
	return s.MaxTS < from || s.MinTS > to
}

// ScanTimeRangeWithBlockSkip scans forward over [from, to], pruning blocks
// outside the range by their footer time bounds in addition to the ID skip.
func (sr *SegmentReader) ScanTimeRangeWithBlockSkip(
	from, to int64,
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	if sr.footer.MaxTS < from || sr.footer.MinTS > to {
		return nil
	}
	timeSkip := func(i int) bool {
		stats := sr.footer.BlockStats
		return stats != nil && i < len(stats) && blockOutsideRange(stats[i], from, to)
	}
	return sr.scanWithBlockSkip(false, timeSkip, skip, fn)
}

// ScanTimeRangeReverseWithBlockSkip is ScanTimeRangeWithBlockSkip in reverse.
func (sr *SegmentReader) ScanTimeRangeReverseWithBlockSkip(
	from, to int64,
	skip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	if sr.footer.MaxTS < from || sr.footer.MinTS > to {
		return nil
	}
	timeSkip := func(i int) bool {
		stats := sr.footer.BlockStats
		return stats != nil && i < len(stats) && blockOutsideRange(stats[i], from, to)
	}
	return sr.scanWithBlockSkip(true, timeSkip, skip, fn)
}

// ScanTimeRange visits records in blocks overlapping [from, to] in write order,
// pruning by footer time bounds with no ID skip.
func (sr *SegmentReader) ScanTimeRange(from, to int64, fn func(data []byte) error) error {
	if sr.footer.MaxTS < from || sr.footer.MinTS > to {
		return nil
	}
	stats := sr.footer.BlockStats
	for i, offset := range sr.footer.BlockOffsets {
		if stats != nil && i < len(stats) && blockOutsideRange(stats[i], from, to) {
			continue
		}
		if err := sr.scanBlock(offset, fn); err != nil {
			return err
		}
	}
	return nil
}

// scanWithBlockSkip is the shared block loop: timeSkip prunes by per-block
// event-time bounds, idSkip by the caller's ID-range predicate.
func (sr *SegmentReader) scanWithBlockSkip(
	reverse bool,
	timeSkip func(i int) bool,
	idSkip func(minID, maxID uint64) bool,
	fn func(data []byte) error,
) error {
	stats := sr.footer.BlockStats
	offsets := sr.footer.BlockOffsets
	n := len(offsets)
	for j := range n {
		i := j
		if reverse {
			i = n - 1 - j
		}
		if timeSkip != nil && timeSkip(i) {
			continue
		}
		if idSkip != nil && stats != nil && i < len(stats) {
			s := stats[i]
			// {0,0} means no usable ID range; scan the block.
			if s.MinID != 0 || s.MaxID != 0 {
				if idSkip(s.MinID, s.MaxID) {
					continue
				}
			}
		}
		if err := sr.scanBlock(offsets[i], fn); err != nil {
			if errors.Is(err, ErrStopScan) {
				return nil
			}
			return err
		}
	}
	return nil
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
	// pread (ReadAt) keeps the reader free of shared file-position state, so
	// concurrent scans of one segment don't serialize on a mutex.
	var blockHeader [blockHeaderSize]byte
	if _, err := sr.file.ReadAt(blockHeader[:], offset); err != nil {
		return fmt.Errorf("%w: read block header at %d: %v", ErrSegmentCorrupted, offset, err)
	}

	magic := binary.LittleEndian.Uint32(blockHeader[0:4])
	if magic != blockMagic {
		return fmt.Errorf("%w: bad block magic at offset %d", ErrSegmentCorrupted, offset)
	}

	uncompressedSize := binary.LittleEndian.Uint32(blockHeader[4:8])
	compressedSize := binary.LittleEndian.Uint32(blockHeader[8:12])

	compressedP := scanCompressedPool.Get().(*[]byte)
	compressed := *compressedP
	if cap(compressed) < int(compressedSize) {
		compressed = make([]byte, compressedSize)
	} else {
		compressed = compressed[:compressedSize]
	}
	defer func() {
		if cap(compressed) <= blockPoolMaxSize {
			c := compressed[:0]
			*compressedP = c
			scanCompressedPool.Put(compressedP)
		}
	}()

	if _, err := sr.file.ReadAt(compressed, offset+blockHeaderSize); err != nil {
		return fmt.Errorf("%w: read block data at %d: %v", ErrSegmentCorrupted, offset, err)
	}

	uncompressedP := scanUncompressedPool.Get().(*[]byte)
	uncompressed := (*uncompressedP)[:0]
	if cap(uncompressed) < int(uncompressedSize) {
		// Drop the undersized pooled buffer (will be GC'd) and let DecodeAll
		// allocate a fresh slice at the right size.
		uncompressed = make([]byte, 0, uncompressedSize)
	}
	uncompressed, err := sr.decoder.DecodeAll(compressed, uncompressed)
	if err != nil {
		// Return the (possibly-grown) buffer to the pool if it still fits the
		// cap. Otherwise drop it.
		if cap(uncompressed) <= blockPoolMaxSize {
			u := uncompressed[:0]
			*uncompressedP = u
			scanUncompressedPool.Put(uncompressedP)
		}
		return fmt.Errorf("%w: decompress block at %d: %v", ErrSegmentCorrupted, offset, err)
	}
	defer func() {
		if cap(uncompressed) <= blockPoolMaxSize {
			u := uncompressed[:0]
			*uncompressedP = u
			scanUncompressedPool.Put(uncompressedP)
		}
	}()

	// Walk the uncompressed buffer directly instead of bytes.NewReader +
	// per-record make([]byte, length). Each fn call gets a zero-copy slice
	// into the already-decompressed block; callers that need to retain the
	// data (DecodeBytes, etc.) copy out of it themselves.
	pos := 0
	for pos < len(uncompressed) {
		if pos+4 > len(uncompressed) {
			return fmt.Errorf("%w: truncated record length at block offset %d", ErrSegmentCorrupted, offset)
		}
		length := int(binary.LittleEndian.Uint32(uncompressed[pos:]))
		pos += 4
		if pos+length > len(uncompressed) {
			return fmt.Errorf("%w: truncated record data at block offset %d", ErrSegmentCorrupted, offset)
		}
		if err := fn(uncompressed[pos : pos+length]); err != nil {
			return err
		}
		pos += length
	}

	return nil
}

// Close releases the reader's file handle and decoder.
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
