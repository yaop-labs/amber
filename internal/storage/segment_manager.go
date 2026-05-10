package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/klauspost/compress/zstd"
)

type RotationPolicy struct {
	MaxRecords uint64
	MaxBytes   int64
}

var DefaultRotationPolicy = RotationPolicy{
	MaxRecords: 1_000_000,
	MaxBytes:   512 << 20,
}

type SegmentManager struct {
	mu         sync.RWMutex
	dir        string
	wal        *WAL
	policy     RotationPolicy
	meta       *StoreMeta
	active     *SegmentWriter
	activeSize int64
	onSeal     func(meta SegmentMeta)
}

func (sm *SegmentManager) SetOnSeal(fn func(meta SegmentMeta)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onSeal = fn
}

func OpenSegmentManager(dir string, policy RotationPolicy) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, fmt.Errorf("segmgr: mkdir %s: %w", dir, err)
	}

	wal, err := OpenWAL(dir)
	if err != nil {
		return nil, fmt.Errorf("segmgr: open wal: %w", err)
	}

	meta, err := loadMeta(dir)
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("segmgr: load meta: %w", err)
	}

	sm := &SegmentManager{
		dir:    dir,
		wal:    wal,
		policy: policy,
		meta:   meta,
	}

	if err := sm.replayWAL(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("segmgr: replay wal: %w", err)
	}

	if err := sm.openActiveSegment(); err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("segmgr: open active segment: %w", err)
	}

	return sm, nil
}

func (sm *SegmentManager) replayWAL() error {
	var activeMeta *SegmentMeta
	for i := range sm.meta.Segments {
		if !sm.meta.Segments[i].Sealed {
			activeMeta = &sm.meta.Segments[i]
			break
		}
	}

	if activeMeta == nil {
		count, err := sm.wal.Replay(func([]byte) error { return nil })
		if err != nil {
			return err
		}
		if count > 0 {
			return sm.wal.Truncate()
		}
		return nil
	}

	segPath := filepath.Join(sm.dir, activeMeta.FileName)

	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		return sm.wal.Truncate()
	}

	writer, fileSize, err := appendSegmentWriter(segPath)
	if err != nil {
		return fmt.Errorf("segmgr: replay: open segment for append: %w", err)
	}
	sm.activeSize = fileSize - segHeaderSize
	if sm.activeSize < 0 {
		sm.activeSize = 0
	}

	count, err := sm.wal.Replay(func(payload []byte) error {
		if len(payload) < 8 {
			return fmt.Errorf("segmgr: replay: payload too short")
		}
		ts := int64(payload[0]) | int64(payload[1])<<8 | int64(payload[2])<<16 |
			int64(payload[3])<<24 | int64(payload[4])<<32 | int64(payload[5])<<40 |
			int64(payload[6])<<48 | int64(payload[7])<<56
		data := payload[8:]
		return writer.WriteRecord(data, ts)
	})

	if err != nil {
		_ = writer.Close()
		return fmt.Errorf("segmgr: replay records: %w", err)
	}

	if count > 0 {
		activeMeta.RecordCount = writer.RecordCount()
		if err := saveMeta(sm.dir, sm.meta); err != nil {
			_ = writer.Close()
			return err
		}
		if err := sm.wal.Truncate(); err != nil {
			_ = writer.Close()
			return err
		}
	}

	sm.active = writer
	return nil
}

func (sm *SegmentManager) openActiveSegment() error {
	if sm.active != nil {
		return nil
	}

	for i := range sm.meta.Segments {
		if !sm.meta.Segments[i].Sealed {
			segPath := filepath.Join(sm.dir, sm.meta.Segments[i].FileName)
			writer, fileSize, err := appendSegmentWriter(segPath)
			if err != nil {
				return fmt.Errorf("segmgr: open active: %w", err)
			}
			sm.active = writer
			sm.activeSize = fileSize - segHeaderSize
			if sm.activeSize < 0 {
				sm.activeSize = 0
			}
			return nil
		}
	}

	return sm.createNewSegment()
}

func (sm *SegmentManager) createNewSegment() error {
	id := sm.meta.NextSegmentID
	fileName := segmentFileName(id)
	segPath := filepath.Join(sm.dir, fileName)

	writer, err := OpenSegmentWriter(segPath)
	if err != nil {
		return fmt.Errorf("segmgr: create segment %d: %w", id, err)
	}

	sm.meta.NextSegmentID++
	sm.meta.Segments = append(sm.meta.Segments, SegmentMeta{
		ID:       id,
		FileName: fileName,
		Sealed:   false,
	})

	if err := saveMeta(sm.dir, sm.meta); err != nil {
		_ = writer.Close()
		return fmt.Errorf("segmgr: save meta after create: %w", err)
	}

	sm.active = writer
	sm.activeSize = 0
	return nil
}

func (sm *SegmentManager) Write(data []byte, ts int64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	payload := makeWALPayload(ts, data)

	if err := sm.wal.Write(payload); err != nil {
		return fmt.Errorf("segmgr: wal write: %w", err)
	}

	if err := sm.active.WriteRecord(data, ts); err != nil {
		return fmt.Errorf("segmgr: segment write: %w", err)
	}
	sm.activeSize += int64(len(data))

	if err := sm.wal.Truncate(); err != nil {
		return fmt.Errorf("segmgr: wal truncate: %w", err)
	}

	if sm.shouldRotate() {
		if err := sm.rotate(); err != nil {
			return fmt.Errorf("segmgr: rotate: %w", err)
		}
	}

	return nil
}

func (sm *SegmentManager) WriteBatch(items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	payloads := make([][]byte, len(items))
	for i, item := range items {
		payloads[i] = makeWALPayload(item.TS, item.Data)
	}

	if err := sm.wal.WriteBatch(payloads); err != nil {
		return fmt.Errorf("segmgr: wal batch: %w", err)
	}

	for _, item := range items {
		if err := sm.active.WriteRecord(item.Data, item.TS); err != nil {
			return fmt.Errorf("segmgr: segment write: %w", err)
		}
		sm.activeSize += int64(len(item.Data))
	}

	if err := sm.wal.Truncate(); err != nil {
		return fmt.Errorf("segmgr: wal truncate: %w", err)
	}

	if sm.shouldRotate() {
		if err := sm.rotate(); err != nil {
			return fmt.Errorf("segmgr: rotate: %w", err)
		}
	}

	return nil
}

type BatchItem struct {
	Data []byte
	TS   int64
}

func (sm *SegmentManager) shouldRotate() bool {
	if sm.policy.MaxRecords > 0 && sm.active.RecordCount() >= sm.policy.MaxRecords {
		return true
	}
	if sm.policy.MaxBytes > 0 && sm.activeSize >= sm.policy.MaxBytes {
		return true
	}
	return false
}

func (sm *SegmentManager) rotate() error {
	if err := sm.active.Close(); err != nil {
		return fmt.Errorf("segmgr: rotate close: %w", err)
	}

	var sealedMeta SegmentMeta
	for i := range sm.meta.Segments {
		if !sm.meta.Segments[i].Sealed {
			sm.meta.Segments[i].Sealed = true
			sm.meta.Segments[i].RecordCount = sm.active.RecordCount()

			segPath := filepath.Join(sm.dir, sm.meta.Segments[i].FileName)
			if info, err := os.Stat(segPath); err == nil {
				sm.meta.Segments[i].SizeBytes = info.Size()
			}

			if sr, err := OpenSegmentReader(segPath, nil); err == nil {
				footer := sr.Footer()
				sm.meta.Segments[i].MinTS = footer.MinTS
				sm.meta.Segments[i].MaxTS = footer.MaxTS
				_ = sr.Close()
			}
			sealedMeta = sm.meta.Segments[i]
			break
		}
	}

	sm.active = nil

	if err := saveMeta(sm.dir, sm.meta); err != nil {
		return err
	}

	if sm.onSeal != nil {
		go sm.onSeal(sealedMeta)
	}

	return sm.createNewSegment()
}

func (sm *SegmentManager) Rotate() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.active == nil || sm.active.RecordCount() == 0 {
		return nil
	}
	return sm.rotate()
}

func (sm *SegmentManager) Segments() []SegmentMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	result := make([]SegmentMeta, 0, len(sm.meta.Segments))
	for _, s := range sm.meta.Segments {
		if s.Sealed {
			result = append(result, s)
		}
	}
	return result
}

func (sm *SegmentManager) RemoveSegment(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i, seg := range sm.meta.Segments {
		if seg.ID != id {
			continue
		}
		if !seg.Sealed {
			return fmt.Errorf("segmgr: cannot remove active segment %d", id)
		}
		sm.meta.Segments = append(sm.meta.Segments[:i], sm.meta.Segments[i+1:]...)
		return saveMeta(sm.dir, sm.meta)
	}
	return nil
}

func (sm *SegmentManager) Flush() error {
	sm.mu.RLock()
	active := sm.active
	sm.mu.RUnlock()
	if active == nil {
		return nil
	}
	return active.Flush()
}

func (sm *SegmentManager) ActiveRecordCount() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.active == nil {
		return 0
	}
	return sm.active.RecordCount()
}

func (sm *SegmentManager) ActiveBlockIndex(fileName string) (*BlockIndexHint, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.active == nil {
		return nil, false
	}
	var activeName string
	for _, s := range sm.meta.Segments {
		if !s.Sealed {
			activeName = s.FileName
			break
		}
	}
	if activeName == "" || activeName != fileName {
		return nil, false
	}
	return sm.active.SnapshotBlockIndex()
}

func (sm *SegmentManager) ActiveSegmentMeta() (SegmentMeta, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, s := range sm.meta.Segments {
		if !s.Sealed {
			return s, true
		}
	}
	return SegmentMeta{}, false
}

func (sm *SegmentManager) SegmentPath(meta SegmentMeta) string {
	return filepath.Join(sm.dir, meta.FileName)
}

func (sm *SegmentManager) Close() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.active != nil {
		if err := sm.active.Close(); err != nil {
			return fmt.Errorf("segmgr: close active: %w", err)
		}

		for i := range sm.meta.Segments {
			if !sm.meta.Segments[i].Sealed {
				sm.meta.Segments[i].Sealed = true
				sm.meta.Segments[i].RecordCount = sm.active.RecordCount()

				segPath := filepath.Join(sm.dir, sm.meta.Segments[i].FileName)
				if info, err := os.Stat(segPath); err == nil {
					sm.meta.Segments[i].SizeBytes = info.Size()
				}
				if sr, err := OpenSegmentReader(segPath, nil); err == nil {
					footer := sr.Footer()
					sm.meta.Segments[i].MinTS = footer.MinTS
					sm.meta.Segments[i].MaxTS = footer.MaxTS
					_ = sr.Close()
				}
				break
			}
		}
		sm.active = nil
		_ = saveMeta(sm.dir, sm.meta)
	}

	return sm.wal.Close()
}

func makeWALPayload(ts int64, data []byte) []byte {
	payload := make([]byte, 8+len(data))
	payload[0] = byte(ts)
	payload[1] = byte(ts >> 8)
	payload[2] = byte(ts >> 16)
	payload[3] = byte(ts >> 24)
	payload[4] = byte(ts >> 32)
	payload[5] = byte(ts >> 40)
	payload[6] = byte(ts >> 48)
	payload[7] = byte(ts >> 56)
	copy(payload[8:], data)
	return payload
}

func appendSegmentWriter(path string) (*SegmentWriter, int64, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return nil, 0, fmt.Errorf("append segment: open %s: %w", path, err)
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("append segment: zstd encoder: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, 0, fmt.Errorf("append segment: stat: %w", err)
	}
	fileSize := info.Size()

	sw := &SegmentWriter{
		file:       f,
		bw:         bufio.NewWriterSize(f, 256*1024),
		encoder:    enc,
		blockSize:  DefaultBlockSize,
		fileOffset: fileSize,
	}

	// Restore writer state from blocks already on disk. Without this, a rotate
	// after a crash-and-replay would write a footer pointing only at the
	// post-replay blocks, orphaning everything written before the crash.
	if fileSize > segHeaderSize {
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("append segment: scan existing blocks: %w", err)
		}
		footer := sr.Footer()
		_ = sr.Close()

		sw.minTS = footer.MinTS
		sw.maxTS = footer.MaxTS
		sw.recordCount = footer.RecordCount
		sw.blockOffsets = append([]int64(nil), footer.BlockOffsets...)
		sw.blockStats = append([]BlockStat(nil), footer.BlockStats...)
	}

	return sw, fileSize, nil
}
