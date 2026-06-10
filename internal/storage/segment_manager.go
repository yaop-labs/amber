// Package storage owns segment files, the WAL, and the durability protocol
// (write -> WAL append -> segment write -> periodic checkpoint -> WAL truncate).
package storage

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/yaop-labs/amber/internal/selfobs"
)

type RotationPolicy struct {
	MaxRecords uint64
	MaxBytes   int64
}

var DefaultRotationPolicy = RotationPolicy{
	MaxRecords: 100_000,
	MaxBytes:   128 << 20,
}

// SegmentSidecarExts lists the files belonging to a sealed segment.
var SegmentSidecarExts = []string{"", ".bidx", ".fidx", ".filt", ".fts.filt", ".pidx"}

type SegmentManager struct {
	mu             sync.RWMutex
	dir            string
	wal            *WAL
	policy         RotationPolicy
	meta           *StoreMeta
	active         *SegmentWriter
	activeSize     int64
	onSeal         func(meta SegmentMeta)
	onSealComplete func(meta SegmentMeta)
	store          SegmentStore
}

// SetStore replaces the store used for sealed segments.
func (sm *SegmentManager) SetStore(s SegmentStore) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.store = s
}

// SetOnSealComplete registers a callback fired after seal callbacks finish.
func (sm *SegmentManager) SetOnSealComplete(fn func(meta SegmentMeta)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onSealComplete = fn
}

func (sm *SegmentManager) SetOnSeal(fn func(meta SegmentMeta)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onSeal = fn
}

func OpenSegmentManager(dir string, policy RotationPolicy) (*SegmentManager, error) {
	if err := os.MkdirAll(dir, 0750); err != nil { //nolint:gosec
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
		store:  NewLocalStore(dir),
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
		// No unsealed segment to replay into. Drop any orphan WAL records.
		count, err := sm.wal.Replay(func([]byte) error { return nil })
		if err != nil {
			return err
		}
		if count > 0 {
			return sm.wal.Truncate()
		}
		return nil
	}

	// Seed the WAL seq counter from the durable watermark so subsequent writes
	// stay strictly monotonic across the restart, even if the WAL is empty.
	if activeMeta.LastSyncedSeq > 0 {
		sm.wal.SetNextSeq(activeMeta.LastSyncedSeq + 1)
	}

	segPath := filepath.Join(sm.dir, activeMeta.FileName)

	if _, err := os.Stat(segPath); os.IsNotExist(err) {
		return sm.wal.Truncate()
	}

	// Truncate the segment file back to the last fsynced offset. WAL replay
	// rebuilds any missing tail.
	if activeMeta.LastSyncedSize > 0 {
		if info, err := os.Stat(segPath); err == nil && info.Size() > activeMeta.LastSyncedSize {
			if err := os.Truncate(segPath, activeMeta.LastSyncedSize); err != nil {
				return fmt.Errorf("segmgr: truncate to last synced size: %w", err)
			}
		}
	}

	writer, fileSize, err := appendSegmentWriter(segPath, activeMeta.MinTS, activeMeta.MaxTS)
	if err != nil {
		return fmt.Errorf("segmgr: replay: open segment for append: %w", err)
	}
	sm.activeSize = max(fileSize-segHeaderSize, 0)

	syncedSeq := activeMeta.LastSyncedSeq
	count, err := sm.wal.ReplayWithSeq(func(seq uint64, payload []byte) error {
		// Records with seq <= syncedSeq are already durable in the segment.
		// Re-applying them would double-write after a crash that landed in
		// the saveMeta-then-truncate window.
		if seq <= syncedSeq {
			return nil
		}
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
	_ = count

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
			writer, fileSize, err := appendSegmentWriter(segPath, sm.meta.Segments[i].MinTS, sm.meta.Segments[i].MaxTS)
			if err != nil {
				return fmt.Errorf("segmgr: open active: %w", err)
			}
			sm.active = writer
			sm.activeSize = max(fileSize-segHeaderSize, 0)
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

	present := true
	sm.meta.NextSegmentID++
	sm.meta.Segments = append(sm.meta.Segments, SegmentMeta{
		ID:           id,
		FileName:     fileName,
		Sealed:       false,
		LocalPresent: &present,
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

	walStart := time.Now()
	seq, err := sm.wal.Write(payload)
	selfobs.WALWriteDuration.WithLabelValues("single").Observe(time.Since(walStart).Seconds())
	selfobs.WALWrites.WithLabelValues("single").Inc()
	if err != nil {
		return fmt.Errorf("segmgr: wal write: %w", err)
	}

	blocksBefore := sm.active.BlockCount()
	if err := sm.active.WriteRecord(data, ts); err != nil {
		return fmt.Errorf("segmgr: segment write: %w", err)
	}
	sm.activeSize += int64(len(data))

	// Sync when WriteRecord flushes a block.
	if sm.active.BlockCount() > blocksBefore {
		if err := sm.checkpoint(seq); err != nil {
			return fmt.Errorf("segmgr: checkpoint: %w", err)
		}
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

	walStart := time.Now()
	firstSeq, err := sm.wal.WriteBatchTS(items)
	selfobs.WALWriteDuration.WithLabelValues("batch").Observe(time.Since(walStart).Seconds())
	selfobs.WALWrites.WithLabelValues("batch").Inc()
	if err != nil {
		return fmt.Errorf("segmgr: wal batch: %w", err)
	}

	var (
		sawFlush     bool
		lastFlushSeq uint64
	)
	for i, item := range items {
		before := sm.active.BlockCount()
		if err := sm.active.WriteRecord(item.Data, item.TS); err != nil {
			return fmt.Errorf("segmgr: segment write: %w", err)
		}
		sm.activeSize += int64(len(item.Data))
		if sm.active.BlockCount() > before {
			sawFlush = true
			lastFlushSeq = firstSeq + uint64(i)
		}
	}

	if sawFlush {
		if err := sm.checkpoint(lastFlushSeq); err != nil {
			return fmt.Errorf("segmgr: checkpoint: %w", err)
		}
	}

	if sm.shouldRotate() {
		if err := sm.rotate(); err != nil {
			return fmt.Errorf("segmgr: rotate: %w", err)
		}
	}

	return nil
}

// checkpoint persists the active segment sync watermark.
// It does not truncate the WAL; records after lastSyncedSeq may still be only
// in memory and the WAL.
func (sm *SegmentManager) checkpoint(lastSyncedSeq uint64) error {
	if sm.active == nil {
		return nil
	}
	syncedOffset, err := sm.active.Sync()
	if err != nil {
		return err
	}

	minTS, maxTS := sm.active.TimeRange()
	for i := range sm.meta.Segments {
		if !sm.meta.Segments[i].Sealed {
			sm.meta.Segments[i].LastSyncedSize = syncedOffset
			sm.meta.Segments[i].LastSyncedSeq = lastSyncedSeq
			sm.meta.Segments[i].RecordCount = sm.active.RecordCount()
			// Persist the time range so a crash-recovery reopen can seed it
			// back: the per-record event timestamp is not recoverable from the
			// segment blocks alone (it is not at a format-agnostic offset, and
			// event time may differ arbitrarily from ingest time), so meta is
			// the durable source of truth for an unsealed segment's range.
			sm.meta.Segments[i].MinTS = minTS
			sm.meta.Segments[i].MaxTS = maxTS
			break
		}
	}

	return saveMeta(sm.dir, sm.meta)
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
	recordCount := sm.active.RecordCount()
	minTS, maxTS := sm.active.TimeRange()

	if err := sm.active.Close(); err != nil {
		return fmt.Errorf("segmgr: rotate close: %w", err)
	}

	var sealedMeta SegmentMeta
	for i := range sm.meta.Segments {
		if !sm.meta.Segments[i].Sealed {
			sm.meta.Segments[i].Sealed = true
			sm.meta.Segments[i].RecordCount = recordCount
			sm.meta.Segments[i].MinTS = minTS
			sm.meta.Segments[i].MaxTS = maxTS

			segPath := filepath.Join(sm.dir, sm.meta.Segments[i].FileName)
			if info, err := os.Stat(segPath); err == nil { //nolint:gosec
				sm.meta.Segments[i].SizeBytes = info.Size()
			}
			sealedMeta = sm.meta.Segments[i]
			break
		}
	}

	sm.active = nil

	if err := saveMeta(sm.dir, sm.meta); err != nil {
		return err
	}

	if sm.onSeal != nil || sm.onSealComplete != nil {
		onSeal := sm.onSeal
		onSealComplete := sm.onSealComplete
		go func() {
			sealStart := time.Now()
			if onSeal != nil {
				onSeal(sealedMeta)
			}
			selfobs.SealDuration.WithLabelValues(filepath.Base(sm.dir)).Observe(time.Since(sealStart).Seconds())
			if onSealComplete != nil {
				onSealComplete(sealedMeta)
			}
		}()
	}

	if err := sm.createNewSegment(); err != nil {
		return err
	}

	// Old segment is sealed and fsync'd via SegmentWriter.Close, so every WAL
	// record that fed it is now durable. The new active is empty. Drop the
	// WAL so it stays bounded.
	return sm.wal.Truncate()
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
		if s.Sealed && !s.DeletePending {
			result = append(result, s)
		}
	}
	return result
}

// SegmentsForRetention returns sealed segments, including delete-pending ones.
func (sm *SegmentManager) SegmentsForRetention() []SegmentMeta {
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

// IsQueryableSegment reports whether fileName is not pending deletion.
func (sm *SegmentManager) IsQueryableSegment(fileName string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, s := range sm.meta.Segments {
		if s.FileName == fileName {
			return !s.DeletePending
		}
	}
	return false
}

// PendingUploads returns sealed segments not yet uploaded.
func (sm *SegmentManager) PendingUploads() []SegmentMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var pending []SegmentMeta
	for _, s := range sm.meta.Segments {
		if s.Sealed && !s.DeletePending && s.UploadState != UploadStateUploaded {
			pending = append(pending, s)
		}
	}
	return pending
}

// MarkUploaded marks a sealed segment as uploaded.
func (sm *SegmentManager) MarkUploaded(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i := range sm.meta.Segments {
		if sm.meta.Segments[i].ID != id {
			continue
		}
		if sm.meta.Segments[i].UploadState == UploadStateUploaded {
			return nil
		}
		sm.meta.Segments[i].UploadState = UploadStateUploaded
		sm.meta.Segments[i].UploadAttempts = 0
		sm.meta.Segments[i].LastUploadErr = ""
		return saveMeta(sm.dir, sm.meta)
	}
	return fmt.Errorf("segmgr: mark uploaded: unknown segment id %d", id)
}

// MarkLocalEvicted records that a segment no longer has a local copy.
func (sm *SegmentManager) MarkLocalEvicted(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i := range sm.meta.Segments {
		if sm.meta.Segments[i].ID != id {
			continue
		}
		seg := &sm.meta.Segments[i]
		if seg.UploadState != UploadStateUploaded {
			return fmt.Errorf("segmgr: mark local evicted: segment %d is not uploaded", id)
		}
		if seg.LocalPresent != nil && !*seg.LocalPresent {
			return nil
		}
		absent := false
		seg.LocalPresent = &absent
		return saveMeta(sm.dir, sm.meta)
	}
	return fmt.Errorf("segmgr: mark local evicted: unknown segment id %d", id)
}

// BeginDeleteSegment marks a sealed segment for terminal deletion.
func (sm *SegmentManager) BeginDeleteSegment(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i := range sm.meta.Segments {
		if sm.meta.Segments[i].ID != id {
			continue
		}
		if !sm.meta.Segments[i].Sealed {
			return fmt.Errorf("segmgr: cannot mark active segment %d for delete", id)
		}
		if sm.meta.Segments[i].DeletePending {
			return nil
		}
		sm.meta.Segments[i].DeletePending = true
		return saveMeta(sm.dir, sm.meta)
	}
	return nil
}

// AdoptUploadedSegment records a sealed segment found in remote storage.
func (sm *SegmentManager) AdoptUploadedSegment(meta SegmentMeta) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i, existing := range sm.meta.Segments {
		if existing.ID != meta.ID {
			continue
		}
		if existing.Sealed {
			return nil
		}
		if sm.active != nil && sm.active.RecordCount() > 0 {
			return fmt.Errorf("segmgr: adopt %d conflicts with non-empty active segment", meta.ID)
		}
		if sm.active != nil {
			_ = sm.active.Close()
			sm.active = nil
			sm.activeSize = 0
		}
		_ = os.Remove(filepath.Join(sm.dir, existing.FileName))
		sm.meta.Segments = append(sm.meta.Segments[:i], sm.meta.Segments[i+1:]...)
		break
	}

	meta.Sealed = true
	meta.UploadState = UploadStateUploaded
	absent := false
	meta.LocalPresent = &absent
	sm.meta.Segments = append(sm.meta.Segments, meta)

	if meta.ID >= sm.meta.NextSegmentID {
		sm.meta.NextSegmentID = meta.ID + 1
	}

	return saveMeta(sm.dir, sm.meta)
}

// RecordUploadFailure increments the failure counter and stores a truncated
// error message. Persists meta so attempt counts survive restart, driving
// backoff convergence even across crashes.
func (sm *SegmentManager) RecordUploadFailure(id uint32, errMsg string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	const maxErrLen = 256
	if len(errMsg) > maxErrLen {
		errMsg = errMsg[:maxErrLen]
	}

	for i := range sm.meta.Segments {
		if sm.meta.Segments[i].ID != id {
			continue
		}
		sm.meta.Segments[i].UploadAttempts++
		sm.meta.Segments[i].LastUploadErr = errMsg
		return saveMeta(sm.dir, sm.meta)
	}
	return fmt.Errorf("segmgr: record upload failure: unknown segment id %d", id)
}

func (sm *SegmentManager) WALCorruptRecords() uint64 {
	if sm.wal == nil {
		return 0
	}
	return sm.wal.CorruptRecords()
}

func (sm *SegmentManager) SegmentCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.meta.Segments)
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

// DeleteSegmentFiles removes the segment data file and all known index
// sidecars from the store. Missing files are silently ignored.
// Call after RemoveSegment to clean up persistent state.
func (sm *SegmentManager) DeleteSegmentFiles(meta SegmentMeta) error {
	var first error
	for _, ext := range SegmentSidecarExts {
		if err := sm.store.Delete(meta.FileName + ext); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// DeleteSegmentFilesLocal removes only the local copies of the segment data
// file and sidecars; the remote (S3) copy, if any, is preserved. For
// LocalStore this is equivalent to DeleteSegmentFiles. Used by the local
// retention tier.
func (sm *SegmentManager) DeleteSegmentFilesLocal(meta SegmentMeta) error {
	var first error
	for _, ext := range SegmentSidecarExts {
		if err := sm.store.DeleteLocal(meta.FileName + ext); err != nil && first == nil {
			first = err
		}
	}
	return first
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
		// Active segment is now sealed and fsync'd; the WAL records that fed
		// it are durable on disk, so drop them before closing.
		_ = sm.wal.Truncate()
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

// appendSegmentWriter reopens an existing (unsealed) segment for append after
// a restart. seedMinTS/seedMaxTS carry the segment's durable time range from
// meta; they override the range a footerless rebuild would otherwise produce,
// which is intentionally unknown (see scanBlockOffsets). When the seed is zero
// (legacy meta with no persisted range) the rebuilt range is kept as-is.
func appendSegmentWriter(path string, seedMinTS, seedMaxTS int64) (*SegmentWriter, int64, error) {
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

	// Rewrite the header when the process crashed before the first flush.
	if fileSize == 0 {
		if err := sw.writeHeader(); err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("append segment: write header: %w", err)
		}
		if err := sw.bw.Flush(); err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("append segment: flush header: %w", err)
		}
		if err := f.Sync(); err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("append segment: sync header: %w", err)
		}
		fileSize = segHeaderSize
		sw.fileOffset = segHeaderSize
	}

	// Restore writer state from blocks already on disk.
	if fileSize > segHeaderSize {
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			_ = f.Close()
			return nil, 0, fmt.Errorf("append segment: scan existing blocks: %w", err)
		}
		footer := sr.Footer()
		_ = sr.Close()

		sw.recordCount = footer.RecordCount
		sw.blockOffsets = append([]int64(nil), footer.BlockOffsets...)
		sw.blockStats = append([]BlockStat(nil), footer.BlockStats...)

		if seedMinTS != 0 || seedMaxTS != 0 {
			sw.minTS = seedMinTS
			sw.maxTS = seedMaxTS
		} else {
			sw.minTS = footer.MinTS
			sw.maxTS = footer.MaxTS
		}
	}

	return sw, fileSize, nil
}
