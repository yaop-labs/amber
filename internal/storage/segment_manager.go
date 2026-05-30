// Package storage owns segment files, the WAL, and the durability protocol
// (write -> WAL append -> segment write -> periodic checkpoint -> WAL truncate).
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
	MaxRecords: 100_000,
	MaxBytes:   128 << 20,
}

// SegmentSidecarExts lists every file extension that belongs to a sealed
// segment, including the data file itself (""). Single source of truth for
// upload, delete, and fetch paths — keep in sync with index/seal_builder.go.
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

// SetStore replaces the SegmentStore used for sealed segment persistence.
// The default is LocalStore (files remain in the data directory). Call before
// any segments are sealed if using a remote store.
func (sm *SegmentManager) SetStore(s SegmentStore) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.store = s
}

// SetOnSealComplete registers a callback fired after onSeal (and all index
// builds it triggers) finishes. Use this to upload sealed files to a remote
// store: by the time it is called, all sidecars are on local disk.
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

	// Truncate the segment file back to the last fsync'd offset. Any bytes
	// past that point came from a bw.Flush + OS page-cache write that never
	// reached fsync, so they may be partial or torn; drop them and let WAL
	// replay rebuild the missing tail.
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

	seq, err := sm.wal.Write(payload)
	if err != nil {
		return fmt.Errorf("segmgr: wal write: %w", err)
	}

	blocksBefore := sm.active.BlockCount()
	if err := sm.active.WriteRecord(data, ts); err != nil {
		return fmt.Errorf("segmgr: segment write: %w", err)
	}
	sm.activeSize += int64(len(data))

	// If WriteRecord just flushed a block, sync the segment and drop the WAL
	// records that are now durable. Otherwise the records stay in the WAL
	// (the only durable copy) and ride along to the next block flush.
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

	firstSeq, err := sm.wal.WriteBatchTS(items)
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

// checkpoint syncs the active segment so that all blocks flushed up to and
// including the record with seq=lastSyncedSeq are durably on disk, and persists
// the new sync watermark in meta.
//
// It deliberately does NOT truncate the WAL. The watermark covers only records
// already in a flushed, fsync'd block; records written after lastSyncedSeq
// (a partial block still buffered in memory) are durable solely in the WAL.
// Truncating here would drop them — the data-loss window that bit WriteBatch,
// whose trailing items land in blockBuf after the last mid-batch flush. The WAL
// is instead truncated only at rotate/Close, where the active segment is sealed
// and fsync'd in its entirety, so every record that fed it is durable.
//
// The WAL therefore retains every record written since the last rotate (bounded
// by the segment rotation policy). On restart, replayWAL truncates the file
// back to LastSyncedSize and re-applies WAL records with seq > LastSyncedSeq;
// records with seq <= LastSyncedSeq are already in durable blocks and skipped,
// so there is neither loss nor duplication.
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
	// Take metadata snapshots from the live writer before Close so we don't
	// have to reopen the file as a SegmentReader under the write lock.
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
			if onSeal != nil {
				onSeal(sealedMeta)
			}
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
		if s.Sealed {
			result = append(result, s)
		}
	}
	return result
}

// PendingUploads returns sealed segments whose UploadState is not Uploaded.
// Used by the background uploader to find work after seal and after restart.
// Returned slice is a snapshot; callers may mutate it freely.
func (sm *SegmentManager) PendingUploads() []SegmentMeta {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var pending []SegmentMeta
	for _, s := range sm.meta.Segments {
		if s.Sealed && s.UploadState != UploadStateUploaded {
			pending = append(pending, s)
		}
	}
	return pending
}

// MarkUploaded transitions a sealed segment to UploadStateUploaded and
// persists the meta. Idempotent: calling on an already-uploaded segment is
// a no-op. Returns an error only if the segment ID is unknown or meta
// persistence fails.
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

// AdoptUploadedSegment inserts a sealed, already-uploaded segment into the
// manager's metadata. Used by the bootstrap S3 reconcile path to surface
// segments that exist in remote storage but not in local meta.json (e.g.
// after node migration). The caller must ensure the segment's data file
// and required sidecars are present on local disk before calling — this
// method only mutates meta.
//
// Conflict handling:
//   - If an entry with the same ID is already Sealed, returns nil (reconcile
//     is idempotent — we've seen this segment before).
//   - If an entry with the same ID is Active (not Sealed) AND empty (no
//     records), the active is discarded: its file is removed, NextSegmentID
//     advances past the adopted ID, and a fresh active will be created on
//     the next write. This resolves the bootstrap-creates-seg_00000001-then-
//     S3-has-seg_00000001 collision on a fresh node.
//   - If the active is non-empty, returns an error: we won't drop on-disk
//     writes silently. The caller should flush and seal first.
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
		// Discard the empty active and let the adopt below replace it.
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

	// If the file is empty the header was never flushed before the crash
	// (OpenSegmentWriter writes it into a bufio.Writer; if the process dies
	// before bw.Flush the file stays 0 bytes). Write it now so WAL replay
	// doesn't produce a headerless segment that OpenSegmentReader rejects.
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

		sw.recordCount = footer.RecordCount
		sw.blockOffsets = append([]int64(nil), footer.BlockOffsets...)
		sw.blockStats = append([]BlockStat(nil), footer.BlockStats...)

		// Block offsets, record count and per-block ID stats are rebuilt
		// correctly from the file. The time range is not — seed it from meta.
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
