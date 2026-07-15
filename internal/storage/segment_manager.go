// Package storage owns segment files, the WAL, and the durability protocol
// (write -> WAL append -> segment write -> periodic checkpoint -> WAL truncate).
package storage

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/yaop-labs/amber/internal/selfobs"
)

// RotationPolicy decides when the active segment is sealed and a new one
// started: whichever of the record-count or byte-size limit is hit first.
type RotationPolicy struct {
	MaxRecords uint64
	// MaxBytes counts uncompressed serialized record payload admitted to the
	// active segment. It is checked after a batch and is not an on-disk size cap.
	MaxBytes int64
}

var DefaultRotationPolicy = RotationPolicy{
	MaxRecords: 100_000,
	MaxBytes:   128 << 20,
}

// SegmentSidecarExts lists the files belonging to a sealed segment.
var SegmentSidecarExts = []string{"", ".bidx", ".fidx", ".filt", ".fts.filt", ".pidx", ".cidx"}

// SegmentManager owns the active segment and WAL and drives the durability
// protocol: it appends to the WAL, writes to the active segment, rotates and
// seals full segments (building their index sidecars on one background worker),
// and checkpoints/truncates the WAL. It is safe for concurrent use.
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
	failed         error
	closed         bool
	closeOnce      sync.Once
	closeErr       error
	// fault is test-only transition injection. Production managers leave it nil.
	fault func(point string) error

	// Seal callbacks run on one background worker, strictly in seal order.
	// A goroutine per seal (the previous design) let slow index builds pile
	// up without bound under sustained ingest: each in-flight build held its
	// segment's indexes in memory and burned CPU, starving the ingest path,
	// which made builds even slower - a feedback loop ending in swap death
	// (found by the obsbench W1 run: 8.8 GB heap at 10M records). A backlog
	// in this queue costs ~100 bytes per entry; queries against not-yet-
	// indexed segments fall back to scans, which is degradation, not failure.
	sealMu    sync.Mutex
	sealQueue []SegmentMeta
	sealWake  chan struct{}
	sealStop  chan struct{}
	sealDone  chan struct{}
}

var ErrSegmentManagerClosed = errors.New("segment manager is closed")

// operational rejects work after a terminal transition failure or Close.
// Caller holds sm.mu.
func (sm *SegmentManager) operational() error {
	if sm.failed != nil {
		return sm.failed
	}
	if sm.closed {
		return ErrSegmentManagerClosed
	}
	if sm.active == nil {
		return errors.New("segmgr: no active segment")
	}
	return nil
}

// failStop makes a transition error sticky. Caller holds sm.mu.
func (sm *SegmentManager) failStop(err error) error {
	if sm.failed == nil {
		sm.failed = err
	}
	return sm.failed
}

func (sm *SegmentManager) injectFault(point string) error {
	if sm.fault == nil {
		return nil
	}
	if err := sm.fault(point); err != nil {
		return fmt.Errorf("segmgr: injected at %s: %w", point, err)
	}
	return nil
}

func (sm *SegmentManager) saveTransitionMeta(transition string) error {
	return saveMetaWithFault(sm.dir, sm.meta, func(stage string) error {
		return sm.injectFault(transition + ":meta:" + stage)
	})
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

// SetOnSeal registers the callback that builds a sealed segment's index
// sidecars; it runs on the seal worker in seal order.
func (sm *SegmentManager) SetOnSeal(fn func(meta SegmentMeta)) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.onSeal = fn
}

// OpenSegmentManager opens the store at dir: it loads the metadata, recovers
// the active segment by truncating it to the last fsynced size and replaying
// the WAL tail, and starts the seal worker.
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

	if _, err := os.Stat(segPath); err != nil {
		if os.IsNotExist(err) {
			// Metadata says this active segment exists. The WAL may be the only
			// surviving copy of acknowledged records, so never erase it while
			// guessing how the file disappeared.
			return fmt.Errorf("segmgr: active segment %s is missing; refusing to truncate recovery WAL", activeMeta.FileName)
		}
		return fmt.Errorf("segmgr: stat active segment %s: %w", activeMeta.FileName, err)
	}

	// Truncate the segment file back to the last fsynced offset. WAL replay
	// rebuilds any missing tail. LastSyncedSize == 0 means no checkpoint ever
	// ran for this segment, so nothing past the header is known durable and
	// the file is cut back to the bare header. Treating zero as "skip the
	// truncate" kept the crash-surviving records in place and then replayed
	// the entire WAL on top of them - every record duplicated (found by the
	// obsbench kill -9 test).
	syncedSize := max(activeMeta.LastSyncedSize, segHeaderSize)
	if info, err := os.Stat(segPath); err == nil && info.Size() > syncedSize {
		if err := os.Truncate(segPath, syncedSize); err != nil {
			return fmt.Errorf("segmgr: truncate to last synced size: %w", err)
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
	if err := sm.injectFault("create:before_segment_file"); err != nil {
		return err
	}
	id := sm.meta.NextSegmentID
	fileName := segmentFileName(id)
	segPath := filepath.Join(sm.dir, fileName)

	writer, err := OpenSegmentWriter(segPath)
	if errors.Is(err, fs.ErrExist) {
		// Crash window: the previous run died after creating this segment
		// file but before saveMeta recorded it (found by the rotation-storm
		// kill -9 test - recovery refused to open with "file exists").
		// Such an orphan holds no acked data: rotate truncates the WAL
		// before createNewSegment, and no Write is accepted until saveMeta
		// below returns. A failed save closes the writer and may leave an
		// empty footer, so size > header is not by itself evidence of data.
		// Scan before deleting; any record or unreadable structure fail-stops.
		if info, statErr := os.Stat(segPath); statErr == nil && info.Size() > segHeaderSize {
			empty, checkErr := segmentFileEmpty(segPath)
			if checkErr != nil || !empty {
				return fmt.Errorf("segmgr: segment file %s exists with %d bytes but is not a verified empty orphan — refusing to overwrite: %v", fileName, info.Size(), checkErr)
			}
		}
		if rmErr := os.Remove(segPath); rmErr != nil {
			return fmt.Errorf("segmgr: remove orphan segment %s: %w", fileName, rmErr)
		}
		writer, err = OpenSegmentWriter(segPath)
	}
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

	if err := sm.saveTransitionMeta("create"); err != nil {
		_ = writer.Close()
		return fmt.Errorf("segmgr: save meta after create: %w", err)
	}

	sm.active = writer
	sm.activeSize = 0
	return nil
}

func segmentFileEmpty(path string) (bool, error) {
	sr, err := OpenSegmentReader(path, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = sr.Close() }()
	count := 0
	if err := sr.Scan(func([]byte) error {
		count++
		return nil
	}); err != nil {
		return false, err
	}
	return count == 0, nil
}

// Write appends one record: WAL append (durable), active-segment write, then
// rotation if the completed write reaches a policy threshold.
func (sm *SegmentManager) Write(data []byte, ts int64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.operational(); err != nil {
		return err
	}

	payload := makeWALPayload(ts, data)

	walStart := time.Now()
	seq, err := sm.wal.Write(payload)
	selfobs.WALWriteDuration.WithLabelValues("single").Observe(time.Since(walStart).Seconds())
	selfobs.WALWrites.WithLabelValues("single").Inc()
	if err != nil {
		err = fmt.Errorf("segmgr: wal write: %w", err)
		if errors.Is(err, ErrWALRecordTooLarge) {
			return err
		}
		return sm.failStop(err)
	}

	blocksBefore := sm.active.BlockCount()
	if err := sm.active.WriteRecord(data, ts); err != nil {
		return sm.failStop(fmt.Errorf("segmgr: segment write: %w", err))
	}
	sm.activeSize += int64(len(data))

	// Sync when WriteRecord flushes a block.
	if sm.active.BlockCount() > blocksBefore {
		if err := sm.checkpoint(seq); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: checkpoint: %w", err))
		}
	}

	if sm.shouldRotate() {
		if err := sm.rotate(); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: rotate: %w", err))
		}
	}

	return nil
}

// WriteBatch appends many records under one WAL fsync (group commit). Rotation
// policy is evaluated after the whole batch, so a segment may overshoot either
// threshold by at most one admitted batch.
func (sm *SegmentManager) WriteBatch(items []BatchItem) error {
	if len(items) == 0 {
		return nil
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.operational(); err != nil {
		return err
	}

	walStart := time.Now()
	firstSeq, err := sm.wal.WriteBatchTS(items)
	selfobs.WALWriteDuration.WithLabelValues("batch").Observe(time.Since(walStart).Seconds())
	selfobs.WALWrites.WithLabelValues("batch").Inc()
	if err != nil {
		err = fmt.Errorf("segmgr: wal batch: %w", err)
		if errors.Is(err, ErrWALRecordTooLarge) {
			return err
		}
		return sm.failStop(err)
	}

	var (
		sawFlush     bool
		lastFlushSeq uint64
	)
	for i, item := range items {
		before := sm.active.BlockCount()
		if err := sm.active.WriteRecord(item.Data, item.TS); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: segment write: %w", err))
		}
		sm.activeSize += int64(len(item.Data))
		if sm.active.BlockCount() > before {
			sawFlush = true
			lastFlushSeq = firstSeq + uint64(i)
		}
	}

	if sawFlush {
		if err := sm.checkpoint(lastFlushSeq); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: checkpoint: %w", err))
		}
	}

	if sm.shouldRotate() {
		if err := sm.rotate(); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: rotate: %w", err))
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

// BatchItem is one record for WriteBatch: its serialized data and event time.
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

	if err := sm.saveTransitionMeta("rotate"); err != nil {
		return err
	}

	// Truncate the WAL BEFORE creating the next segment. The sealed segment is
	// fsync'd via SegmentWriter.Close, so every WAL record that fed it is
	// durable. A crash between here and createNewSegment leaves meta with no
	// unsealed segment, which replayWAL handles by dropping orphan records.
	// The reverse order (create first, truncate last) had a crash window where
	// replay would re-apply the entire WAL into the fresh empty segment,
	// duplicating every record of the just-sealed one.
	if err := sm.injectFault("rotate:before_wal_truncate"); err != nil {
		return err
	}
	if err := sm.wal.Truncate(); err != nil {
		return fmt.Errorf("segmgr: rotate truncate wal: %w", err)
	}

	if sm.onSeal != nil || sm.onSealComplete != nil {
		sm.enqueueSeal(sealedMeta)
	}

	return sm.createNewSegment()
}

// enqueueSeal hands the sealed segment to the single seal worker, starting
// it on first use. Caller holds sm.mu.
func (sm *SegmentManager) enqueueSeal(meta SegmentMeta) {
	sm.sealMu.Lock()
	if sm.sealStop == nil {
		sm.sealStop = make(chan struct{})
		sm.sealDone = make(chan struct{})
		sm.sealWake = make(chan struct{}, 1)
		go sm.runSealWorker()
	}
	sm.sealQueue = append(sm.sealQueue, meta)
	sm.sealMu.Unlock()
	select {
	case sm.sealWake <- struct{}{}:
	default:
	}
}

// SealBacklog reports queued-but-unbuilt seals; worth a gauge.
func (sm *SegmentManager) SealBacklog() int {
	sm.sealMu.Lock()
	defer sm.sealMu.Unlock()
	return len(sm.sealQueue)
}

func (sm *SegmentManager) runSealWorker() {
	defer close(sm.sealDone)
	for {
		sm.sealMu.Lock()
		var meta SegmentMeta
		have := len(sm.sealQueue) > 0
		if have {
			meta = sm.sealQueue[0]
			sm.sealQueue = sm.sealQueue[1:]
		}
		sm.sealMu.Unlock()

		if !have {
			select {
			case <-sm.sealWake:
				continue
			case <-sm.sealStop:
				return
			}
		}

		// Hooks are registered once at startup; read them per job so a
		// late SetOnSeal (bootstrap wiring) is still observed.
		sm.mu.RLock()
		onSeal, onSealComplete := sm.onSeal, sm.onSealComplete
		sm.mu.RUnlock()

		sealStart := time.Now()
		if onSeal != nil {
			onSeal(meta)
		}
		selfobs.SealDuration.WithLabelValues(filepath.Base(sm.dir)).Observe(time.Since(sealStart).Seconds())
		if onSealComplete != nil {
			onSealComplete(meta)
		}

		select {
		case <-sm.sealStop:
			return
		default:
		}
	}
}

// stopSealWorker ends the worker after the in-flight job; queued seals are
// dropped - bootstrap rebuilds missing sidecar indexes lazily on next open.
func (sm *SegmentManager) stopSealWorker() {
	sm.sealMu.Lock()
	stop, done := sm.sealStop, sm.sealDone
	sm.sealMu.Unlock()
	if stop == nil {
		return
	}
	select {
	case <-stop:
	default:
		close(stop)
	}
	<-done
}

// Rotate seals the active segment and starts a fresh one, even if the policy
// limit has not been reached.
func (sm *SegmentManager) Rotate() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if err := sm.operational(); err != nil {
		return err
	}
	if sm.active.RecordCount() == 0 {
		return nil
	}
	if err := sm.rotate(); err != nil {
		return sm.failStop(fmt.Errorf("segmgr: rotate: %w", err))
	}
	return nil
}

// Segments returns a snapshot of all segment metadata, sealed and active.
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
		seg.LocalDeletePending = false
		return saveMeta(sm.dir, sm.meta)
	}
	return fmt.Errorf("segmgr: mark local evicted: unknown segment id %d", id)
}

// BeginLocalEviction durably records an in-progress local-tier eviction. It
// never permits eviction of the only copy. CompleteLocalEviction must be
// called after all local segment files have been removed.
func (sm *SegmentManager) BeginLocalEviction(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i := range sm.meta.Segments {
		if sm.meta.Segments[i].ID != id {
			continue
		}
		seg := &sm.meta.Segments[i]
		if seg.UploadState != UploadStateUploaded {
			return fmt.Errorf("segmgr: begin local eviction: segment %d is not uploaded", id)
		}
		if !seg.HasLocalCopy() && !seg.LocalDeletePending {
			return nil
		}
		if seg.LocalDeletePending {
			return nil
		}
		seg.LocalDeletePending = true
		return saveMeta(sm.dir, sm.meta)
	}
	return fmt.Errorf("segmgr: begin local eviction: unknown segment id %d", id)
}

// CompleteLocalEviction commits the local cache state after local unlink has
// succeeded. Repeating it is harmless, which lets a crash at any transition
// point converge on the next retention run.
func (sm *SegmentManager) CompleteLocalEviction(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	for i := range sm.meta.Segments {
		if sm.meta.Segments[i].ID != id {
			continue
		}
		seg := &sm.meta.Segments[i]
		if seg.UploadState != UploadStateUploaded {
			return fmt.Errorf("segmgr: complete local eviction: segment %d is not uploaded", id)
		}
		if !seg.HasLocalCopy() && !seg.LocalDeletePending {
			return nil
		}
		absent := false
		seg.LocalPresent = &absent
		seg.LocalDeletePending = false
		return saveMeta(sm.dir, sm.meta)
	}
	return fmt.Errorf("segmgr: complete local eviction: unknown segment id %d", id)
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

	for _, existing := range sm.meta.Segments {
		if existing.ID != meta.ID {
			continue
		}
		if existing.Sealed {
			return nil
		}
		return fmt.Errorf("segmgr: adopt %d conflicts with active segment; call PrepareAdoptUploadedSegment before download", meta.ID)
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

// PrepareAdoptUploadedSegment vacates an empty active segment whose ID/path
// collides with a remote sealed segment before that remote file is downloaded.
// It immediately creates a fresh active segment with the next ID, so failed or
// partial reconciliation cannot leave ingest without an active writer.
func (sm *SegmentManager) PrepareAdoptUploadedSegment(id uint32) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.failed != nil {
		return sm.failed
	}
	if sm.closed {
		return ErrSegmentManagerClosed
	}

	for i, existing := range sm.meta.Segments {
		if existing.ID != id || existing.Sealed {
			continue
		}
		if sm.active == nil || sm.active.RecordCount() > 0 {
			return fmt.Errorf("segmgr: prepare adopt %d conflicts with non-empty or missing active writer", id)
		}
		if err := sm.active.Close(); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: prepare adopt close empty active: %w", err))
		}
		sm.active = nil
		sm.activeSize = 0
		if err := os.Remove(filepath.Join(sm.dir, existing.FileName)); err != nil && !os.IsNotExist(err) {
			return sm.failStop(fmt.Errorf("segmgr: prepare adopt remove empty active: %w", err))
		}
		sm.meta.Segments = append(sm.meta.Segments[:i], sm.meta.Segments[i+1:]...)
		if err := saveMeta(sm.dir, sm.meta); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: prepare adopt save meta: %w", err))
		}
		if err := sm.createNewSegment(); err != nil {
			return sm.failStop(fmt.Errorf("segmgr: prepare adopt create replacement active: %w", err))
		}
		return nil
	}
	return nil
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

// WALCorruptRecords returns how many malformed records the last WAL replay
// skipped, for surfacing as a health metric.
func (sm *SegmentManager) WALCorruptRecords() uint64 {
	if sm.wal == nil {
		return 0
	}
	return sm.wal.CorruptRecords()
}

// SegmentCount returns the number of segments tracked.
func (sm *SegmentManager) SegmentCount() int {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.meta.Segments)
}

// ManagerStats is a consistent operational snapshot of one segment manager.
// SegmentBytes is physical container size and excludes the WAL and sidecars.
type ManagerStats struct {
	SealedSegments    int
	ActiveSegment     bool
	ActiveRecords     uint64
	TotalRecords      uint64
	SegmentBytes      int64
	WALBytes          int64
	WALCorruptRecords uint64
}

// Stats returns record and disk-usage counters without scanning segment data.
func (sm *SegmentManager) Stats() (ManagerStats, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var stats ManagerStats
	for _, segment := range sm.meta.Segments {
		if segment.Sealed {
			stats.SealedSegments++
			stats.TotalRecords += segment.RecordCount
			stats.SegmentBytes += segment.SizeBytes
			continue
		}
		stats.ActiveSegment = true
		if sm.active != nil {
			stats.ActiveRecords = sm.active.RecordCount()
			stats.TotalRecords += stats.ActiveRecords
		}
		info, err := os.Stat(filepath.Join(sm.dir, segment.FileName)) //nolint:gosec
		if err != nil {
			return ManagerStats{}, fmt.Errorf("segmgr: stat active segment: %w", err)
		}
		stats.SegmentBytes += info.Size()
	}
	if sm.wal != nil {
		walBytes, err := sm.wal.Size()
		if err != nil {
			return ManagerStats{}, fmt.Errorf("segmgr: read WAL size: %w", err)
		}
		stats.WALBytes = walBytes
		stats.WALCorruptRecords = sm.wal.CorruptRecords()
	}
	return stats, nil
}

// RemoveSegment deletes a sealed segment and its sidecars and drops it from the
// metadata, the terminal step of retention.
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

// Flush makes buffered active-segment bytes queryable. WAL durability is
// established before segment writes; this method does not advance metadata's
// checkpoint watermark.
func (sm *SegmentManager) Flush() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if err := sm.operational(); err != nil {
		return err
	}
	if err := sm.active.Flush(); err != nil {
		return sm.failStop(fmt.Errorf("segmgr: flush active: %w", err))
	}
	return nil
}

// ActiveRecordCount returns the record count of the current active segment.
func (sm *SegmentManager) ActiveRecordCount() uint64 {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	if sm.active == nil {
		return 0
	}
	return sm.active.RecordCount()
}

// ActiveBlockIndex returns the active segment's block index hint for warm
// reads, or false if fileName is not the current active segment.
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

// ActiveSegmentMeta returns the metadata of the current active segment, or
// false when there is none.
func (sm *SegmentManager) ActiveSegmentMeta() (SegmentMeta, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, s := range sm.meta.Segments {
		if !s.Sealed {
			// Persisted active metadata is only a checkpoint. WAL replay and
			// post-checkpoint writes may have widened the live range without
			// updating meta.json yet, so query bootstrap must use the writer's
			// authoritative in-process state.
			if sm.active != nil {
				s.RecordCount = sm.active.RecordCount()
				if s.RecordCount > 0 {
					s.MinTS, s.MaxTS = sm.active.TimeRange()
				}
			}
			return s, true
		}
	}
	return SegmentMeta{}, false
}

// SegmentPath returns the on-disk path of a segment's data file.
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

// Close stops the seal worker (draining the queue), flushes and closes the
// active segment and WAL.
func (sm *SegmentManager) Close() error {
	sm.closeOnce.Do(func() {
		sm.closeErr = sm.close()
	})
	return sm.closeErr
}

func (sm *SegmentManager) close() error {
	// Stop the seal worker before taking sm.mu: an in-flight seal callback
	// reads hooks under sm.mu.RLock, so waiting for it while holding the
	// write lock would deadlock.
	sm.stopSealWorker()

	sm.mu.Lock()
	defer sm.mu.Unlock()
	var errs []error
	preexistingFailure := sm.failed
	if preexistingFailure != nil {
		errs = append(errs, preexistingFailure)
	}

	if sm.active != nil {
		activeCloseErr := sm.active.Close()
		if activeCloseErr != nil {
			activeCloseErr = fmt.Errorf("segmgr: close active: %w", activeCloseErr)
			errs = append(errs, activeCloseErr)
			_ = sm.failStop(activeCloseErr)
		}

		// Never seal metadata or destroy the WAL after a prior ambiguous
		// transition error. On reopen, the durable watermark rolls the active
		// file back and WAL replay reconstructs the acknowledged suffix.
		if preexistingFailure == nil && activeCloseErr == nil {
			var transitionErr error
			foundActiveMeta := false
			for i := range sm.meta.Segments {
				if !sm.meta.Segments[i].Sealed {
					foundActiveMeta = true
					sm.meta.Segments[i].Sealed = true
					sm.meta.Segments[i].RecordCount = sm.active.RecordCount()
					sm.meta.Segments[i].MinTS, sm.meta.Segments[i].MaxTS = sm.active.TimeRange()

					segPath := filepath.Join(sm.dir, sm.meta.Segments[i].FileName)
					if info, err := os.Stat(segPath); err == nil {
						sm.meta.Segments[i].SizeBytes = info.Size()
					} else {
						transitionErr = fmt.Errorf("segmgr: stat active on close: %w", err)
					}
					break
				}
			}
			if !foundActiveMeta {
				transitionErr = errors.New("segmgr: active writer has no metadata on close")
			}
			if transitionErr != nil {
				errs = append(errs, transitionErr)
				_ = sm.failStop(transitionErr)
			} else if err := sm.saveTransitionMeta("close"); err != nil {
				err = fmt.Errorf("segmgr: save meta on close: %w", err)
				errs = append(errs, err)
				_ = sm.failStop(err)
			} else if err := sm.injectFault("close:before_wal_truncate"); err != nil {
				errs = append(errs, err)
				_ = sm.failStop(err)
			} else if err := sm.wal.Truncate(); err != nil {
				err = fmt.Errorf("segmgr: truncate wal on close: %w", err)
				errs = append(errs, err)
				_ = sm.failStop(err)
			}
		}
		sm.active = nil
	}

	if err := sm.wal.Close(); err != nil {
		errs = append(errs, fmt.Errorf("segmgr: close wal: %w", err))
	}
	sm.closed = true
	return errors.Join(errs...)
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
