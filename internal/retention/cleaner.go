// Package retention enforces age, byte-size, and segment-count limits on
// sealed segments by deleting oldest first.
package retention

import (
	"log/slog"
	"sort"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/internal/storage"
)

// Policy is the retention policy for one stream.
// Local limits remove local files. Global limits remove the segment.
type Policy struct {
	LocalMaxAge   time.Duration
	LocalMaxBytes int64

	MaxAge        time.Duration
	MaxTotalBytes int64
	MaxSegments   int
}

func (p Policy) hasLocalTier() bool { return p.LocalMaxAge > 0 || p.LocalMaxBytes > 0 }

type Cleaner struct {
	manager         *storage.SegmentManager
	sparse          *index.SparseIndex
	policy          Policy
	dataDir         string
	stream          string // "logs" or "spans"
	log             *slog.Logger
	onDelete        func(storage.SegmentMeta)
	requireUploaded bool
}

// NewCleaner returns a Cleaner for one stream.
func NewCleaner(
	manager *storage.SegmentManager,
	sparse *index.SparseIndex,
	policy Policy,
	dataDir string,
	stream string,
	log *slog.Logger,
) *Cleaner {
	return &Cleaner{
		manager: manager,
		sparse:  sparse,
		policy:  policy,
		dataDir: dataDir,
		stream:  stream,
		log:     log,
	}
}

func (c *Cleaner) SetOnDelete(fn func(storage.SegmentMeta)) {
	c.onDelete = fn
}

// RequireUploaded requires UploadStateUploaded before global deletion.
func (c *Cleaner) RequireUploaded(v bool) {
	c.requireUploaded = v
}

// Run applies local and global retention and returns the number of segments
// touched. Per-segment errors are logged and do not stop the run.
func (c *Cleaner) Run() (int, error) {
	segments := c.manager.SegmentsForRetention()
	if len(segments) == 0 {
		return 0, nil
	}

	touched := 0

	if c.policy.hasLocalTier() {
		var localCandidates []storage.SegmentMeta
		for _, s := range segments {
			if !s.DeletePending && s.UploadState == storage.UploadStateUploaded && s.HasLocalCopy() {
				localCandidates = append(localCandidates, s)
			}
		}
		evicted := c.runLocalEviction(localCandidates)
		touched += evicted
	}

	globalCandidates := segments
	if c.requireUploaded {
		eligible := globalCandidates[:0:0]
		for _, s := range globalCandidates {
			if s.DeletePending || s.UploadState == storage.UploadStateUploaded {
				eligible = append(eligible, s)
			}
		}
		globalCandidates = eligible
	}
	if len(globalCandidates) == 0 {
		return touched, nil
	}

	toDelete := c.selectForDeletion(globalCandidates)
	if len(toDelete) == 0 {
		return touched, nil
	}

	for _, item := range toDelete {
		if err := c.deleteSegment(item.seg); err != nil {
			c.log.Error("failed to delete segment",
				"file", item.seg.FileName,
				"err", err,
			)
			continue
		}
		touched++
		selfobs.RetentionEvictions.WithLabelValues(item.reason).Inc()
	}

	return touched, nil
}

// runLocalEviction removes local copies selected by the local limits.
func (c *Cleaner) runLocalEviction(candidates []storage.SegmentMeta) int {
	if len(candidates) == 0 {
		return 0
	}

	now := time.Now().UnixNano()
	type localEvict struct {
		seg    storage.SegmentMeta
		reason string
	}
	var picks []localEvict

	if c.policy.LocalMaxAge > 0 {
		cutoff := now - c.policy.LocalMaxAge.Nanoseconds()
		for _, s := range candidates {
			if s.MaxTS < cutoff {
				picks = append(picks, localEvict{seg: s, reason: "local_max_age"})
			}
		}
	}

	remaining := candidates[:0:0]
	pickedSet := make(map[uint32]struct{}, len(picks))
	for _, p := range picks {
		pickedSet[p.seg.ID] = struct{}{}
	}
	for _, s := range candidates {
		if _, ok := pickedSet[s.ID]; !ok {
			remaining = append(remaining, s)
		}
	}

	if c.policy.LocalMaxBytes > 0 {
		sort.SliceStable(remaining, func(i, j int) bool {
			return remaining[i].MaxTS < remaining[j].MaxTS
		})
		var totalBytes int64
		for _, s := range remaining {
			totalBytes += s.SizeBytes
		}
		for i := 0; i < len(remaining) && totalBytes > c.policy.LocalMaxBytes; i++ {
			picks = append(picks, localEvict{seg: remaining[i], reason: "local_max_bytes"})
			totalBytes -= remaining[i].SizeBytes
		}
	}

	if len(picks) == 0 {
		return 0
	}

	evicted := 0
	for _, p := range picks {
		if err := c.evictLocal(p.seg); err != nil {
			c.log.Error("local eviction failed",
				"file", p.seg.FileName,
				"err", err,
			)
			continue
		}
		evicted++
		selfobs.RetentionLocalEvictions.WithLabelValues(c.stream, p.reason).Inc()
	}
	return evicted
}

// evictLocal removes one segment's local files.
// It marks metadata before deleting files so a crash cannot leave metadata
// claiming a local copy that is gone.
func (c *Cleaner) evictLocal(seg storage.SegmentMeta) error {
	if err := c.manager.MarkLocalEvicted(seg.ID); err != nil {
		return err
	}
	if err := c.manager.DeleteSegmentFilesLocal(seg); err != nil {
		return err
	}
	c.log.Info("local segment evicted",
		"kind", c.stream,
		"file", seg.FileName,
		"size_mb", seg.SizeBytes/1024/1024,
		"max_ts", seg.MaxTS,
	)
	return nil
}

type evictionCandidate struct {
	seg    storage.SegmentMeta
	reason string
}

func (c *Cleaner) selectForDeletion(segments []storage.SegmentMeta) []evictionCandidate {
	var toDelete []evictionCandidate
	now := time.Now().UnixNano()

	for _, seg := range segments {
		if seg.DeletePending {
			toDelete = append(toDelete, evictionCandidate{seg: seg, reason: "delete_pending"})
		}
	}
	segments = filterOutCandidates(segments, toDelete)

	if c.policy.MaxAge > 0 {
		cutoff := now - c.policy.MaxAge.Nanoseconds()
		for _, seg := range segments {
			if seg.MaxTS < cutoff {
				toDelete = append(toDelete, evictionCandidate{seg: seg, reason: "max_age"})
			}
		}
	}

	remaining := filterOutCandidates(segments, toDelete)

	sort.SliceStable(remaining, func(i, j int) bool {
		return remaining[i].MaxTS < remaining[j].MaxTS
	})

	if c.policy.MaxSegments > 0 && len(remaining) > c.policy.MaxSegments {
		excess := len(remaining) - c.policy.MaxSegments
		for _, s := range remaining[:excess] {
			toDelete = append(toDelete, evictionCandidate{seg: s, reason: "max_segments"})
		}
		remaining = remaining[excess:]
	}

	if c.policy.MaxTotalBytes > 0 {
		var totalBytes int64
		for _, seg := range remaining {
			totalBytes += seg.SizeBytes
		}
		for i := 0; i < len(remaining) && totalBytes > c.policy.MaxTotalBytes; i++ {
			toDelete = append(toDelete, evictionCandidate{seg: remaining[i], reason: "max_total_bytes"})
			totalBytes -= remaining[i].SizeBytes
		}
	}

	return toDelete
}

func filterOutCandidates(all []storage.SegmentMeta, exclude []evictionCandidate) []storage.SegmentMeta {
	if len(exclude) == 0 {
		return all
	}
	excludeSet := make(map[uint32]struct{}, len(exclude))
	for _, c := range exclude {
		excludeSet[c.seg.ID] = struct{}{}
	}
	result := make([]storage.SegmentMeta, 0, len(all))
	for _, s := range all {
		if _, skip := excludeSet[s.ID]; !skip {
			result = append(result, s)
		}
	}
	return result
}

func (c *Cleaner) deleteSegment(seg storage.SegmentMeta) error {
	if err := c.manager.BeginDeleteSegment(seg.ID); err != nil {
		return err
	}

	c.sparse.Remove(seg.ID)
	if err := c.sparse.Save(c.dataDir); err != nil {
		c.log.Warn("sparse index save failed", "err", err)
	}

	if err := c.manager.DeleteSegmentFiles(seg); err != nil {
		return err
	}

	if err := c.manager.RemoveSegment(seg.ID); err != nil {
		return err
	}

	if c.onDelete != nil {
		c.onDelete(seg)
	}

	c.log.Info("segment deleted",
		"file", seg.FileName,
		"records", seg.RecordCount,
		"size_mb", seg.SizeBytes/1024/1024,
		"min_ts", seg.MinTS,
		"max_ts", seg.MaxTS,
	)

	return nil
}

func (c *Cleaner) StartLoop(interval time.Duration, done <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			n, err := c.Run()
			if err != nil {
				c.log.Error("retention run failed", "err", err)
			} else if n > 0 {
				c.log.Info("retention cleaned segments", "count", n)
			}
		}
	}
}

func filterOut(all, exclude []storage.SegmentMeta) []storage.SegmentMeta {
	excludeSet := make(map[uint32]struct{}, len(exclude))
	for _, s := range exclude {
		excludeSet[s.ID] = struct{}{}
	}
	result := make([]storage.SegmentMeta, 0, len(all))
	for _, s := range all {
		if _, skip := excludeSet[s.ID]; !skip {
			result = append(result, s)
		}
	}
	return result
}
