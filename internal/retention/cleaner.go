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

// Policy is one stream's two-tier retention. Local thresholds evict the disk
// copy but leave the remote (S3) object; global thresholds delete the segment
// everywhere. Zero values mean disabled for that threshold. The cleaner runs
// the local pass first, then the global pass.
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
	stream          string // "logs" | "spans", labels local-evict metrics
	log             *slog.Logger
	onDelete        func(storage.SegmentMeta)
	requireUploaded bool
}

// NewCleaner builds a Cleaner. stream labels per-stream metrics; pass "logs"
// or "spans". An empty stream is allowed for legacy/test callers; the local
// eviction counter will then carry an empty kind label, which is harmless.
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

// RequireUploaded gates retention on the segment having reached
// UploadStateUploaded. Enable on nodes using a remote SegmentStore so a
// transient S3 outage doesn't delete the only copy of a segment that
// hasn't been uploaded yet. Default (off) preserves local-only behavior.
func (c *Cleaner) RequireUploaded(v bool) {
	c.requireUploaded = v
}

// Run executes both retention stages. Returns total segments touched (local
// evictions + global deletions). Errors during a single segment are logged
// and counted as "not touched"; the loop continues to the next.
func (c *Cleaner) Run() (int, error) {
	segments := c.manager.Segments()
	if len(segments) == 0 {
		return 0, nil
	}

	touched := 0

	// Stage 1: local-tier eviction. Only segments that are durably uploaded
	// AND still have a local copy are candidates. No RequireUploaded check
	// needed here — the Uploaded gate is intrinsic to local eviction's
	// safety story: you cannot drop the local file if no remote copy exists.
	if c.policy.hasLocalTier() {
		var localCandidates []storage.SegmentMeta
		for _, s := range segments {
			if s.UploadState == storage.UploadStateUploaded && s.HasLocalCopy() {
				localCandidates = append(localCandidates, s)
			}
		}
		evicted := c.runLocalEviction(localCandidates)
		touched += evicted
	}

	// Stage 2: global retention. Re-read segments since stage 1 only mutated
	// LocalPresent; the slice itself is stable, but reusing the snapshot
	// keeps stage 2's view consistent within this Run.
	globalCandidates := segments
	if c.requireUploaded {
		eligible := globalCandidates[:0:0]
		for _, s := range globalCandidates {
			if s.UploadState == storage.UploadStateUploaded {
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

// runLocalEviction evicts local copies according to LocalMaxAge and
// LocalMaxBytes. Selection mirrors the global pass: age first, then oldest-
// first for the byte budget. Errors are logged per segment and don't abort
// the pass.
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

// evictLocal removes one segment's local files (data + sidecars) and marks
// the manifest. The remote copy is untouched; subsequent queries will refetch.
// Order matters: mark meta first so a crash between file delete and meta
// update doesn't leave a phantom-present record pointing at a missing file.
// With mark-first, a crash leaves the file on disk but meta saying absent;
// next-tick scan will see HasLocalCopy()==false and skip it — the file is
// orphaned until manual cleanup, but data is intact.
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

// evictionCandidate carries the segment and the policy that selected it,
// so the metric label is accurate even when multiple policies overlap.
type evictionCandidate struct {
	seg    storage.SegmentMeta
	reason string
}

func (c *Cleaner) selectForDeletion(segments []storage.SegmentMeta) []evictionCandidate {
	var toDelete []evictionCandidate
	now := time.Now().UnixNano()

	if c.policy.MaxAge > 0 {
		cutoff := now - c.policy.MaxAge.Nanoseconds()
		for _, seg := range segments {
			if seg.MaxTS < cutoff {
				toDelete = append(toDelete, evictionCandidate{seg: seg, reason: "max_age"})
			}
		}
	}

	remaining := filterOutCandidates(segments, toDelete)

	// MaxSegments and MaxTotalBytes both evict from the oldest end. Sort by
	// MaxTS ascending so segments[0] is the oldest. Without this we relied on
	// the manager returning segments in insertion order — true today, fragile
	// to assume forever.
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

// filterOutCandidates returns segments not present in exclude. Mirrors
// filterOut but takes the new candidate shape.
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
	if err := c.manager.RemoveSegment(seg.ID); err != nil {
		return err
	}

	if err := c.manager.DeleteSegmentFiles(seg); err != nil {
		return err
	}

	c.sparse.Remove(seg.ID)
	if err := c.sparse.Save(c.dataDir); err != nil {
		c.log.Warn("sparse index save failed", "err", err)
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
