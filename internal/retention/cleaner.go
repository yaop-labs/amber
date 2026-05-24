// Package retention enforces age, byte-size, and segment-count limits on
// sealed segments by deleting oldest first.
package retention

import (
	"log/slog"
	"sort"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/storage"
)

type Policy struct {
	MaxAge        time.Duration
	MaxTotalBytes int64
	MaxSegments   int
}

type Cleaner struct {
	manager  *storage.SegmentManager
	sparse   *index.SparseIndex
	policy   Policy
	dataDir  string
	log      *slog.Logger
	onDelete func(storage.SegmentMeta)
}

func NewCleaner(
	manager *storage.SegmentManager,
	sparse *index.SparseIndex,
	policy Policy,
	dataDir string,
	log *slog.Logger,
) *Cleaner {
	return &Cleaner{
		manager: manager,
		sparse:  sparse,
		policy:  policy,
		dataDir: dataDir,
		log:     log,
	}
}

func (c *Cleaner) SetOnDelete(fn func(storage.SegmentMeta)) {
	c.onDelete = fn
}

func (c *Cleaner) Run() (int, error) {
	segments := c.manager.Segments()
	if len(segments) == 0 {
		return 0, nil
	}

	toDelete := c.selectForDeletion(segments)
	if len(toDelete) == 0 {
		return 0, nil
	}

	deleted := 0
	for _, seg := range toDelete {
		if err := c.deleteSegment(seg); err != nil {
			c.log.Error("failed to delete segment",
				"file", seg.FileName,
				"err", err,
			)
			continue
		}
		deleted++
	}

	return deleted, nil
}

func (c *Cleaner) selectForDeletion(segments []storage.SegmentMeta) []storage.SegmentMeta {
	var toDelete []storage.SegmentMeta
	now := time.Now().UnixNano()

	if c.policy.MaxAge > 0 {
		cutoff := now - c.policy.MaxAge.Nanoseconds()
		for _, seg := range segments {
			if seg.MaxTS < cutoff {
				toDelete = append(toDelete, seg)
			}
		}
	}

	remaining := filterOut(segments, toDelete)

	// MaxSegments and MaxTotalBytes both evict from the oldest end. Sort by
	// MaxTS ascending so segments[0] is the oldest. Without this we relied on
	// the manager returning segments in insertion order — true today, fragile
	// to assume forever.
	sort.SliceStable(remaining, func(i, j int) bool {
		return remaining[i].MaxTS < remaining[j].MaxTS
	})

	if c.policy.MaxSegments > 0 && len(remaining) > c.policy.MaxSegments {
		excess := len(remaining) - c.policy.MaxSegments
		toDelete = append(toDelete, remaining[:excess]...)
		remaining = remaining[excess:]
	}

	if c.policy.MaxTotalBytes > 0 {
		var totalBytes int64
		for _, seg := range remaining {
			totalBytes += seg.SizeBytes
		}
		for i := 0; i < len(remaining) && totalBytes > c.policy.MaxTotalBytes; i++ {
			toDelete = append(toDelete, remaining[i])
			totalBytes -= remaining[i].SizeBytes
		}
	}

	return toDelete
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
