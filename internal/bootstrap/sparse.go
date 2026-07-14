package bootstrap

import (
	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/storage"
)

// BuildSparseIndex derives query-planning ranges from authoritative segment
// metadata plus the live active writer state. sparse.idx is only a persisted
// acceleration artifact and is never consulted for membership.
func BuildSparseIndex(manager *storage.SegmentManager) *index.SparseIndex {
	sparse := index.NewSparseIndex()
	add := func(seg storage.SegmentMeta) {
		if seg.RecordCount == 0 {
			return
		}
		sparse.Add(index.SegmentTimeRange{
			SegmentID: seg.ID,
			FileName:  seg.FileName,
			MinTS:     seg.MinTS,
			MaxTS:     seg.MaxTS,
		})
	}
	for _, seg := range manager.Segments() {
		add(seg)
	}
	if active, ok := manager.ActiveSegmentMeta(); ok {
		add(active)
	}
	return sparse
}
