package ingest

import (
	"sync"

	"github.com/yaop-labs/amber/internal/model"
)

// CardinalityGuard rejects entries that would blow up storage cardinality:
// attrs-per-entry, attr-value length, and unique attr keys per service. Zero
// for any limit disables that check. Per-service key sets grow without
// eviction — fine for stable workloads, revisit with a sliding window if
// services churn keys at deploy.
type CardinalityGuard struct {
	maxAttrsPerEntry int
	maxValueBytes    int
	maxKeysPerSvc    int

	mu             sync.Mutex
	keysPerService map[string]map[string]struct{}
}

func NewCardinalityGuard(maxAttrsPerEntry, maxValueBytes, maxKeysPerSvc int) *CardinalityGuard {
	return &CardinalityGuard{
		maxAttrsPerEntry: maxAttrsPerEntry,
		maxValueBytes:    maxValueBytes,
		maxKeysPerSvc:    maxKeysPerSvc,
		keysPerService:   make(map[string]map[string]struct{}),
	}
}

// Check returns "" if the entry passes, or a short reason label suitable for
// the metrics `reason` dimension if it must be dropped.
func (g *CardinalityGuard) Check(service string, attrs []model.Attr) string {
	if g == nil {
		return ""
	}
	if g.maxAttrsPerEntry > 0 && len(attrs) > g.maxAttrsPerEntry {
		return "attrs_per_entry"
	}
	if g.maxValueBytes > 0 {
		for _, a := range attrs {
			if len(a.Value) > g.maxValueBytes {
				return "attr_value_too_long"
			}
		}
	}
	if g.maxKeysPerSvc > 0 && len(attrs) > 0 {
		g.mu.Lock()
		defer g.mu.Unlock()
		known, ok := g.keysPerService[service]
		if !ok {
			known = make(map[string]struct{}, len(attrs))
			g.keysPerService[service] = known
		}
		for _, a := range attrs {
			if _, seen := known[a.Key]; seen {
				continue
			}
			if len(known) >= g.maxKeysPerSvc {
				return "key_cardinality"
			}
			known[a.Key] = struct{}{}
		}
	}
	return ""
}
