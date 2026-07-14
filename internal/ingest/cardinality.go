package ingest

import (
	"sync"

	"github.com/yaop-labs/amber/internal/model"
)

// CardinalityGuard rejects entries that would blow up storage cardinality:
// attrs-per-entry, attr-value length, and unique attr keys per service. Zero
// for any limit disables that check. Per-service key sets grow without
// eviction - fine for stable workloads, revisit with a sliding window if
// services churn keys at deploy.
type CardinalityGuard struct {
	maxAttrsPerEntry int
	maxValueBytes    int
	maxKeysPerSvc    int
	maxServices      int

	mu             sync.Mutex
	keysPerService map[string]map[string]struct{}
}

// NewCardinalityGuard returns a guard with the given limits; a zero limit
// disables that check.
func NewCardinalityGuard(maxAttrsPerEntry, maxValueBytes, maxKeysPerSvc int, maxServices ...int) *CardinalityGuard {
	g := &CardinalityGuard{
		maxAttrsPerEntry: maxAttrsPerEntry,
		maxValueBytes:    maxValueBytes,
		maxKeysPerSvc:    maxKeysPerSvc,
		keysPerService:   make(map[string]map[string]struct{}),
	}
	if len(maxServices) > 0 {
		g.maxServices = maxServices[0]
	}
	return g
}

// Check returns "" if the entry passes, or a short reason label suitable for
// the metrics `reason` dimension if it must be dropped.
func (g *CardinalityGuard) Check(service string, attrs []model.Attr) string {
	reason, _ := g.Admit(service, attrs, func() bool { return true })
	return reason
}

// Admit atomically validates cardinality, attempts queue admission, and only
// commits newly observed keys if enqueue succeeds. The callback must be
// non-blocking and must return true exactly when it accepted the record.
func (g *CardinalityGuard) Admit(service string, attrs []model.Attr, enqueue func() bool) (string, bool) {
	if g == nil {
		return "", enqueue()
	}
	if g.maxAttrsPerEntry > 0 && len(attrs) > g.maxAttrsPerEntry {
		return "attrs_per_entry", false
	}
	if g.maxValueBytes > 0 {
		for _, a := range attrs {
			if len(a.Value) > g.maxValueBytes {
				return "attr_value_too_long", false
			}
		}
	}
	if (g.maxKeysPerSvc > 0 || g.maxServices > 0) && len(attrs) > 0 {
		g.mu.Lock()
		defer g.mu.Unlock()
		known, ok := g.keysPerService[service]
		if !ok {
			if g.maxServices > 0 && len(g.keysPerService) >= g.maxServices {
				return "service_cardinality", false
			}
			known = make(map[string]struct{}, min(len(attrs), g.maxKeysPerSvc))
		}
		newKeys := make([]string, 0, len(attrs))
		seenNew := make(map[string]struct{}, len(attrs))
		for _, a := range attrs {
			if _, seen := known[a.Key]; seen {
				continue
			}
			if _, seen := seenNew[a.Key]; seen {
				continue
			}
			seenNew[a.Key] = struct{}{}
			newKeys = append(newKeys, a.Key)
		}
		if g.maxKeysPerSvc > 0 && len(known)+len(newKeys) > g.maxKeysPerSvc {
			return "key_cardinality", false
		}
		if !enqueue() {
			return "", false
		}
		if !ok {
			g.keysPerService[service] = known
		}
		for _, key := range newKeys {
			known[key] = struct{}{}
		}
		return "", true
	}
	return "", enqueue()
}
