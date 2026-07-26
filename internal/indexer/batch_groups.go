package indexer

import "github.com/yaop-labs/amber/internal/index"

const (
	// Normal batches have a handful of values per indexed field. Do not let
	// one high-cardinality batch permanently pin a large map and thousands of
	// tiny ID buffers in the active indexer.
	maxRetainedIndexGroups = 256
	maxRetainedGroupIDs    = 16 << 10
)

// idGroupScratch groups IDs by one field value. The map is cleared after each
// batch so it never retains request-owned strings; backing ID slices are
// reused independently of their previous keys.
type idGroupScratch struct {
	slots map[string]int
	ids   [][]uint64
	used  int
}

func (g *idGroupScratch) add(value string, id uint64) {
	if g.slots == nil {
		g.slots = make(map[string]int, 8)
	}
	slot, ok := g.slots[value]
	if !ok {
		slot = g.used
		g.used++
		g.slots[value] = slot
		if slot == len(g.ids) {
			g.ids = append(g.ids, nil)
		}
		g.ids[slot] = g.ids[slot][:0]
	}
	g.ids[slot] = append(g.ids[slot], id)
}

func (g *idGroupScratch) flush(idx *index.MultiFieldIndex, field string) {
	if len(g.slots) == 0 {
		return
	}
	bitmap := idx.GetOrCreate(field)
	for value, slot := range g.slots {
		bitmap.AddMany(value, g.ids[slot])
	}
	g.reset()
}

func (g *idGroupScratch) reset() {
	if g.used > maxRetainedIndexGroups {
		g.slots = nil
		g.ids = nil
		g.used = 0
		return
	}

	clear(g.slots)
	for i := 0; i < g.used; i++ {
		if cap(g.ids[i]) > maxRetainedGroupIDs {
			g.ids[i] = nil
		} else {
			g.ids[i] = g.ids[i][:0]
		}
	}
	g.used = 0
}
