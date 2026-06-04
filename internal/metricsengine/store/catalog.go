package store

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

const catalogFileName = "catalog.json"

type Catalog struct {
	NextID uint64         `json:"next_id"`
	Series []CatalogEntry `json:"series"`
}

type CatalogEntry struct {
	ID     uint64         `json:"id"`
	Labels model.LabelSet `json:"labels"`
}

func loadCatalog(dir string) (Catalog, error) {
	path := filepath.Join(dir, catalogFileName)
	payload, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Catalog{NextID: 1}, nil
	}
	if err != nil {
		return Catalog{}, err
	}
	var catalog Catalog
	if err := json.Unmarshal(payload, &catalog); err != nil {
		return Catalog{}, err
	}
	if catalog.NextID == 0 {
		catalog.NextID = 1
	}
	return catalog, nil
}

func saveCatalog(dir string, catalog Catalog) error {
	payload, err := json.MarshalIndent(catalog, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, catalogFileName)
	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(dir)
}

func (c Catalog) Registry() *index.Registry {
	registry := index.NewRegistry()
	for _, entry := range c.Series {
		registry.Import(index.SeriesID(entry.ID), entry.Labels)
	}
	return registry
}

func (c *Catalog) Ensure(labels model.LabelSet) uint64 {
	canonical := labels.Canonical()
	for _, entry := range c.Series {
		if entry.Labels.Equal(canonical) {
			return entry.ID
		}
	}
	id := c.NextID
	if id == 0 {
		id = 1
	}
	c.NextID = id + 1
	c.Series = append(c.Series, CatalogEntry{ID: id, Labels: canonical})
	return id
}

func rebuildCatalogFromManifest(dir string, manifest Manifest) (Catalog, error) {
	catalog := Catalog{NextID: 1}
	for _, meta := range manifest.Blocks {
		directory, err := block.ReadDirectory(filepath.Join(dir, meta.Path))
		if err != nil {
			return Catalog{}, err
		}
		for _, entry := range directory.Series {
			catalog.Ensure(entry.Labels)
		}
	}
	return catalog, nil
}

// reconcileLastTouchFromBlocks walks every sealed block in the manifest
// and updates each series' last-touch in the registry to (at minimum)
// the block's TimeMax for that series. Closes two distinct boot-time
// correctness gaps; both are pinned by tests (see doc.go invariants 3
// and 5).
//
// Gap (a) — sentinel-zero leak. Without this pass, a series whose
// only evidence-of-life is on-disk blocks would land at lastTouch=0
// after Import, the sweep would never evict it, and a crashed
// ephemeral series would leak forever. (INDEX_EVICTION_SPEC_v0 §1.)
// Pinned by TestReconcileLastTouchFromBlocks.
//
// Gap (b) — group-commit safety net for a lost REGISTER. The catalog
// log uses group commit; a crash in the last <= flushInterval window
// may drop an in-flight REGISTER. If the dropped record is for a
// series whose samples already reached a block (independent WAL+block
// path), the block carries the labels but the catalog log does not.
// The UpdateLastTouch->GetOrCreateAt fallback below resurrects the
// series by labels — Import-by-id would fail (id collision with
// another series the registry allocated post-crash), but
// GetOrCreateAt(labels) uses labels as the durable identity, which is
// what matters. Pinned by TestCatalogLogLostRegisterByBlockReconcileAlone.
//
// Cost: O(blocks × series_per_block) at boot, one-time. At our cadence
// (a few blocks per retention window) this is trivial. Tolerates
// missing/corrupt blocks: a block that can't be read returns an error
// to the caller — the boot path treats this as fatal because a block
// listed in the manifest but unreadable is a data-integrity signal
// worth refusing-to-start over.
func reconcileLastTouchFromBlocks(dir string, manifest Manifest, registry *index.Registry) error {
	for _, meta := range manifest.Blocks {
		directory, err := block.ReadDirectory(filepath.Join(dir, meta.Path))
		if err != nil {
			// Skip unreadable block — same posture as the catalog
			// rebuild path, which also tolerates per-block read
			// failures by surfacing the error. Reconcile is best-
			// effort: any series this block would have touched will
			// stay at lastTouch=0, the sweep-safe sentinel, until a
			// real ingest re-touches it.
			return err
		}
		for _, entry := range directory.Series {
			if !registry.UpdateLastTouch(index.SeriesID(entry.SeriesID), entry.TimeMax) {
				// The series id from this block is not known to the
				// registry — typically because its REGISTER was lost
				// in the last fsync window before a crash (catalog
				// group-commit safety, see catalog_log_committer.go
				// header). Resurrect by labels: GetOrCreateAt either
				// finds an existing entry with these labels (allocated
				// from a different on-disk source, e.g. WAL replay)
				// and advances its lastTouch, or creates a fresh
				// entry. Either way the labels are now known to the
				// registry; subsequent Match calls find them.
				registry.GetOrCreateAt(entry.Labels, entry.TimeMax)
			}
		}
	}
	return nil
}
