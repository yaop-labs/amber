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
