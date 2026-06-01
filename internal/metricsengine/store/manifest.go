package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
)

const manifestFileName = "manifest.json"

type Manifest struct {
	Version int         `json:"version"`
	Blocks  []BlockMeta `json:"blocks"`
}

type BlockMeta struct {
	Path        string              `json:"path"`
	MinTime     int64               `json:"min_time"`
	MaxTime     int64               `json:"max_time"`
	SeriesCount int                 `json:"series_count"`
	LabelValues map[string][]string `json:"label_values,omitempty"`
}

func loadManifest(dir string) (Manifest, error) {
	path := filepath.Join(dir, manifestFileName)
	payload, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return Manifest{Version: 1}, nil
	}
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	if err := json.Unmarshal(payload, &manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.Version == 0 {
		manifest.Version = 1
	}
	sortManifest(manifest.Blocks)
	return manifest, nil
}

func saveManifest(dir string, manifest Manifest) error {
	sortManifest(manifest.Blocks)
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, manifestFileName)
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

func sortManifest(blocks []BlockMeta) {
	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].MinTime == blocks[j].MinTime {
			return blocks[i].Path < blocks[j].Path
		}
		return blocks[i].MinTime < blocks[j].MinTime
	})
}

func rebuildManifest(dir string) (Manifest, error) {
	blockPaths, err := filepath.Glob(filepath.Join(dir, "block-*.meb"))
	if err != nil {
		return Manifest{}, err
	}
	compactPaths, err := filepath.Glob(filepath.Join(dir, "compact-*.meb"))
	if err != nil {
		return Manifest{}, err
	}
	paths := append(blockPaths, compactPaths...)
	sort.Strings(paths)
	manifest := Manifest{Version: 1, Blocks: make([]BlockMeta, 0, len(paths))}
	for _, path := range paths {
		directory, err := block.ReadDirectory(path)
		if err != nil {
			return Manifest{}, err
		}
		minTime, maxTime, _ := directory.TimeRange()
		manifest.Blocks = append(manifest.Blocks, BlockMeta{
			Path:        filepath.Base(path),
			MinTime:     minTime,
			MaxTime:     maxTime,
			SeriesCount: len(directory.Series),
			LabelValues: labelValues(directory),
		})
	}
	sortManifest(manifest.Blocks)
	return manifest, nil
}

func labelValues(directory block.Directory) map[string][]string {
	values := make(map[string]map[string]struct{})
	for _, entry := range directory.Series {
		for _, label := range entry.Labels {
			if values[label.Name] == nil {
				values[label.Name] = make(map[string]struct{})
			}
			values[label.Name][label.Value] = struct{}{}
		}
	}
	out := make(map[string][]string, len(values))
	for name, set := range values {
		for value := range set {
			out[name] = append(out[name], value)
		}
		sort.Strings(out[name])
	}
	return out
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}
