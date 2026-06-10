package histogram

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// Compaction merges the per-request hblock files into one block. Without it
// every OTLP POST leaves a file on disk and both query and validation paths
// (which read every block) degrade linearly with request count.
//
// Crash safety uses a marker protocol instead of a manifest (this store has
// none — the directory glob is the source of truth):
//
//  1. write compact-pending.json {dest, sources}, fsync;
//  2. write the merged block atomically (temp -> fsync -> rename);
//  3. delete the source blocks;
//  4. delete the marker.
//
// Recovery at open: a marker whose dest exists (or is empty — everything was
// expired) finishes step 3/4; a marker whose dest is missing is dropped, the
// sources are untouched, and the compaction simply never happened. While
// step 3 runs, blockPaths hides the sources from in-process readers so the
// merged block and its sources are never both visible.

const compactMarkerName = "compact-pending.json"

// defaultCompactMinBlocks triggers auto-compaction in WriteBlock once the
// block-file count reaches this threshold. Options.CompactMinBlocks: 0 uses
// the default, negative disables auto-compaction.
const defaultCompactMinBlocks = 64

type compactMarker struct {
	// Dest is the merged block's base name; empty when every tick was past
	// retention and the sources are simply deleted.
	Dest    string   `json:"dest"`
	Sources []string `json:"sources"`
}

func (s *Store) compactMinBlocks() int {
	switch {
	case s.opts.CompactMinBlocks < 0:
		return 0
	case s.opts.CompactMinBlocks == 0:
		return defaultCompactMinBlocks
	default:
		return s.opts.CompactMinBlocks
	}
}

// Compact merges all block files into one, dropping ticks older than the
// retention cutoff (mirrors the scalar store: whole-file retention alone
// would never fire on a continuously re-compacted block). Returns the merged
// block path, or "" when there was nothing to do or everything had expired.
func (s *Store) Compact() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.compactLocked()
}

func (s *Store) compactLocked() (string, error) {
	paths, err := s.rawBlockPaths()
	if err != nil {
		return "", err
	}
	if len(paths) < 2 {
		return "", nil
	}

	var cutoff int64
	hasCutoff := false
	if s.opts.Retention > 0 {
		cutoff = s.clock().Add(-s.opts.Retention).UnixMilli()
		hasCutoff = true
	}

	type expGroup struct {
		labels     model.LabelSet
		timestamps []int64
		sketches   []*ExponentialHistogram
	}
	type explicitGroup struct {
		labels     model.LabelSet
		timestamps []int64
		buckets    []*ExplicitBucketHistogram
	}
	expGroups := make(map[string]*expGroup)
	explicitGroups := make(map[string]*explicitGroup)
	var expOrder, explicitOrder []string

	for _, path := range paths {
		exps, explicits, err := ReadBlock(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("histogram: compact read %s: %w", filepath.Base(path), err)
		}
		for _, series := range exps {
			labels := series.Entry.Labels.Canonical()
			key := labels.Key()
			g := expGroups[key]
			if g == nil {
				g = &expGroup{labels: labels}
				expGroups[key] = g
				expOrder = append(expOrder, key)
			}
			for i, ts := range series.Timestamps {
				if hasCutoff && ts < cutoff {
					continue
				}
				g.timestamps = append(g.timestamps, ts)
				g.sketches = append(g.sketches, series.Sketches[i])
			}
		}
		for _, series := range explicits {
			labels := series.Entry.Labels.Canonical()
			// Bounds may differ across blocks for the same labels; a series
			// holds one bounds set, so such ticks stay in separate entries.
			key := labels.Key() + "\x00" + boundsKey(series.Bounds)
			g := explicitGroups[key]
			if g == nil {
				g = &explicitGroup{labels: labels}
				explicitGroups[key] = g
				explicitOrder = append(explicitOrder, key)
			}
			for i, ts := range series.Timestamps {
				if hasCutoff && ts < cutoff {
					continue
				}
				g.timestamps = append(g.timestamps, ts)
				g.buckets = append(g.buckets, series.Buckets[i])
			}
		}
	}

	var exp []ExpSeries
	var explicit []ExplicitSeries
	nextID := uint64(1)
	for _, key := range expOrder {
		g := expGroups[key]
		if len(g.timestamps) == 0 {
			continue
		}
		sortExpTicks(g.timestamps, g.sketches)
		exp = append(exp, ExpSeries{ID: nextID, Labels: g.labels, Timestamps: g.timestamps, Sketches: g.sketches})
		nextID++
	}
	for _, key := range explicitOrder {
		g := explicitGroups[key]
		if len(g.timestamps) == 0 {
			continue
		}
		sortExplicitTicks(g.timestamps, g.buckets)
		explicit = append(explicit, ExplicitSeries{ID: nextID, Labels: g.labels, Timestamps: g.timestamps, Buckets: g.buckets})
		nextID++
	}

	sources := make([]string, 0, len(paths))
	for _, p := range paths {
		sources = append(sources, filepath.Base(p))
	}

	destBase := ""
	if len(exp) > 0 || len(explicit) > 0 {
		destBase = fmt.Sprintf("hblock-%06d.mhb", s.seq)
	}

	if err := writeCompactMarker(s.dir, compactMarker{Dest: destBase, Sources: sources}); err != nil {
		return "", err
	}
	destPath := ""
	if destBase != "" {
		destPath = filepath.Join(s.dir, destBase)
		if err := WriteBlock(destPath, exp, explicit); err != nil {
			// Leave the marker: recovery sees a missing dest and aborts.
			_ = os.Remove(filepath.Join(s.dir, compactMarkerName))
			return "", err
		}
		s.seq++
	}

	// Hide the sources from in-process readers before deleting, so a query
	// listing blocks never sees the merged block and its sources together.
	s.pendingMu.Lock()
	s.pendingDrop = make(map[string]struct{}, len(sources))
	for _, src := range sources {
		s.pendingDrop[src] = struct{}{}
	}
	s.pendingMu.Unlock()
	defer func() {
		s.pendingMu.Lock()
		s.pendingDrop = nil
		s.pendingMu.Unlock()
	}()

	for _, src := range sources {
		if src == destBase {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, src)); err != nil && !os.IsNotExist(err) {
			return "", fmt.Errorf("histogram: compact remove %s: %w", src, err)
		}
	}
	if err := os.Remove(filepath.Join(s.dir, compactMarkerName)); err != nil && !os.IsNotExist(err) {
		return "", err
	}
	if err := syncDir(s.dir); err != nil {
		return "", err
	}
	return destPath, nil
}

func writeCompactMarker(dir string, m compactMarker) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, compactMarkerName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(payload); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return syncDir(dir)
}

// recoverCompaction finishes or rolls back a compaction interrupted by a
// crash. Called once at open, before the store is used.
func recoverCompaction(dir string) error {
	path := filepath.Join(dir, compactMarkerName)
	payload, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	var m compactMarker
	if err := json.Unmarshal(payload, &m); err != nil {
		// Torn marker write: the merged block was never started (the marker
		// is written and fsynced before the dest), so dropping the marker
		// rolls the compaction back losslessly.
		return removeAndSync(dir, path)
	}
	if m.Dest != "" {
		if _, err := os.Stat(filepath.Join(dir, m.Dest)); err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			// Dest never landed: sources are intact, roll back.
			return removeAndSync(dir, path)
		}
	}
	// Dest is durable (or everything was expired): finish deleting sources.
	for _, src := range m.Sources {
		if src == m.Dest {
			continue
		}
		if err := os.Remove(filepath.Join(dir, src)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return removeAndSync(dir, path)
}

func removeAndSync(dir, path string) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(dir)
}

func sortExpTicks(timestamps []int64, sketches []*ExponentialHistogram) {
	if sort.SliceIsSorted(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] }) {
		return
	}
	idx := sortedTickIndex(timestamps)
	reorderInt64(timestamps, idx)
	reorderSketches(sketches, idx)
}

func sortExplicitTicks(timestamps []int64, buckets []*ExplicitBucketHistogram) {
	if sort.SliceIsSorted(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] }) {
		return
	}
	idx := sortedTickIndex(timestamps)
	reorderInt64(timestamps, idx)
	reorderBuckets(buckets, idx)
}

func sortedTickIndex(timestamps []int64) []int {
	idx := make([]int, len(timestamps))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return timestamps[idx[a]] < timestamps[idx[b]] })
	return idx
}

func reorderInt64(values []int64, idx []int) {
	tmp := make([]int64, len(values))
	for i, j := range idx {
		tmp[i] = values[j]
	}
	copy(values, tmp)
}

func reorderSketches(values []*ExponentialHistogram, idx []int) {
	tmp := make([]*ExponentialHistogram, len(values))
	for i, j := range idx {
		tmp[i] = values[j]
	}
	copy(values, tmp)
}

func reorderBuckets(values []*ExplicitBucketHistogram, idx []int) {
	tmp := make([]*ExplicitBucketHistogram, len(values))
	for i, j := range idx {
		tmp[i] = values[j]
	}
	copy(values, tmp)
}
