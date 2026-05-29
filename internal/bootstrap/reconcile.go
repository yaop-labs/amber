package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/yaop-labs/amber/internal/storage"
)

// ReconcileFromRemote scans the configured SegmentStore for sealed segments
// not yet known to manager and adopts them into local meta. Used when a
// node starts against a remote store that already contains data (e.g.
// after migration to a new node, or recovery after losing local disk).
//
// Operation is best-effort and idempotent: failures on individual segments
// log and continue; segments already present in local meta are skipped.
// The data file and every sidecar are downloaded via store.Get(), which
// already handles atomic temp+rename, so a crash mid-reconcile leaves
// either the full set or none.
//
// Returns the number of segments newly adopted.
func ReconcileFromRemote(
	ctx context.Context,
	manager *storage.SegmentManager,
	store storage.SegmentStore,
	dir string,
	log *slog.Logger,
) (int, error) {
	if store == nil {
		return 0, errors.New("bootstrap: reconcile: nil store")
	}

	remote, err := store.List()
	if err != nil {
		return 0, fmt.Errorf("bootstrap: reconcile: list remote: %w", err)
	}

	known := make(map[string]struct{}, len(manager.Segments()))
	for _, s := range manager.Segments() {
		known[s.FileName] = struct{}{}
	}

	adopted := 0
	for _, fileName := range remote {
		if ctx.Err() != nil {
			return adopted, ctx.Err()
		}
		if _, ok := known[fileName]; ok {
			continue
		}

		id, ok := storage.ParseSegmentID(fileName)
		if !ok {
			log.Warn("reconcile: skipping unrecognized remote name", "name", fileName)
			continue
		}

		if err := fetchAllSidecars(store, fileName, log); err != nil {
			log.Warn("reconcile: fetch failed; skipping", "segment", fileName, "err", err)
			continue
		}

		meta, err := readSegmentFooter(filepath.Join(dir, fileName), id, fileName)
		if err != nil {
			log.Warn("reconcile: read footer; skipping", "segment", fileName, "err", err)
			continue
		}

		if err := manager.AdoptUploadedSegment(meta); err != nil {
			log.Warn("reconcile: adopt failed", "segment", fileName, "err", err)
			continue
		}
		adopted++
		log.Info("reconcile: adopted remote segment", "segment", fileName, "min_ts", meta.MinTS, "max_ts", meta.MaxTS, "records", meta.RecordCount)
	}
	return adopted, nil
}

// fetchAllSidecars pulls the data file and every sidecar extension from the
// remote store into the local cache. Missing remote sidecars are tolerated:
// the index-rebuild path in LoadSealedIndexes will reconstruct them from the
// data file if needed.
func fetchAllSidecars(store storage.SegmentStore, fileName string, log *slog.Logger) error {
	for _, ext := range storage.SegmentSidecarExts {
		name := fileName + ext
		rc, err := store.Get(name)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				if ext != "" {
					// Sidecar absence is okay; bootstrap will rebuild it.
					continue
				}
				return fmt.Errorf("data file missing: %w", err)
			}
			if ext == "" {
				return fmt.Errorf("get %s: %w", name, err)
			}
			log.Warn("reconcile: sidecar fetch error", "name", name, "err", err)
			continue
		}
		// store.Get already wrote to the local cache atomically; we just need
		// to close the returned handle.
		_ = rc.Close()
	}
	return nil
}

// readSegmentFooter opens the downloaded segment file and reads the footer
// to populate min/max timestamps, record count, and size. These are needed
// by retention and query planning.
func readSegmentFooter(path string, id uint32, fileName string) (storage.SegmentMeta, error) {
	sr, err := storage.OpenSegmentReader(path, nil)
	if err != nil {
		return storage.SegmentMeta{}, fmt.Errorf("open reader: %w", err)
	}
	defer sr.Close()

	footer := sr.Footer()

	info, err := os.Stat(path) //nolint:gosec
	if err != nil {
		return storage.SegmentMeta{}, fmt.Errorf("stat: %w", err)
	}

	return storage.SegmentMeta{
		ID:          id,
		FileName:    fileName,
		MinTS:       footer.MinTS,
		MaxTS:       footer.MaxTS,
		RecordCount: footer.RecordCount,
		SizeBytes:   info.Size(),
	}, nil
}
