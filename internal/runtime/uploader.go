package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/storage"
)

// uploader drains pending segments to a remote SegmentStore in the background.
//
// Why background, not synchronous-in-rotate(): a synchronous upload would
// stall the seal goroutine (and thus the next rotation) for the duration of
// a network round-trip. On S3 outages that means ingest stops once the
// active segment fills. Background uploads let writes continue against the
// local store; the segment is marked UploadStateLocal until the network
// recovers.
//
// Durability: every retry decision is persisted via RecordUploadFailure /
// MarkUploaded so attempt counters and the final Uploaded transition
// survive process restart. A crash mid-upload simply re-enqueues on the
// next start (we re-read PendingUploads from meta.json).
//
// Concurrency: one goroutine per manager. Multi-upload parallelism would
// reorder retries and complicate backoff; segment uploads are not on the
// hot path for ingest latency, so serial is fine.
type uploader struct {
	manager *storage.SegmentManager
	store   storage.SegmentStore
	dir     string
	log     *slog.Logger

	// notify is buffered with capacity 1 — additional notifications coalesce
	// (the worker re-reads PendingUploads on every wake regardless).
	notify chan struct{}

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// Backoff: 1s → 2s → 4s → … capped at 5min. The cap is high enough that a
// transient S3 outage doesn't drown the log with retries, low enough that
// recovery happens within minutes of S3 coming back. Jitter ±25% to avoid
// thundering herds when a fleet of nodes retries after a shared outage.
const (
	uploadBackoffInitial = 1 * time.Second
	uploadBackoffMax     = 5 * time.Minute
)

func newUploader(manager *storage.SegmentManager, store storage.SegmentStore, dir string, log *slog.Logger) *uploader {
	return &uploader{
		manager: manager,
		store:   store,
		dir:     dir,
		log:     log,
		notify:  make(chan struct{}, 1),
	}
}

// Start launches the worker goroutine. Returns immediately. The worker stops
// when ctx is cancelled or Stop is called.
func (u *uploader) Start(ctx context.Context) {
	ctx, u.cancel = context.WithCancel(ctx)
	u.wg.Add(1)
	go u.run(ctx)
}

// Stop cancels the worker and waits for it to exit.
func (u *uploader) Stop() {
	if u.cancel != nil {
		u.cancel()
	}
	u.wg.Wait()
}

// Enqueue signals the worker to recheck PendingUploads. Non-blocking: a
// pending notification coalesces with new ones, since the worker rescans
// the full list on each wake.
func (u *uploader) Enqueue() {
	select {
	case u.notify <- struct{}{}:
	default:
	}
}

func (u *uploader) run(ctx context.Context) {
	defer u.wg.Done()

	// Start by draining anything left over from previous runs (crash recovery).
	u.Enqueue()

	for {
		pending := u.manager.PendingUploads()
		if len(pending) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-u.notify:
				continue
			}
		}

		// Per-cycle attempt of every pending segment. Failures push them to the
		// next wake; we don't retry tightly here. Worst case: one segment is
		// the bottleneck and others wait — acceptable, ordering is FIFO by ID.
		nextWake := uploadBackoffMax
		for _, seg := range pending {
			if ctx.Err() != nil {
				return
			}
			if err := u.uploadOne(seg); err != nil {
				_ = u.manager.RecordUploadFailure(seg.ID, err.Error())
				delay := backoffDelay(seg.UploadAttempts + 1)
				if delay < nextWake {
					nextWake = delay
				}
				u.log.Warn("s3 upload failed", "segment", seg.FileName, "attempt", seg.UploadAttempts+1, "err", err)
				continue
			}
			if err := u.manager.MarkUploaded(seg.ID); err != nil {
				u.log.Warn("mark uploaded", "segment", seg.FileName, "err", err)
			}
		}

		// If everything succeeded, nextWake stays at max; we'll block on notify
		// instead of waking up for nothing.
		if u.manager.PendingUploads() == nil {
			select {
			case <-ctx.Done():
				return
			case <-u.notify:
			}
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-u.notify:
		case <-time.After(nextWake):
		}
	}
}

// uploadOne uploads all sidecars for a single segment. Returns the first
// error encountered. Missing local files are not errors — they were never
// built (e.g. a segment with no spans has no posting list); the remote
// store simply has fewer keys.
func (u *uploader) uploadOne(seg storage.SegmentMeta) error {
	for _, ext := range storage.SegmentSidecarExts {
		name := seg.FileName + ext
		path := filepath.Join(u.dir, name)
		f, err := os.Open(path) //nolint:gosec
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("open %s: %w", name, err)
		}
		err = u.store.Put(name, f)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("put %s: %w", name, err)
		}
	}
	return nil
}

// backoffDelay returns the delay before the next retry given the current
// attempt count (1-based). Exponential with ±25% jitter, capped at
// uploadBackoffMax.
func backoffDelay(attempts uint32) time.Duration {
	// Cap the exponent so the shift can't overflow: at attempts=30 the raw
	// value already exceeds uploadBackoffMax by orders of magnitude.
	exp := attempts - 1
	if exp > 20 {
		exp = 20
	}
	d := uploadBackoffInitial << exp
	if d <= 0 || d > uploadBackoffMax {
		d = uploadBackoffMax
	}
	// Jitter ±25%.
	jitter := time.Duration(rand.Int64N(int64(d) / 2))
	return d - d/4 + jitter
}
