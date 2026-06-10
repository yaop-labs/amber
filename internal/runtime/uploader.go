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

// uploader uploads sealed segments in the background.
// Upload state is stored in segment metadata and retried after restart.
type uploader struct {
	manager *storage.SegmentManager
	store   storage.SegmentStore
	dir     string
	log     *slog.Logger

	// notify coalesces upload wakeups.
	notify chan struct{}

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

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

// Start starts the upload worker.
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

// Enqueue wakes the upload worker.
func (u *uploader) Enqueue() {
	select {
	case u.notify <- struct{}{}:
	default:
	}
}

func (u *uploader) run(ctx context.Context) {
	defer u.wg.Done()

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

// uploadOne uploads one segment and its sidecars.
// Missing sidecars are ignored.
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

// backoffDelay returns the retry delay for a 1-based attempt count.
func backoffDelay(attempts uint32) time.Duration {
	exp := min(attempts - 1, 20)
	d := uploadBackoffInitial << exp
	if d <= 0 || d > uploadBackoffMax {
		d = uploadBackoffMax
	}
	jitter := time.Duration(rand.Int64N(int64(d) / 2)) //nolint:gosec
	out := min(d - d/4 + jitter, uploadBackoffMax)
	return out
}
