package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/storage"
)

// fakeStore implements storage.SegmentStore with a knob to fail the first N
// Put calls, then succeed. List/Get/Delete are unused by the uploader path.
type fakeStore struct {
	mu       sync.Mutex
	failures int32 // remaining Puts that should fail
	puts     []string
	getCalls int
	failErr  error
}

func (f *fakeStore) Put(name string, _ io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return f.failErr
	}
	f.puts = append(f.puts, name)
	return nil
}
func (f *fakeStore) Get(name string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	return nil, fmt.Errorf("not implemented")
}
func (f *fakeStore) Delete(_ string) error   { return nil }
func (f *fakeStore) List() ([]string, error) { return nil, nil }

func (f *fakeStore) putsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.puts)
}

func newQuietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newSegmentWithFile(t *testing.T) (*storage.SegmentManager, string, uint32) {
	t.Helper()
	dir := t.TempDir()
	sm, err := storage.OpenSegmentManager(dir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}
	t.Cleanup(func() { _ = sm.Close() })

	if err := sm.Write([]byte("payload"), time.Now().UnixNano()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	pending := sm.PendingUploads()
	if len(pending) != 1 {
		t.Fatalf("pending: got %d, want 1", len(pending))
	}
	return sm, dir, pending[0].ID
}

func TestUploader_SuccessMarksUploaded(t *testing.T) {
	sm, dir, id := newSegmentWithFile(t)
	store := &fakeStore{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	u := newUploader(sm, store, dir, newQuietLogger())
	u.Start(ctx)
	defer u.Stop()

	u.Enqueue()

	if !eventually(t, 2*time.Second, func() bool {
		for _, seg := range sm.Segments() {
			if seg.ID == id && seg.UploadState == storage.UploadStateUploaded {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("segment %d not marked Uploaded", id)
	}

	// At least the .alog data file should have been Put. Some sidecars do not
	// exist on disk for a minimal segment, which is fine.
	if store.putsCount() == 0 {
		t.Errorf("no Put calls observed")
	}
}

func TestUploader_RetriesAfterFailure(t *testing.T) {
	sm, dir, id := newSegmentWithFile(t)
	store := &fakeStore{failures: 2, failErr: errors.New("transient")}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Shrink backoff for the test so we don't wait ~3s for the second retry.
	u := newUploader(sm, store, dir, newQuietLogger())
	u.Start(ctx)
	defer u.Stop()

	u.Enqueue()

	if !eventually(t, 10*time.Second, func() bool {
		for _, seg := range sm.Segments() {
			if seg.ID == id && seg.UploadState == storage.UploadStateUploaded {
				return true
			}
		}
		return false
	}) {
		// Surface attempt counter for diagnosis.
		for _, seg := range sm.Segments() {
			if seg.ID == id {
				t.Logf("segment %d attempts=%d err=%q", id, seg.UploadAttempts, seg.LastUploadErr)
			}
		}
		t.Fatalf("upload never succeeded after transient failures")
	}
}

func TestBackoffDelay_BoundsAndJitter(t *testing.T) {
	// Attempt 1: roughly uploadBackoffInitial ± 25%.
	for i := 0; i < 100; i++ {
		d := backoffDelay(1)
		if d < uploadBackoffInitial*3/4 || d > uploadBackoffInitial*5/4 {
			t.Fatalf("attempt 1: got %v, out of [%v, %v]", d, uploadBackoffInitial*3/4, uploadBackoffInitial*5/4)
		}
	}

	// Attempt 100: shift would overflow; must clamp at max.
	d := backoffDelay(100)
	if d > uploadBackoffMax {
		t.Fatalf("attempt 100: got %v, want <= %v", d, uploadBackoffMax)
	}
	if d < uploadBackoffMax*3/4 {
		t.Fatalf("attempt 100: got %v, want >= %v", d, uploadBackoffMax*3/4)
	}
}

func TestUploader_CrashRecoveryDrainsPending(t *testing.T) {
	dir := t.TempDir()
	sm, err := storage.OpenSegmentManager(dir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("OpenSegmentManager: %v", err)
	}

	if err := sm.Write([]byte("a"), time.Now().UnixNano()); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := sm.Rotate(); err != nil {
		t.Fatalf("Rotate: %v", err)
	}
	// Simulate a crash before the uploader had a chance to run by closing
	// without ever starting one. Pending state is now durable in meta.json.
	if err := sm.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	sm2, err := storage.OpenSegmentManager(dir, storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer sm2.Close()

	pending := sm2.PendingUploads()
	if len(pending) == 0 {
		t.Fatalf("expected pending uploads after reopen")
	}

	store := &fakeStore{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u := newUploader(sm2, store, dir, newQuietLogger())
	u.Start(ctx)
	defer u.Stop()
	// No Enqueue call: the worker primes itself on Start by calling Enqueue
	// internally, which is the crash-recovery path under test.

	if !eventually(t, 2*time.Second, func() bool {
		return len(sm2.PendingUploads()) == 0
	}) {
		t.Fatalf("crash-recovery did not drain pending uploads")
	}
}

// eventually polls cond every 25ms until it returns true or the deadline is
// reached. Cheaper than time.Sleep and avoids flakes when CI is loaded.
func eventually(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return cond()
}

// Compile-time check that fakeStore satisfies the interface.
var _ storage.SegmentStore = (*fakeStore)(nil)
