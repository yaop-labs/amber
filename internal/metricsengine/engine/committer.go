package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

// committer batches WAL fsync calls across concurrent writers.
// Append returns after the caller's sequence is covered by a completed Sync.
type committer struct {
	wal           *wal.WAL
	flushInterval time.Duration

	mu sync.Mutex
	// nextSeq is assigned to the next append. tick advances syncedSeq to
	// lastWrittenSeq after a successful fsync.
	nextSeq        uint64
	lastWrittenSeq uint64
	syncedSeq      atomic.Uint64
	pending        *sync.Cond // signalled on every Sync completion
	closed         bool
	closeOnce      sync.Once
	stop           chan struct{} // closed by Close to stop the goroutine
	done           chan struct{} // closed by the goroutine on exit
}

func newCommitter(w *wal.WAL, flushInterval time.Duration) *committer {
	if flushInterval <= 0 {
		flushInterval = 5 * time.Millisecond
	}
	c := &committer{
		wal:           w,
		flushInterval: flushInterval,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	c.pending = sync.NewCond(&c.mu)
	go c.run()
	return c
}

// Append writes records to the WAL and waits until they are fsynced.
// On error, the caller should treat ingest as failed and leave in-memory state
// unchanged.
func (c *committer) Append(records []wal.Record) error {
	if len(records) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCommitterClosed
	}
	c.nextSeq++
	mySeq := c.nextSeq
	c.mu.Unlock()

	if err := c.wal.AppendBatchUnsynced(records); err != nil {
		// Publish the sequence so later syncs can advance waiters past a
		// failed append.
		c.mu.Lock()
		if mySeq > c.lastWrittenSeq {
			c.lastWrittenSeq = mySeq
		}
		c.mu.Unlock()
		return err
	}

	c.mu.Lock()
	if mySeq > c.lastWrittenSeq {
		c.lastWrittenSeq = mySeq
	}
	for c.syncedSeq.Load() < mySeq {
		if c.closed {
			c.mu.Unlock()
			return errCommitterClosed
		}
		c.pending.Wait()
	}
	c.mu.Unlock()
	return nil
}

// run fsyncs pending WAL records on the configured interval.
func (c *committer) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = c.tick()
		case <-c.stop:
			// Drain once before exit.
			_ = c.tick()
			return
		}
	}
}

// flushAndStop drains pending writes and stops the goroutine.
func (c *committer) flushAndStop() error {
	var lastErr error
	c.closeOnce.Do(func() {
		close(c.stop)
		<-c.done
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.pending.Broadcast()
	})
	return lastErr
}

// tick fsyncs pending records once and wakes waiters.
func (c *committer) tick() error {
	c.mu.Lock()
	target := c.lastWrittenSeq
	already := c.syncedSeq.Load()
	c.mu.Unlock()
	if target <= already {
		return nil
	}
	err := c.wal.Sync()
	if err != nil {
		return err
	}
	c.syncedSeq.Store(target)
	c.mu.Lock()
	c.pending.Broadcast()
	c.mu.Unlock()
	return nil
}

var errCommitterClosed = walClosedError{}

type walClosedError struct{}

func (walClosedError) Error() string { return "engine: WAL committer is closed" }
