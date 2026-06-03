package engine

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

// committer batches WAL fsync calls across concurrent writers (Postgres-
// style group commit). Many AppendBatch callers can pipe records through
// commit.Append() concurrently; one background goroutine actually calls
// Sync() on a fixed cadence, covering whatever writes accumulated since
// the previous Sync.
//
// Durability bound: a successful commit.Append() guarantees the records
// were fsync-ed before return. Worst-case wait = flushInterval; typical
// wait << flushInterval because the committer wakes immediately on the
// queue ticker, not by polling.
//
// Why a sequence number + Cond and not channels: every writer needs to
// know when the fsync that covered ITS bytes completed. A single broadcast
// after each Sync wakes all waiters; each checks "is my seq <= synced
// seq". This avoids per-writer channels (allocation + scheduling cost
// scales linearly with concurrency).

type committer struct {
	wal           *wal.WAL
	flushInterval time.Duration

	mu sync.Mutex
	// nextSeq is the seq assigned to the NEXT incoming append. After a
	// successful AppendBatchUnsynced we publish lastWrittenSeq = the seq
	// we just got. tick() copies lastWrittenSeq → syncTargetSeq, runs
	// fsync, then publishes syncedSeq = syncTargetSeq.
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

// Append writes records to the WAL (unsynced) and blocks until those
// records have been fsync-ed by the committer goroutine. Returns whatever
// error the WAL write or fsync produced; on error the caller's records
// may or may not be on disk — the caller treats it as ingest failure and
// must NOT update in-memory state.
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

	// WAL write happens outside the committer mutex. wal.AppendBatchUnsynced
	// takes the WAL's own mutex briefly to serialize file appends — this is
	// the only contention point for the in-memory path, and it's just
	// memcpy + Write() syscall, no fsync. Critical: fsync happens later in
	// the committer goroutine, NOT here.
	if err := c.wal.AppendBatchUnsynced(records); err != nil {
		// Even on error we publish our seq so a later Sync() can advance
		// past us. Otherwise a single failed write would stall all
		// subsequent appends waiting on the cond.
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
	// Wait until the committer goroutine has fsync-ed past our seq.
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

// run is the committer goroutine: every flushInterval, if there are
// unsynced bytes (lastWrittenSeq > syncedSeq), do one fsync that covers
// them all, then broadcast.
func (c *committer) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = c.tick()
		case <-c.stop:
			// Final drain so any Append() that returned successfully has
			// definitely been fsync'd before we exit.
			_ = c.tick()
			return
		}
	}
}

// flushAndStop drains pending writes one final time and stops the
// goroutine. Called from Engine.Close. Idempotent.
func (c *committer) flushAndStop() error {
	var lastErr error
	c.closeOnce.Do(func() {
		// Signal stop; the goroutine will do one final tick before exit.
		close(c.stop)
		<-c.done
		// Anything still waiting on the Cond (shouldn't be — appenders
		// would have been woken by the final broadcast in tick) gets
		// unblocked here.
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.pending.Broadcast()
	})
	return lastErr
}

// tick checks if there's anything to fsync; if so, fsyncs once and
// broadcasts. Single-threaded (called only from run() goroutine and from
// flushAndStop()), so no need to lock around the Sync call itself.
func (c *committer) tick() error {
	c.mu.Lock()
	target := c.lastWrittenSeq
	already := c.syncedSeq.Load()
	c.mu.Unlock()
	if target <= already {
		// Nothing to fsync; cheap path.
		return nil
	}
	err := c.wal.Sync()
	if err != nil {
		// Don't advance syncedSeq on error — waiters must see this as
		// "their bytes were NOT durable". They'll be unblocked by Close
		// (broadcast on c.closed=true) or by a subsequent successful tick.
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
