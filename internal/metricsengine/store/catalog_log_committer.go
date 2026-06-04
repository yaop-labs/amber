package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// catalogCommitter batches catalog log fsyncs across concurrent
// AppendRegister/AppendEvict callers (Postgres-style group commit).
// Direct transplant of internal/metricsengine/engine/committer.go —
// same shape, same invariants, retargeted at *catalogLog instead of
// *wal.WAL.
//
// Why we need it: the cutover (3a.4) replaced the JSON saveCatalog
// path with append-only log writes. The JSON path batched a whole
// ensureCatalog's worth of new series into one fsync (one rewrite-
// and-fsync of the catalog file); the new log writes one fsync per
// REGISTER. At warmup (~1300 series in ~60s) that's ~22 fsyncs/sec
// extra, which knocked control_3a throughput from 96 to 75 samples/sec
// — the same bug class as the original engine.mu fsync-under-lock
// pre-PR1. This committer fixes the same way PR1 fixed the engine.
//
// Durability bound: a successful Append() guarantees the record was
// fsync'd before return. Worst-case wait = flushInterval; typical wait
// << flushInterval because the committer wakes immediately on the
// ticker, not by polling.
//
// Why a sequence number + Cond and not channels: every writer needs to
// know when the fsync that covered ITS bytes completed. A single
// broadcast after each Sync wakes all waiters; each checks "is my seq
// <= synced seq". Avoids per-writer channels (allocation + scheduling
// cost would scale linearly with concurrency at exactly the moment we
// most want to avoid scaling-with-concurrency).
//
// Crash safety w.r.t. group commit: a lost REGISTER from the batched
// window is safe IF AND ONLY IF the series' samples reached the
// WAL/blocks. That coupling is the load-bearing invariant — the
// engine's WAL group-commit fsync'd the samples, and on next boot
// reconcileLastTouchFromBlocks (catalog.go) re-registers any series it
// finds in block directories. So a lost REGISTER becomes a recovered
// REGISTER via the block path. See TestCatalogLogLostRegisterRecovered
// FromBlocks (catalog_log_committer_test.go).

type catalogCommitter struct {
	log           *catalogLog
	flushInterval time.Duration

	mu             sync.Mutex
	nextSeq        uint64
	lastWrittenSeq uint64
	syncedSeq      atomic.Uint64
	pending        *sync.Cond
	closed         bool
	closeOnce      sync.Once
	stop           chan struct{}
	done           chan struct{}
}

var errCatalogCommitterClosed = errors.New("catalog log: committer is closed")

func newCatalogCommitter(log *catalogLog, flushInterval time.Duration) *catalogCommitter {
	if flushInterval <= 0 {
		flushInterval = 5 * time.Millisecond
	}
	c := &catalogCommitter{
		log:           log,
		flushInterval: flushInterval,
		stop:          make(chan struct{}),
		done:          make(chan struct{}),
	}
	c.pending = sync.NewCond(&c.mu)
	go c.run()
	return c
}

// Append writes a record to the log (unsynced) and blocks until that
// record has been fsync'd by the committer goroutine. Returns whatever
// error the write or fsync produced; on error the caller's record may
// or may not be on disk — caller treats it as ingest failure and must
// NOT update in-memory state.
func (c *catalogCommitter) Append(rec []byte) error {
	if len(rec) == 0 {
		return nil
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCatalogCommitterClosed
	}
	c.nextSeq++
	mySeq := c.nextSeq
	c.mu.Unlock()

	// Write happens outside the committer mutex. writeUnsynced takes the
	// catalogLog's own mutex briefly to serialize file appends; no fsync
	// here.
	if err := c.log.writeUnsynced(rec); err != nil {
		// Even on error we publish our seq so a later sync can advance
		// past us. Otherwise one failed write would stall all subsequent
		// appends waiting on the cond.
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
			return errCatalogCommitterClosed
		}
		c.pending.Wait()
	}
	c.mu.Unlock()
	return nil
}

func (c *catalogCommitter) run() {
	defer close(c.done)
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = c.tick()
		case <-c.stop:
			// Final drain so any Append() that returned successfully
			// has definitely been fsync'd before we exit.
			_ = c.tick()
			return
		}
	}
}

// flushAndStop drains pending writes one final time and stops the
// goroutine. Called from Store.Close (via catalogLog.Close). Idempotent.
func (c *catalogCommitter) flushAndStop() {
	c.closeOnce.Do(func() {
		close(c.stop)
		<-c.done
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		c.pending.Broadcast()
	})
}

func (c *catalogCommitter) tick() error {
	c.mu.Lock()
	target := c.lastWrittenSeq
	already := c.syncedSeq.Load()
	c.mu.Unlock()
	if target <= already {
		return nil
	}
	if err := c.log.sync(); err != nil {
		// Don't advance syncedSeq on error — waiters must see "their
		// bytes were NOT durable". They'll be unblocked by Close or by
		// a subsequent successful tick.
		return err
	}
	c.syncedSeq.Store(target)
	c.mu.Lock()
	c.pending.Broadcast()
	c.mu.Unlock()
	return nil
}
