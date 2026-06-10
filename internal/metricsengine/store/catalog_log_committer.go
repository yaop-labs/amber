package store

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// catalogCommitter batches fsyncs for catalog log appends.
// Append returns after the caller's sequence has been synced. If a process
// exits before a batched REGISTER reaches disk, startup reconciliation can
// rebuild the series from block directories.
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

// Append writes a record and waits until the committer has fsynced it.
// On error, the caller should treat ingest as failed and leave in-memory state
// unchanged.
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

	if err := c.log.writeUnsynced(rec); err != nil {
		// Publish the sequence so a later sync can advance waiters past a
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
			// Drain once before exit.
			_ = c.tick()
			return
		}
	}
}

// flushAndStop drains pending writes and stops the goroutine.
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
		return err
	}
	c.syncedSeq.Store(target)
	c.mu.Lock()
	c.pending.Broadcast()
	c.mu.Unlock()
	return nil
}
