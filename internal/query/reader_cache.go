package query

import (
	"container/list"
	"sync"

	"github.com/yaop-labs/amber/internal/storage"
)

type readerCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	order    *list.List
}

type cachedReader struct {
	mu     sync.Mutex
	path   string
	reader *storage.SegmentReader
}

type readerCacheEntry struct {
	path   string
	cached *cachedReader
}

func newReaderCache(capacity int) *readerCache {
	if capacity < 1 {
		capacity = 1
	}
	return &readerCache{
		capacity: capacity,
		items:    make(map[string]*list.Element, capacity),
		order:    list.New(),
	}
}

func (c *readerCache) acquire(path string) (*cachedReader, error) {
	c.mu.Lock()
	if el, ok := c.items[path]; ok {
		c.order.MoveToFront(el)
		cr := el.Value.(*readerCacheEntry).cached
		c.mu.Unlock()
		cr.mu.Lock()
		return cr, nil
	}
	c.mu.Unlock()

	sr, err := storage.OpenSegmentReader(path, nil)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	if el, ok := c.items[path]; ok {

		cr := el.Value.(*readerCacheEntry).cached
		c.order.MoveToFront(el)
		c.mu.Unlock()
		_ = sr.Close()
		cr.mu.Lock()
		return cr, nil
	}

	cr := &cachedReader{path: path, reader: sr}
	ent := &readerCacheEntry{path: path, cached: cr}
	el := c.order.PushFront(ent)
	c.items[path] = el

	var evicted *cachedReader
	if c.order.Len() > c.capacity {
		oldest := c.order.Back()
		if oldest != nil {
			oldEnt := oldest.Value.(*readerCacheEntry)
			c.order.Remove(oldest)
			delete(c.items, oldEnt.path)
			evicted = oldEnt.cached
		}
	}
	c.mu.Unlock()

	if evicted != nil {

		go func(e *cachedReader) {
			e.mu.Lock()
			if e.reader != nil {
				_ = e.reader.Close()
				e.reader = nil
			}
			e.mu.Unlock()
		}(evicted)
	}

	cr.mu.Lock()
	return cr, nil
}

func (c *readerCache) release(cr *cachedReader) {
	cr.mu.Unlock()
}

func (c *readerCache) invalidate(path string) {
	c.mu.Lock()
	el, ok := c.items[path]
	if !ok {
		c.mu.Unlock()
		return
	}
	ent := el.Value.(*readerCacheEntry)
	c.order.Remove(el)
	delete(c.items, path)
	c.mu.Unlock()

	go func(e *cachedReader) {
		e.mu.Lock()
		if e.reader != nil {
			_ = e.reader.Close()
			e.reader = nil
		}
		e.mu.Unlock()
	}(ent.cached)
}

func (c *readerCache) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, el := range c.items {
		ent := el.Value.(*readerCacheEntry)
		ent.cached.mu.Lock()
		if ent.cached.reader != nil {
			_ = ent.cached.reader.Close()
			ent.cached.reader = nil
		}
		ent.cached.mu.Unlock()
	}
	c.items = make(map[string]*list.Element)
	c.order.Init()
}
