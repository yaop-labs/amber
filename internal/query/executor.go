// Package query plans and executes log and span queries against sealed and
// active segments, using bitmap, FTS, and ribbon-filter indexes.
package query

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/indexer"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/selfobs"
	"github.com/yaop-labs/amber/internal/storage"
)

type logMinHeap []model.LogEntry

func (h logMinHeap) Len() int { return len(h) }
func (h logMinHeap) Less(i, j int) bool {
	return logEntryOlder(h[i], h[j])
}
func (h logMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *logMinHeap) Push(x any)   { *h = append(*h, x.(model.LogEntry)) }
func (h *logMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

type spanMinHeap []model.SpanEntry

func (h spanMinHeap) Len() int { return len(h) }
func (h spanMinHeap) Less(i, j int) bool {
	return spanEntryOlder(h[i], h[j])
}
func (h spanMinHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *spanMinHeap) Push(x any)   { *h = append(*h, x.(model.SpanEntry)) }
func (h *spanMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func peekEntryIDUint64(data []byte) (uint64, bool) {
	if len(data) < 10 {
		return 0, false
	}
	return binary.BigEndian.Uint64(data[2:10]), true
}

func logEntryOlder(a, b model.LogEntry) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.Before(b.Timestamp)
	}
	return bytes.Compare(a.ID[:], b.ID[:]) < 0
}

func logEntryNewer(a, b model.LogEntry) bool {
	if !a.Timestamp.Equal(b.Timestamp) {
		return a.Timestamp.After(b.Timestamp)
	}
	return bytes.Compare(a.ID[:], b.ID[:]) > 0
}

func spanEntryOlder(a, b model.SpanEntry) bool {
	if !a.StartTime.Equal(b.StartTime) {
		return a.StartTime.Before(b.StartTime)
	}
	return bytes.Compare(a.ID[:], b.ID[:]) < 0
}

func spanEntryNewer(a, b model.SpanEntry) bool {
	if !a.StartTime.Equal(b.StartTime) {
		return a.StartTime.After(b.StartTime)
	}
	return bytes.Compare(a.ID[:], b.ID[:]) > 0
}

// Executor runs log and span queries: it prunes segments, applies the bitmap,
// FTS, ribbon, and posting-list sidecars (registered warm at seal time and
// LRU-cached when loaded from disk), reverse-scans with a top-k heap, and
// paginates by cursor. It also serves a result cache. Safe for concurrent use.
type Executor struct {
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	spanSparse  *index.SparseIndex
	planner     *Planner
	logDir      string
	spanDir     string

	active *indexer.ActiveIndex

	logBitmapCache    *indexLRU[*index.MultiFieldIndex]
	spanBitmapCache   *indexLRU[index.SpanBitmap]
	spanCoverCache    *indexLRU[*index.CoverIndex]
	ftsCache          *indexLRU[*index.FTSIndex]
	logPostingCache   *indexLRU[*index.PostingList]
	spanPostingCache  *indexLRU[*index.PostingList]
	logRibbonCache    *indexLRU[*index.RibbonFilter]
	logFTSRibbonCache *indexLRU[*index.RibbonFilter]
	spanRibbonCache   *indexLRU[*index.RibbonFilter]

	logReaders  *readerCache
	spanReaders *readerCache

	resultCache *queryCache

	// logActiveServices/spanActiveServices memoize the one-time service scan
	// of the current active segment (records that predate this process and so
	// are absent from ActiveIndex). Entries written while the process runs
	// reach ActiveIndex, and rotation changes the file name, so a cached set
	// stays valid for the lifetime of its segment.
	logActiveServices  activeServicesCache
	spanActiveServices activeServicesCache
}

type activeServicesCache struct {
	mu   sync.Mutex
	file string
	set  map[string]struct{}
}

type corruptRefetchContextKey struct{}

type queryCacheEntry struct {
	logs    *LogResult
	spans   *SpanResult
	expires int64
	// from/to is the query's time window (unixnano), used to invalidate only
	// entries an ingest batch can affect. Records carry event time, so a
	// batch may land anywhere - overlap with the batch's [min,max] timestamp
	// range is the correctness condition, not "touches the active segment".
	from int64
	to   int64
}

type queryCache struct {
	mu             sync.Mutex
	entries        map[[32]byte]queryCacheEntry
	inflight       map[[32]byte]chan struct{}
	logGeneration  uint64
	spanGeneration uint64
	ttl            time.Duration
	maxSize        int
}

func newQueryCache(maxSize int, ttl time.Duration) *queryCache {
	if maxSize <= 0 || ttl <= 0 {
		return nil
	}
	return &queryCache{
		entries:  make(map[[32]byte]queryCacheEntry, maxSize),
		inflight: make(map[[32]byte]chan struct{}),
		ttl:      ttl,
		maxSize:  maxSize,
	}
}

func (c *queryCache) waitOrStart(ctx context.Context, key [32]byte) (wait bool, done func(), err error) {
	if c == nil {
		return false, func() {}, nil
	}
	c.mu.Lock()
	if ch, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-ch:
			return true, nil, nil
		case <-ctx.Done():
			return false, nil, ctx.Err()
		}
	}
	ch := make(chan struct{})
	c.inflight[key] = ch
	c.mu.Unlock()
	return false, func() {
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(ch)
	}, nil
}

func (c *queryCache) getLog(key [32]byte) (*LogResult, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	e, ok := c.entries[key]
	c.mu.Unlock()
	if !ok || e.logs == nil || time.Now().UnixNano() > e.expires {
		return nil, false
	}
	return cloneLogResult(e.logs), true
}

func (c *queryCache) getSpan(key [32]byte) (*SpanResult, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	e, ok := c.entries[key]
	c.mu.Unlock()
	if !ok || e.spans == nil || time.Now().UnixNano() > e.expires {
		return nil, false
	}
	return cloneSpanResult(e.spans), true
}

func (c *queryCache) logGenerationSnapshot() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.logGeneration
}

func (c *queryCache) spanGenerationSnapshot() uint64 {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.spanGeneration
}

func (c *queryCache) putLog(key [32]byte, r *LogResult, from, to int64, generation uint64) bool {
	if c == nil || r == nil {
		return false
	}
	// Empty results are not cached; ingest may make them stale immediately.
	if len(r.Entries) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.logGeneration {
		return false
	}
	if len(c.entries) >= c.maxSize {
		c.sweepLocked()
	}
	c.entries[key] = queryCacheEntry{
		logs:    cloneLogResult(r),
		expires: time.Now().Add(c.ttl).UnixNano(),
		from:    from,
		to:      to,
	}
	return true
}

func (c *queryCache) putSpan(key [32]byte, r *SpanResult, from, to int64, generation uint64) bool {
	if c == nil || r == nil {
		return false
	}
	if len(r.Spans) == 0 {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if generation != c.spanGeneration {
		return false
	}
	if len(c.entries) >= c.maxSize {
		c.sweepLocked()
	}
	c.entries[key] = queryCacheEntry{
		spans:   cloneSpanResult(r),
		expires: time.Now().Add(c.ttl).UnixNano(),
		from:    from,
		to:      to,
	}
	return true
}

func (c *queryCache) invalidateLogRange(from, to int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.logGeneration++
	for k, e := range c.entries {
		if e.logs != nil && e.from <= to && e.to >= from {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
}

func (c *queryCache) invalidateSpanRange(from, to int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.spanGeneration++
	for k, e := range c.entries {
		if e.spans != nil && e.from <= to && e.to >= from {
			delete(c.entries, k)
		}
	}
	c.mu.Unlock()
}

func (c *queryCache) sweepLocked() {
	now := time.Now().UnixNano()
	for k, e := range c.entries {
		if e.expires < now {
			delete(c.entries, k)
		}
	}
	if len(c.entries) >= c.maxSize {
		c.entries = make(map[[32]byte]queryCacheEntry, c.maxSize)
	}
}

func (c *queryCache) clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.logGeneration++
	c.spanGeneration++
	c.entries = make(map[[32]byte]queryCacheEntry, c.maxSize)
	c.mu.Unlock()
}

func (c *queryCache) clearLogs() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.logGeneration++
	for key, entry := range c.entries {
		if entry.logs != nil {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
}

func (c *queryCache) clearSpans() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.spanGeneration++
	for key, entry := range c.entries {
		if entry.spans != nil {
			delete(c.entries, key)
		}
	}
	c.mu.Unlock()
}

func cloneLogResult(src *LogResult) *LogResult {
	if src == nil {
		return nil
	}
	dst := *src
	dst.Entries = append([]model.LogEntry(nil), src.Entries...)
	for i := range dst.Entries {
		dst.Entries[i].Attrs = append([]model.Attr(nil), src.Entries[i].Attrs...)
	}
	return &dst
}

func cloneSpanResult(src *SpanResult) *SpanResult {
	if src == nil {
		return nil
	}
	dst := *src
	dst.Spans = append([]model.SpanEntry(nil), src.Spans...)
	for i := range dst.Spans {
		dst.Spans[i].Attrs = append([]model.Attr(nil), src.Spans[i].Attrs...)
	}
	return &dst
}

func hashLogQuery(q *LogQuery) [32]byte {
	h := sha256.New()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(q.From.UnixNano()))
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(q.To.UnixNano()))
	h.Write(buf[:])
	h.Write([]byte{'|'})
	for _, s := range q.Services {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})
	for _, s := range q.Hosts {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})
	for _, s := range q.Levels {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})

	if len(q.Attrs) > 0 {
		keys := make([]string, 0, len(q.Attrs))
		for k := range q.Attrs {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			h.Write([]byte(k))
			h.Write([]byte{0})
			h.Write([]byte(q.Attrs[k]))
			h.Write([]byte{0})
		}
	}
	h.Write([]byte{'|'})
	h.Write(q.TraceID[:])
	h.Write([]byte{'|'})
	h.Write([]byte(q.FullText))
	h.Write([]byte{'|'})
	binary.BigEndian.PutUint64(buf[:], uint64(q.Limit))
	h.Write(buf[:])
	h.Write([]byte{'|'})
	h.Write([]byte(q.Cursor))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func hashSpanQuery(q *SpanQuery) [32]byte {
	h := sha256.New()
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(q.From.UnixNano()))
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(q.To.UnixNano()))
	h.Write(buf[:])
	h.Write([]byte{'|'})
	for _, s := range q.Services {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})
	for _, s := range q.Operations {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	h.Write([]byte{'|'})
	h.Write(q.TraceID[:])
	h.Write([]byte{'|'})
	binary.BigEndian.PutUint64(buf[:], uint64(q.MinDuration))
	h.Write(buf[:])
	binary.BigEndian.PutUint64(buf[:], uint64(q.MaxDuration))
	h.Write(buf[:])
	h.Write([]byte{'|'})
	for _, s := range q.Statuses {
		h.Write([]byte{byte(s)})
	}
	h.Write([]byte{'|'})
	binary.BigEndian.PutUint64(buf[:], uint64(q.Limit))
	h.Write(buf[:])
	h.Write([]byte{'|'})
	h.Write([]byte(q.Cursor))
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

const defaultIndexCacheSize = 32

const (
	defaultResultCacheSize = 256
	defaultResultCacheTTL  = 5 * time.Second
)

// NewExecutor builds an executor with the default index cache size and no
// explicit segment directories (sealed sidecars are loaded relative to the
// managers). NewExecutorWithCache gives full control.
func NewExecutor(
	logManager *storage.SegmentManager,
	spanManager *storage.SegmentManager,
	logSparse *index.SparseIndex,
	spanSparse *index.SparseIndex,
) *Executor {
	return NewExecutorWithCache(logManager, spanManager, logSparse, spanSparse, "", "", defaultIndexCacheSize)
}

// NewExecutorWithCache builds an executor with the given log/span segment
// directories and sealed-index LRU cache size.
func NewExecutorWithCache(
	logManager *storage.SegmentManager,
	spanManager *storage.SegmentManager,
	logSparse *index.SparseIndex,
	spanSparse *index.SparseIndex,
	logDir, spanDir string,
	cacheSize int,
) *Executor {
	if cacheSize < 1 {
		cacheSize = defaultIndexCacheSize
	}
	return &Executor{
		logManager:        logManager,
		spanManager:       spanManager,
		logSparse:         logSparse,
		spanSparse:        spanSparse,
		planner:           NewPlanner(logSparse),
		logDir:            logDir,
		spanDir:           spanDir,
		active:            indexer.New(logManager, spanManager),
		logBitmapCache:    newIndexLRU[*index.MultiFieldIndex](cacheSize),
		spanBitmapCache:   newIndexLRU[index.SpanBitmap](cacheSize),
		spanCoverCache:    newIndexLRU[*index.CoverIndex](cacheSize),
		ftsCache:          newIndexLRU[*index.FTSIndex](cacheSize),
		logPostingCache:   newIndexLRU[*index.PostingList](cacheSize),
		spanPostingCache:  newIndexLRU[*index.PostingList](cacheSize),
		logRibbonCache:    newIndexLRU[*index.RibbonFilter](cacheSize),
		logFTSRibbonCache: newIndexLRU[*index.RibbonFilter](cacheSize),
		spanRibbonCache:   newIndexLRU[*index.RibbonFilter](cacheSize),
		logReaders:        newReaderCache(cacheSize),
		spanReaders:       newReaderCache(cacheSize),
		resultCache:       newQueryCache(defaultResultCacheSize, defaultResultCacheTTL),
	}
}

// ActiveIndex returns the writer-side active index.
func (e *Executor) ActiveIndex() *indexer.ActiveIndex { return e.active }

// SetSegmentStores configures remote fetches for missing local segments.
func (e *Executor) SetSegmentStores(logStore, spanStore storage.SegmentStore, log *slog.Logger) {
	if logStore != nil && e.logReaders != nil {
		e.logReaders.setFetcher(makeStoreFetcher(logStore, e.logDir, "logs", log))
	}
	if spanStore != nil && e.spanReaders != nil {
		e.spanReaders.setFetcher(makeStoreFetcher(spanStore, e.spanDir, "spans", log))
	}
}

// InvalidateLogSegment drops cached sidecar indexes for a log segment, e.g.
// after it is removed by retention.
func (e *Executor) InvalidateLogSegment(seg storage.SegmentMeta) {
	if e.logReaders != nil {
		e.logReaders.invalidate(e.logManager.SegmentPath(seg))
	}
	e.logBitmapCache.delete(seg.FileName)
	e.ftsCache.delete(seg.FileName)
	e.logPostingCache.delete(seg.FileName)
	e.logRibbonCache.delete(seg.FileName)
	e.logFTSRibbonCache.delete(seg.FileName)
	e.resultCache.clearLogs()
}

// ClearResultCache drops cached query results.
func (e *Executor) ClearResultCache() {
	e.resultCache.clear()
}

// InvalidateLogResultRange advances the log data generation and drops cached
// log results whose query window overlaps the new batch.
func (e *Executor) InvalidateLogResultRange(from, to int64) {
	e.resultCache.invalidateLogRange(from, to)
}

// InvalidateSpanResultRange advances the span data generation and drops cached
// span results whose query window overlaps the new batch.
func (e *Executor) InvalidateSpanResultRange(from, to int64) {
	e.resultCache.invalidateSpanRange(from, to)
}

// InvalidateSpanSegment drops cached sidecar indexes for a span segment.
func (e *Executor) InvalidateSpanSegment(seg storage.SegmentMeta) {
	if e.spanReaders != nil {
		e.spanReaders.invalidate(e.spanManager.SegmentPath(seg))
	}
	e.spanBitmapCache.delete(seg.FileName)
	e.spanCoverCache.delete(seg.FileName)
	e.spanPostingCache.delete(seg.FileName)
	e.spanRibbonCache.delete(seg.FileName)
	e.resultCache.clearSpans()
}

// Close releases the executor's cached resources.
func (e *Executor) Close() {
	if e.logReaders != nil {
		e.logReaders.close()
	}
	if e.spanReaders != nil {
		e.spanReaders.close()
	}
}

// The Register* methods install a freshly built sidecar index for a sealed
// segment directly into the executor's caches ("warm registration"), so the
// first query after a seal skips reloading it from disk.

// RegisterBitmapIndex registers a log segment's bitmap index.
func (e *Executor) RegisterBitmapIndex(segmentFile string, idx *index.MultiFieldIndex) {
	e.logBitmapCache.put(segmentFile, idx)
}

// RegisterSpanBitmapIndex registers a span segment's bitmap index.
func (e *Executor) RegisterSpanBitmapIndex(segmentFile string, idx *index.MultiFieldIndex) {
	e.spanBitmapCache.put(segmentFile, idx)
}

// RegisterFTSIndex registers a log segment's full-text index.
func (e *Executor) RegisterFTSIndex(segmentFile string, idx *index.FTSIndex) {
	e.ftsCache.put(segmentFile, idx)
}

// RegisterLogRibbon registers a log segment's service-name ribbon filter.
func (e *Executor) RegisterLogRibbon(segmentFile string, f *index.RibbonFilter) {
	e.logRibbonCache.put(segmentFile, f)
}

// RegisterLogFTSRibbon registers a log segment's FTS-token ribbon filter.
func (e *Executor) RegisterLogFTSRibbon(segmentFile string, f *index.RibbonFilter) {
	e.logFTSRibbonCache.put(segmentFile, f)
}

// RegisterSpanRibbon registers a span segment's service-name ribbon filter.
func (e *Executor) RegisterSpanRibbon(segmentFile string, f *index.RibbonFilter) {
	e.spanRibbonCache.put(segmentFile, f)
}

// RegisterLogPostingList registers a log segment's trace-ID posting list.
func (e *Executor) RegisterLogPostingList(segmentFile string, pl *index.PostingList) {
	e.logPostingCache.put(segmentFile, pl)
}

// RegisterSpanPostingList registers a span segment's trace-ID posting list.
func (e *Executor) RegisterSpanPostingList(segmentFile string, pl *index.PostingList) {
	e.spanPostingCache.put(segmentFile, pl)
}

func (e *Executor) logPosting(name string) (*index.PostingList, bool) {
	if pl, ok := e.logPostingCache.get(name); ok {
		return pl, true
	}
	if e.logDir == "" {
		return nil, false
	}
	pl, err := index.LoadPostingList(e.logDir + "/" + name + ".pidx")
	if err != nil {
		return nil, false
	}
	e.logPostingCache.put(name, pl)
	return pl, true
}

func (e *Executor) spanPosting(name string) (*index.PostingList, bool) {
	if pl, ok := e.spanPostingCache.get(name); ok {
		return pl, true
	}
	if e.spanDir == "" {
		return nil, false
	}
	pl, err := index.LoadPostingList(e.spanDir + "/" + name + ".pidx")
	if err != nil {
		return nil, false
	}
	e.spanPostingCache.put(name, pl)
	return pl, true
}

func (e *Executor) logBitmap(name string) (*index.MultiFieldIndex, bool) {
	if idx, ok := e.active.LookupLog(name); ok {
		return idx, true
	}
	if idx, ok := e.logBitmapCache.get(name); ok {
		return idx, true
	}
	if e.logDir == "" {
		return nil, false
	}
	idx, err := index.LoadMultiFieldIndex(filepath.Join(e.logDir, name+".bidx"))
	if err != nil {
		return nil, false
	}
	e.logBitmapCache.put(name, idx)
	return idx, true
}

func (e *Executor) spanBitmap(name string) (index.SpanBitmap, bool) {
	if idx, ok := e.active.LookupSpan(name); ok {
		return idx, true
	}
	if idx, ok := e.spanBitmapCache.get(name); ok {
		return idx, true
	}
	if e.spanDir == "" {
		return nil, false
	}
	path := filepath.Join(e.spanDir, name+".bidx")
	// Sealed BID3 indexes open lazily (directory only); a query then preads
	// just the postings it needs instead of decoding the whole .bidx. Legacy
	// BID2 files fall back to a full resident load.
	if idx, err := index.OpenSeekableBitmapIndex(path); err == nil {
		e.spanBitmapCache.put(name, idx)
		return idx, true
	} else if !errors.Is(err, index.ErrBitmapNotSeekable) {
		return nil, false
	}
	idx, err := index.LoadMultiFieldIndex(path)
	if err != nil {
		return nil, false
	}
	e.spanBitmapCache.put(name, idx)
	return idx, true
}

func (e *Executor) fts(name string) (*index.FTSIndex, bool) {
	if idx, ok := e.ftsCache.get(name); ok {
		return idx, true
	}
	if e.logDir == "" {
		return nil, false
	}
	idx, err := index.LoadFTSIndex(filepath.Join(e.logDir, name+".fidx"))
	if err != nil {
		return nil, false
	}
	e.ftsCache.put(name, idx)
	return idx, true
}

func (e *Executor) logRibbon(name string) (*index.RibbonFilter, bool) {
	if f, ok := e.logRibbonCache.get(name); ok {
		return f, true
	}
	if e.logDir == "" {
		return nil, false
	}
	f, err := index.LoadRibbonFilter(filepath.Join(e.logDir, name+".filt"))
	if err != nil {
		return nil, false
	}
	e.logRibbonCache.put(name, f)
	return f, true
}

func (e *Executor) logFTSRibbon(name string) (*index.RibbonFilter, bool) {
	if f, ok := e.logFTSRibbonCache.get(name); ok {
		return f, true
	}
	if e.logDir == "" {
		return nil, false
	}
	f, err := index.LoadRibbonFilter(filepath.Join(e.logDir, name+".fts.filt"))
	if err != nil {
		return nil, false
	}
	e.logFTSRibbonCache.put(name, f)
	return f, true
}

func (e *Executor) spanRibbon(name string) (*index.RibbonFilter, bool) {
	if f, ok := e.spanRibbonCache.get(name); ok {
		return f, true
	}
	if e.spanDir == "" {
		return nil, false
	}
	f, err := index.LoadRibbonFilter(filepath.Join(e.spanDir, name+".filt"))
	if err != nil {
		return nil, false
	}
	e.spanRibbonCache.put(name, f)
	return f, true
}

// Services returns the distinct service names known across segments, for the
// services API.
func (e *Executor) Services() []string {
	seen := make(map[string]struct{})

	for _, seg := range e.logManager.Segments() {
		if idx, ok := e.logBitmap(seg.FileName); ok {
			for _, s := range idx.FieldValues("service") {
				seen[s] = struct{}{}
			}
		}
	}
	for _, seg := range e.spanManager.Segments() {
		if idx, ok := e.spanBitmap(seg.FileName); ok {
			for _, s := range idx.FieldValues("service") {
				seen[s] = struct{}{}
			}
		}
	}

	if activeMeta, ok := e.logManager.ActiveSegmentMeta(); ok {
		if idx, ok := e.active.LookupLog(activeMeta.FileName); ok {
			for _, s := range idx.FieldValues("service") {
				seen[s] = struct{}{}
			}
		}
	}
	if activeMeta, ok := e.spanManager.ActiveSegmentMeta(); ok {
		if idx, ok := e.active.LookupSpan(activeMeta.FileName); ok {
			for _, s := range idx.FieldValues("service") {
				seen[s] = struct{}{}
			}
		}
	}

	e.scanActiveServices(seen)

	services := make([]string, 0, len(seen))
	for s := range seen {
		services = append(services, s)
	}
	return services
}

func (e *Executor) scanActiveServices(seen map[string]struct{}) {
	streams := []struct {
		manager *storage.SegmentManager
		cache   *activeServicesCache
		isLog   bool
	}{
		{manager: e.logManager, cache: &e.logActiveServices, isLog: true},
		{manager: e.spanManager, cache: &e.spanActiveServices, isLog: false},
	}
	for _, stream := range streams {
		mgr := stream.manager
		if mgr == nil {
			continue
		}
		activeMeta, ok := mgr.ActiveSegmentMeta()
		if !ok {
			continue
		}
		cache := stream.cache
		cache.mu.Lock()
		if cache.file != activeMeta.FileName {
			set := make(map[string]struct{})
			segPath := mgr.SegmentPath(activeMeta)
			hint, _ := mgr.ActiveBlockIndex(activeMeta.FileName)
			if sr, err := storage.OpenSegmentReader(segPath, hint); err == nil {
				_ = sr.Scan(func(data []byte) error {
					if stream.isLog {
						var logEntry model.LogEntry
						if _, err := logEntry.ReadFrom(bytes.NewReader(data)); err != nil {
							return err
						}
						if logEntry.Service != "" {
							set[logEntry.Service] = struct{}{}
						}
						return nil
					}
					var spanEntry model.SpanEntry
					if _, err := spanEntry.ReadFrom(bytes.NewReader(data)); err != nil {
						return err
					}
					if spanEntry.Service != "" {
						set[spanEntry.Service] = struct{}{}
					}
					return nil
				})
				_ = sr.Close()
				cache.file = activeMeta.FileName
				cache.set = set
			}
		}
		for s := range cache.set {
			seen[s] = struct{}{}
		}
		cache.mu.Unlock()
	}
}

func filterQueryableSegments(segs []index.SegmentTimeRange, manager *storage.SegmentManager) []index.SegmentTimeRange {
	if len(segs) == 0 || manager == nil {
		return segs
	}
	out := segs[:0]
	for _, seg := range segs {
		if manager.IsQueryableSegment(seg.FileName) {
			out = append(out, seg)
		}
	}
	return out
}

// ExecLog runs a log query: it serves from the result cache when possible,
// otherwise prunes segments, applies the available indexes, reverse-scans newest
// first into a top-k heap, post-filters, and returns one cursor-paginated page.
func (e *Executor) ExecLog(ctx context.Context, q *LogQuery) (r *LogResult, err error) {
	start := time.Now()
	defer func() {
		selfobs.QueryDuration.WithLabelValues("log").Observe(time.Since(start).Seconds())
		if err != nil {
			selfobs.QueryErrors.WithLabelValues("log").Inc()
			return
		}
		cache := "miss"
		if r != nil && r.CacheHit {
			cache = "hit"
		}
		selfobs.QueryTotal.WithLabelValues("log", cache).Inc()
	}()

	if q == nil {
		return nil, errors.New("query: log query is nil")
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cacheKey := hashLogQuery(q)
	var queryGeneration uint64

	for {
		if cached, ok := e.resultCache.getLog(cacheKey); ok {
			cached.CacheHit = true
			return cached, nil
		}
		wait, done, err := e.resultCache.waitOrStart(ctx, cacheKey)
		if err != nil {
			return nil, err
		}
		if wait {
			continue
		}
		defer done()
		queryGeneration = e.resultCache.logGenerationSnapshot()
		break
	}

	plan := e.planner.Plan(q)

	if len(plan.Segments) == 0 {
		// Don't cache: queryCache.putLog skips empty results to avoid
		// pinning a stale "no data" answer over a 5s TTL.
		return &LogResult{}, nil
	}

	segs := make([]index.SegmentTimeRange, len(plan.Segments))
	copy(segs, plan.Segments)
	segs = filterQueryableSegments(segs, e.logManager)
	sort.Slice(segs, func(i, j int) bool { return segs[i].MaxTS > segs[j].MaxTS })

	cursor, _ := DecodeCursor(q.Cursor) // pre-validated in q.Validate

	k := q.Limit
	if k <= 0 {
		k = 100
	}

	var ftsTokens [][]byte
	if q.FullText != "" {
		for _, tok := range index.TokenizeFTS(q.FullText) {
			if tok != "" {
				ftsTokens = append(ftsTokens, []byte(tok))
			}
		}
	}

	hp := &logMinHeap{}
	heap.Init(hp)
	totalHits := 0
	scanned := 0
	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !cursor.IsZero() && seg.MinTS > cursor.Timestamp {
			continue
		}
		if hp.Len() >= k {
			thresholdTS := (*hp)[0].Timestamp.UnixNano()
			if seg.MaxTS < thresholdTS {
				continue
			}
		}
		scanned++
		matched, err := e.execLogSegment(ctx, q, plan, seg, cursor, hp, k, ftsTokens)
		if err != nil {
			return nil, fmt.Errorf("executor: segment %s: %w", seg.FileName, err)
		}
		totalHits += matched
	}

	entries := make([]model.LogEntry, hp.Len())
	for i := len(entries) - 1; i >= 0; i-- {
		entries[i] = heap.Pop(hp).(model.LogEntry)
	}

	truncated := len(entries) == q.Limit
	var nextCursor string
	if truncated {
		last := entries[len(entries)-1]
		nextCursor = EncodeCursor(Cursor{
			Timestamp: last.Timestamp.UnixNano(),
			EntryID:   last.ID,
		})
	}

	result := &LogResult{
		Entries:    entries,
		TotalHits:  totalHits,
		Truncated:  truncated,
		NextCursor: nextCursor,
		SegTotal:   len(segs),
		SegScanned: scanned,
	}
	e.resultCache.putLog(cacheKey, result, q.FromUnixNano(), q.ToUnixNano(), queryGeneration)
	return result, nil
}

func (e *Executor) execLogSegment(
	ctx context.Context,
	q *LogQuery,
	plan *ExecutionPlan,
	seg index.SegmentTimeRange,
	cursor Cursor,
	hp *logMinHeap,
	k int,
	ftsTokens [][]byte,
) (int, error) {

	// allowedSlice is the sorted candidate-ID set ANDed across the index
	// steps; hasFilter distinguishes "no filtering" from "filtered to
	// something" (an empty filtered set returns early).
	var allowedSlice []uint64
	hasFilter := false
	restrict := func(ids []uint64) bool {
		if !hasFilter {
			allowedSlice = ids
			hasFilter = true
		} else {
			allowedSlice = index.IntersectSorted(allowedSlice, ids)
		}
		return len(allowedSlice) > 0
	}

	if plan.HasStep(StepBitmapFilter) {
		if bm, ok := e.logBitmap(seg.FileName); ok {
			conditions := buildBitmapConditions(q)
			if len(conditions) > 0 {
				if !restrict(bm.FilterMulti(conditions)) {
					return 0, nil
				}
			}
		}
	}

	needScanFTS := false
	if plan.HasStep(StepFTSSearch) {

		if len(ftsTokens) > 0 {
			if ribbon, ok := e.logFTSRibbon(seg.FileName); ok {
				allHit := true
				for _, token := range ftsTokens {
					if !ribbon.Contains(token) {
						allHit = false
						break
					}
				}
				if !allHit {
					return 0, nil
				}
			}
		}
		if fts, ok := e.fts(seg.FileName); ok {
			// The row scan applies top-k after all predicates. Capping this
			// ascending candidate set at the page limit (or at the default
			// 100K segment size) would discard newer matches in stores using a
			// larger rotation policy. The index is only a pruning aid, so it
			// must return the complete per-segment candidate set.
			ftsIDs, err := fts.Search(ctx, q.FullText, 0)
			if err != nil {
				return 0, fmt.Errorf("fts search: %w", err)
			}
			if !restrict(ftsIDs) {
				return 0, nil
			}
		} else if len(ftsTokens) > 0 {
			needScanFTS = true
		}
	}

	if !model.IsZeroTraceID(q.TraceID) {
		if ribbon, ok := e.logRibbon(seg.FileName); ok {
			if !ribbon.Contains(q.TraceID[:]) {
				return 0, nil
			}
		}
		if pl, ok := e.logPosting(seg.FileName); ok {
			ids := pl.Lookup(q.TraceID[:])
			if len(ids) == 0 {
				return 0, nil
			}
			slices.Sort(ids)
			if !restrict(ids) {
				return 0, nil
			}
		}
	}

	segPath := e.logManager.SegmentPath(storage.SegmentMeta{FileName: seg.FileName})

	var sr *storage.SegmentReader
	if hint, isActive := e.logManager.ActiveBlockIndex(seg.FileName); isActive {
		var err error
		sr, err = storage.OpenSegmentReader(segPath, hint)
		if err != nil {
			return 0, fmt.Errorf("open segment: %w", err)
		}
		defer func() { _ = sr.Close() }()
	} else {
		cr, err := e.logReaders.acquire(segPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && !e.logManager.IsQueryableSegment(seg.FileName) {
				return 0, nil
			}
			return 0, fmt.Errorf("open segment: %w", err)
		}
		defer e.logReaders.release(cr)
		sr = cr.reader
	}

	heapBeforeScan := append(logMinHeap(nil), (*hp)...)
	matched := 0

	var ftsTokenStrs []string
	var ftsMemo map[string]bool
	if needScanFTS {
		ftsTokenStrs = make([]string, len(ftsTokens))
		for i, t := range ftsTokens {
			ftsTokenStrs[i] = string(t)
		}
		ftsMemo = make(map[string]bool)
	}

	skip := func(minID, maxID uint64) bool {
		if ctx.Err() != nil {
			return false
		}
		// Once the heap holds k entries, a block whose newest record is
		// older than the oldest kept entry cannot improve the result - the
		// intra-segment mirror of the per-segment MaxTS skip. Entry IDs are
		// ULID-derived (bits 63..32 = low 32 bits of the ms timestamp), so
		// the comparison runs in that truncated space with a wrap guard;
		// blocks span minutes, the wrap period is ~49 days.
		if hp.Len() >= k {
			thrLow := uint32((*hp)[0].Timestamp.UnixMilli())
			blockLow := uint32(maxID >> 32)
			if delta := thrLow - blockLow; delta > 0 && delta < 1<<31 {
				return true
			}
		}
		if hasFilter {
			i := sort.Search(len(allowedSlice), func(i int) bool {
				return allowedSlice[i] >= minID
			})
			if i == len(allowedSlice) || allowedSlice[i] > maxID {
				return true
			}
		}
		return false
	}

	scanFn := func(data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		id, idOK := peekEntryIDUint64(data)
		if hasFilter && idOK {
			if _, found := slices.BinarySearch(allowedSlice, id); !found {
				return nil
			}
		}

		var entry model.LogEntry
		if err := entry.DecodeBytes(data); err != nil {
			return fmt.Errorf("decode log record: %w", err)
		}

		if !matchesTimeRange(entry, q) || !matchesAttrs(entry, q) {
			return nil
		}
		if len(q.Services) > 0 && !containsStr(q.Services, entry.Service) {
			return nil
		}
		if len(q.Levels) > 0 && !containsStr(q.Levels, entry.Level.String()) {
			return nil
		}
		if len(q.Hosts) > 0 && !containsStr(q.Hosts, entry.Host) {
			return nil
		}

		if needScanFTS {
			match, seen := ftsMemo[entry.Body]
			if !seen {
				match = bodyMatchesTokens(entry.Body, ftsTokenStrs)
				if len(ftsMemo) < ftsMemoCap {
					ftsMemo[entry.Body] = match
				}
			}
			if !match {
				return nil
			}
		}

		if !model.IsZeroTraceID(q.TraceID) && entry.TraceID != q.TraceID {
			return nil
		}

		if !cursor.IsZero() && !cursor.After(entry.Timestamp.UnixNano(), entry.ID) {
			return nil
		}

		matched++
		if hp.Len() < k {
			heap.Push(hp, entry)
		} else if logEntryNewer(entry, (*hp)[0]) {
			(*hp)[0] = entry
			heap.Fix(hp, 0)
		}
		return nil
	}

	var scanErr error
	if q.HasTimeRange() {
		scanErr = sr.ScanTimeRangeReverseWithBlockSkip(q.FromUnixNano(), q.ToUnixNano(), skip, scanFn)
	} else {
		scanErr = sr.ScanReverseWithBlockSkip(skip, scanFn)
	}
	if scanErr != nil {
		if errors.Is(scanErr, storage.ErrSegmentCorrupted) && ctx.Value(corruptRefetchContextKey{}) != segPath {
			if err := e.logReaders.refreshCorrupt(segPath); err == nil {
				*hp = append((*hp)[:0], heapBeforeScan...)
				heap.Init(hp)
				retryCtx := context.WithValue(ctx, corruptRefetchContextKey{}, segPath)
				return e.execLogSegment(retryCtx, q, plan, seg, cursor, hp, k, ftsTokens)
			}
		}
		return matched, fmt.Errorf("scan segment: %w", scanErr)
	}

	return matched, nil
}

// ExecSpan runs a span query, mirroring ExecLog: index-assisted, reverse-scan
// top-k, post-filter (service, operation, duration, status), cursor-paginated.
func (e *Executor) ExecSpan(ctx context.Context, q *SpanQuery) (r *SpanResult, err error) {
	start := time.Now()
	cacheHit := false
	defer func() {
		selfobs.QueryDuration.WithLabelValues("span").Observe(time.Since(start).Seconds())
		if err != nil {
			selfobs.QueryErrors.WithLabelValues("span").Inc()
			return
		}
		cache := "miss"
		if cacheHit {
			cache = "hit"
		}
		selfobs.QueryTotal.WithLabelValues("span", cache).Inc()
	}()

	if q == nil {
		return nil, errors.New("query: span query is nil")
	}
	if err := q.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	cacheKey := hashSpanQuery(q)
	var queryGeneration uint64

	for {
		if cached, ok := e.resultCache.getSpan(cacheKey); ok {
			cacheHit = true
			return cached, nil
		}
		wait, done, err := e.resultCache.waitOrStart(ctx, cacheKey)
		if err != nil {
			return nil, err
		}
		if wait {
			continue
		}
		defer done()
		queryGeneration = e.resultCache.spanGenerationSnapshot()
		break
	}

	spanPlanner := NewPlanner(e.spanSparse)

	lq := &LogQuery{From: q.From, To: q.To}
	plan := spanPlanner.Plan(lq)

	if len(plan.Segments) == 0 {
		return &SpanResult{}, nil
	}

	segs := make([]index.SegmentTimeRange, len(plan.Segments))
	copy(segs, plan.Segments)
	segs = filterQueryableSegments(segs, e.spanManager)
	sort.Slice(segs, func(i, j int) bool { return segs[i].MaxTS > segs[j].MaxTS })

	cursor, _ := DecodeCursor(q.Cursor) // pre-validated in q.Validate

	k := q.Limit
	if k <= 0 {
		k = 100
	}

	hp := &spanMinHeap{}
	heap.Init(hp)
	totalHits := 0
	for _, seg := range segs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !cursor.IsZero() && seg.MinTS > cursor.Timestamp {
			continue
		}
		if hp.Len() >= k {
			thresholdTS := (*hp)[0].StartTime.UnixNano()
			if seg.MaxTS < thresholdTS {
				continue
			}
		}
		matched, err := e.execSpanSegment(ctx, q, seg, cursor, hp, k)
		if err != nil {
			return nil, fmt.Errorf("executor: span segment %s: %w", seg.FileName, err)
		}
		totalHits += matched
	}

	spans := make([]model.SpanEntry, hp.Len())
	for i := len(spans) - 1; i >= 0; i-- {
		spans[i] = heap.Pop(hp).(model.SpanEntry)
	}

	truncated := len(spans) == q.Limit
	var nextCursor string
	if truncated {
		last := spans[len(spans)-1]
		nextCursor = EncodeCursor(Cursor{
			Timestamp: last.StartTime.UnixNano(),
			EntryID:   last.ID,
		})
	}

	result := &SpanResult{
		Spans:      spans,
		TotalHits:  totalHits,
		Truncated:  truncated,
		NextCursor: nextCursor,
	}
	e.resultCache.putSpan(cacheKey, result, q.FromUnixNano(), q.ToUnixNano(), queryGeneration)
	return result, nil
}

func (e *Executor) execSpanSegment(
	ctx context.Context,
	q *SpanQuery,
	seg index.SegmentTimeRange,
	cursor Cursor,
	hp *spanMinHeap,
	k int,
) (int, error) {

	var allowedSlice []uint64
	hasFilter := false
	restrict := func(ids []uint64) bool {
		if !hasFilter {
			allowedSlice = ids
			hasFilter = true
		} else {
			allowedSlice = index.IntersectSorted(allowedSlice, ids)
		}
		return len(allowedSlice) > 0
	}

	if !model.IsZeroTraceID(q.TraceID) {
		if ribbon, ok := e.spanRibbon(seg.FileName); ok {
			if !ribbon.Contains(q.TraceID[:]) {
				return 0, nil
			}
		}
		if pl, ok := e.spanPosting(seg.FileName); ok {
			ids := pl.Lookup(q.TraceID[:])
			slices.Sort(ids)
			if !restrict(ids) {
				return 0, nil
			}
		}
	}

	// Service/operation/status are bitmap-indexed in the span .bidx; intersect
	// the matching candidates so the scan decodes only those, not every span.
	// The scan still applies these filters exactly (plus time/duration/cursor),
	// so a missing or coarse bitmap only loses pruning, never correctness.
	// Service/operation/status are bitmap-indexed in the span .bidx, and duration
	// is indexed into log2 buckets; intersect the matching candidates so the scan
	// decodes only those. Constrain only by fields the bitmap actually holds - a
	// field missing from an older .bidx falls through to the scan's exact filter
	// (correctness), losing only the pruning - and the scan re-applies every
	// predicate exactly, so coarse buckets never leak a wrong result.
	if bm, ok := e.spanBitmap(seg.FileName); ok {
		conditions := buildSpanBitmapConditions(q)
		for field := range conditions {
			if !bm.HasField(field) {
				delete(conditions, field)
			}
		}
		if len(conditions) > 0 {
			if !restrict(bm.FilterMulti(conditions)) {
				return 0, nil
			}
		}
		if (q.MinDuration > 0 || q.MaxDuration > 0) && bm.HasField(index.DurationBucketField) {
			labels := index.DurationBucketLabels(q.MinDuration, q.MaxDuration)
			if !restrict(bm.FilterAny(index.DurationBucketField, labels)) {
				return 0, nil
			}
		}
	}

	segPath := e.spanManager.SegmentPath(storage.SegmentMeta{FileName: seg.FileName})

	var sr *storage.SegmentReader
	if hint, isActive := e.spanManager.ActiveBlockIndex(seg.FileName); isActive {
		var err error
		sr, err = storage.OpenSegmentReader(segPath, hint)
		if err != nil {
			return 0, fmt.Errorf("open span segment: %w", err)
		}
		defer func() { _ = sr.Close() }()
	} else {
		cr, err := e.spanReaders.acquire(segPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) && !e.spanManager.IsQueryableSegment(seg.FileName) {
				return 0, nil
			}
			return 0, fmt.Errorf("open span segment: %w", err)
		}
		defer e.spanReaders.release(cr)
		sr = cr.reader
	}

	heapBeforeScan := append(spanMinHeap(nil), (*hp)...)
	matched := 0

	scanFn := func(data []byte) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if hasFilter {
			if id, ok := peekEntryIDUint64(data); ok {
				if _, found := slices.BinarySearch(allowedSlice, id); !found {
					return nil
				}
			}
		}

		var span model.SpanEntry
		if err := span.DecodeBytes(data); err != nil {
			return fmt.Errorf("decode span record: %w", err)
		}

		if !model.IsZeroTraceID(q.TraceID) && span.TraceID != q.TraceID {
			return nil
		}
		if !q.From.IsZero() && span.StartTime.Before(q.From) {
			return nil
		}
		if !q.To.IsZero() && span.StartTime.After(q.To) {
			return nil
		}
		if len(q.Services) > 0 && !containsStr(q.Services, span.Service) {
			return nil
		}
		if len(q.Operations) > 0 && !containsStr(q.Operations, span.Operation) {
			return nil
		}
		if len(q.Statuses) > 0 && !containsStatus(q.Statuses, span.Status) {
			return nil
		}
		if q.MinDuration > 0 && span.Duration() < q.MinDuration {
			return nil
		}
		if q.MaxDuration > 0 && span.Duration() > q.MaxDuration {
			return nil
		}

		if !cursor.IsZero() && !cursor.After(span.StartTime.UnixNano(), span.ID) {
			return nil
		}

		matched++
		if hp.Len() < k {
			heap.Push(hp, span)
		} else if spanEntryNewer(span, (*hp)[0]) {
			(*hp)[0] = span
			heap.Fix(hp, 0)
		}
		return nil
	}

	hasTimeRange := !q.From.IsZero() || !q.To.IsZero()
	from, to := int64(0), int64(^uint64(0)>>1)
	if hasTimeRange {
		if !q.From.IsZero() {
			from = q.From.UnixNano()
		}
		if !q.To.IsZero() {
			to = q.To.UnixNano()
		}
	}

	skip := func(minID, maxID uint64) bool {
		if ctx.Err() != nil {
			return false
		}
		// Same intra-segment threshold skip as the log path: a block whose
		// newest record predates the oldest kept span cannot improve the
		// heap (truncated ms-timestamp space, wrap-guarded).
		if hp.Len() >= k {
			thrLow := uint32((*hp)[0].StartTime.UnixMilli())
			blockLow := uint32(maxID >> 32)
			if delta := thrLow - blockLow; delta > 0 && delta < 1<<31 {
				return true
			}
		}
		if hasFilter {
			i := sort.Search(len(allowedSlice), func(i int) bool {
				return allowedSlice[i] >= minID
			})
			if i == len(allowedSlice) || allowedSlice[i] > maxID {
				return true
			}
		}
		return false
	}

	var scanErr error
	if hasTimeRange {
		scanErr = sr.ScanTimeRangeReverseWithBlockSkip(from, to, skip, scanFn)
	} else {
		scanErr = sr.ScanReverseWithBlockSkip(skip, scanFn)
	}
	if scanErr != nil {
		if errors.Is(scanErr, storage.ErrSegmentCorrupted) && ctx.Value(corruptRefetchContextKey{}) != segPath {
			if err := e.spanReaders.refreshCorrupt(segPath); err == nil {
				*hp = append((*hp)[:0], heapBeforeScan...)
				heap.Init(hp)
				retryCtx := context.WithValue(ctx, corruptRefetchContextKey{}, segPath)
				return e.execSpanSegment(retryCtx, q, seg, cursor, hp, k)
			}
		}
		return matched, fmt.Errorf("scan span segment: %w", scanErr)
	}

	return matched, nil
}

func buildBitmapConditions(q *LogQuery) map[string][]string {
	conditions := make(map[string][]string)
	if len(q.Services) > 0 {
		conditions["service"] = q.Services
	}
	if len(q.Hosts) > 0 {
		conditions["host"] = q.Hosts
	}
	if len(q.Levels) > 0 {
		conditions["level"] = q.Levels
	}
	return conditions
}

// buildSpanBitmapConditions maps a span query's exact-match fields to the span
// .bidx fields built by BuildSpanSealIndexes (service/operation/status), so the
// scan decodes only candidate spans instead of every span in the segment.
func buildSpanBitmapConditions(q *SpanQuery) map[string][]string {
	conditions := make(map[string][]string)
	if len(q.Services) > 0 {
		conditions["service"] = q.Services
	}
	if len(q.Operations) > 0 {
		conditions["operation"] = q.Operations
	}
	if len(q.Statuses) > 0 {
		statuses := make([]string, len(q.Statuses))
		for i, s := range q.Statuses {
			statuses[i] = s.String()
		}
		conditions["status"] = statuses
	}
	return conditions
}

func matchesTimeRange(entry model.LogEntry, q *LogQuery) bool {
	if !q.From.IsZero() && entry.Timestamp.Before(q.From) {
		return false
	}
	if !q.To.IsZero() && entry.Timestamp.After(q.To) {
		return false
	}
	return true
}

func matchesAttrs(entry model.LogEntry, q *LogQuery) bool {
	for k, v := range q.Attrs {
		found := false
		for _, attr := range entry.Attrs {
			if attr.Key == k && attr.Value == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

// ftsMemoCap bounds the per-scan body-match memo.
const ftsMemoCap = 8192

// bodyMatchesTokens reports whether body contains every query token.
func bodyMatchesTokens(body string, queryToks []string) bool {
	bodyToks := index.TokenizeFTS(body)
	for _, q := range queryToks {
		found := false
		for _, bt := range bodyToks {
			if bt == q {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func containsStatus(slice []model.SpanStatus, s model.SpanStatus) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}
