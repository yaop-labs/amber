// Package query plans and executes log and span queries against sealed and
// active segments, using bitmap, FTS, and ribbon-filter indexes.
package query

import (
	"bytes"
	"container/heap"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring/v2/roaring64"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/indexer"
	"github.com/yaop-labs/amber/internal/metrics"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

type logMinHeap []model.LogEntry

func (h logMinHeap) Len() int { return len(h) }
func (h logMinHeap) Less(i, j int) bool {
	return h[i].Timestamp.Before(h[j].Timestamp)
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
	return h[i].StartTime.Before(h[j].StartTime)
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

// func blockSkipPredicate(allowed *roaring64.Bitmap) func(minID, maxID uint64) bool {
// 	return func(minID, maxID uint64) bool {
// 		var count uint64
// 		if minID == 0 {
// 			count = allowed.Rank(maxID)
// 		} else {
// 			count = allowed.Rank(maxID) - allowed.Rank(minID-1)
// 		}
// 		return count == 0
// 	}
// }

type Executor struct {
	logManager  *storage.SegmentManager
	spanManager *storage.SegmentManager
	logSparse   *index.SparseIndex
	spanSparse  *index.SparseIndex
	planner     *Planner
	logDir      string
	spanDir     string

	active *indexer.ActiveIndex

	sealedMu      sync.RWMutex
	logRibbons    map[string]*index.RibbonFilter
	logFTSRibbons map[string]*index.RibbonFilter
	spanRibbons   map[string]*index.RibbonFilter

	logBitmapCache   *indexLRU[*index.MultiFieldIndex]
	spanBitmapCache  *indexLRU[*index.MultiFieldIndex]
	ftsCache         *indexLRU[*index.FTSIndex]
	logPostingCache  *indexLRU[*index.PostingList]
	spanPostingCache *indexLRU[*index.PostingList]

	logReaders  *readerCache
	spanReaders *readerCache

	resultCache *queryCache
}

type queryCacheEntry struct {
	logs    *LogResult
	spans   *SpanResult
	expires int64
}

type queryCache struct {
	mu       sync.Mutex
	entries  map[[32]byte]queryCacheEntry
	inflight map[[32]byte]chan struct{}
	ttl      time.Duration
	maxSize  int
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

func (c *queryCache) waitOrStart(key [32]byte) (wait bool, done func()) {
	if c == nil {
		return false, func() {}
	}
	c.mu.Lock()
	if ch, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		<-ch
		return true, nil
	}
	ch := make(chan struct{})
	c.inflight[key] = ch
	c.mu.Unlock()
	return false, func() {
		c.mu.Lock()
		delete(c.inflight, key)
		c.mu.Unlock()
		close(ch)
	}
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
	return e.logs, true
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
	return e.spans, true
}

func (c *queryCache) putLog(key [32]byte, r *LogResult) {
	if c == nil || r == nil {
		return
	}
	// Skip empty results: caching "0 entries" pins a stale-empty answer
	// for the whole TTL, which is visibly wrong for embedders that ingest
	// then immediately query (a typical forager turn-around). Empty results
	// are also cheap to recompute — the planner short-circuits when there
	// is nothing to scan.
	if len(r.Entries) == 0 {
		return
	}
	c.mu.Lock()
	if len(c.entries) >= c.maxSize {
		c.sweepLocked()
	}
	c.entries[key] = queryCacheEntry{
		logs:    r,
		expires: time.Now().Add(c.ttl).UnixNano(),
	}
	c.mu.Unlock()
}

func (c *queryCache) putSpan(key [32]byte, r *SpanResult) {
	if c == nil || r == nil {
		return
	}
	if len(r.Spans) == 0 {
		return
	}
	c.mu.Lock()
	if len(c.entries) >= c.maxSize {
		c.sweepLocked()
	}
	c.entries[key] = queryCacheEntry{
		spans:   r,
		expires: time.Now().Add(c.ttl).UnixNano(),
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
	c.entries = make(map[[32]byte]queryCacheEntry, c.maxSize)
	c.mu.Unlock()
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

func NewExecutor(
	logManager *storage.SegmentManager,
	spanManager *storage.SegmentManager,
	logSparse *index.SparseIndex,
	spanSparse *index.SparseIndex,
) *Executor {
	return NewExecutorWithCache(logManager, spanManager, logSparse, spanSparse, "", "", defaultIndexCacheSize)
}

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
		logManager:       logManager,
		spanManager:      spanManager,
		logSparse:        logSparse,
		spanSparse:       spanSparse,
		planner:          NewPlanner(logSparse),
		logDir:           logDir,
		spanDir:          spanDir,
		active:           indexer.New(logManager, spanManager),
		logRibbons:       make(map[string]*index.RibbonFilter),
		logFTSRibbons:    make(map[string]*index.RibbonFilter),
		spanRibbons:      make(map[string]*index.RibbonFilter),
		logBitmapCache:   newIndexLRU[*index.MultiFieldIndex](cacheSize),
		spanBitmapCache:  newIndexLRU[*index.MultiFieldIndex](cacheSize),
		ftsCache:         newIndexLRU[*index.FTSIndex](cacheSize),
		logPostingCache:  newIndexLRU[*index.PostingList](cacheSize),
		spanPostingCache: newIndexLRU[*index.PostingList](cacheSize),
		logReaders:       newReaderCache(cacheSize),
		spanReaders:      newReaderCache(cacheSize),
		resultCache:      newQueryCache(defaultResultCacheSize, defaultResultCacheTTL),
	}
}

// ActiveIndex exposes the writer-side bitmap so wire-up code (runtime.New)
// can hand it to Batcher as the ActiveIndexer. Read-side query paths use it
// internally via LookupLog/LookupSpan.
func (e *Executor) ActiveIndex() *indexer.ActiveIndex { return e.active }

// SetSegmentStores wires remote-fetch fallbacks on the reader caches. After
// this call, a query whose target segment is missing locally will fetch it
// from the matching store before opening. Wire-up code in runtime.New is
// the only expected caller; pass nil to leave fallbacks disabled.
func (e *Executor) SetSegmentStores(logStore, spanStore storage.SegmentStore) {
	if logStore != nil && e.logReaders != nil {
		e.logReaders.setFetcher(makeStoreFetcher(logStore, e.logDir))
	}
	if spanStore != nil && e.spanReaders != nil {
		e.spanReaders.setFetcher(makeStoreFetcher(spanStore, e.spanDir))
	}
}

func (e *Executor) InvalidateLogSegment(seg storage.SegmentMeta) {
	if e.logReaders != nil {
		e.logReaders.invalidate(e.logManager.SegmentPath(seg))
	}
	e.logBitmapCache.delete(seg.FileName)
	e.ftsCache.delete(seg.FileName)
	e.sealedMu.Lock()
	delete(e.logRibbons, seg.FileName)
	delete(e.logFTSRibbons, seg.FileName)
	e.sealedMu.Unlock()
	e.resultCache.clear()
}

// ClearResultCache drops every cached LogResult/SpanResult. Used by
// benchmarks and loadbench that need to measure cold execution, and by
// future admin endpoints that flush cache after schema-affecting changes.
// Same underlying call as the side-effect in InvalidateLogSegment.
func (e *Executor) ClearResultCache() {
	e.resultCache.clear()
}

func (e *Executor) InvalidateSpanSegment(seg storage.SegmentMeta) {
	if e.spanReaders != nil {
		e.spanReaders.invalidate(e.spanManager.SegmentPath(seg))
	}
	e.spanBitmapCache.delete(seg.FileName)
	e.sealedMu.Lock()
	delete(e.spanRibbons, seg.FileName)
	e.sealedMu.Unlock()
	e.resultCache.clear()
}

func (e *Executor) Close() {
	if e.logReaders != nil {
		e.logReaders.close()
	}
	if e.spanReaders != nil {
		e.spanReaders.close()
	}
}

func (e *Executor) RegisterBitmapIndex(segmentFile string, idx *index.MultiFieldIndex) {
	e.logBitmapCache.put(segmentFile, idx)
}

func (e *Executor) RegisterSpanBitmapIndex(segmentFile string, idx *index.MultiFieldIndex) {
	e.spanBitmapCache.put(segmentFile, idx)
}

func (e *Executor) RegisterFTSIndex(segmentFile string, idx *index.FTSIndex) {
	e.ftsCache.put(segmentFile, idx)
}

func (e *Executor) RegisterLogRibbon(segmentFile string, f *index.RibbonFilter) {
	e.sealedMu.Lock()
	e.logRibbons[segmentFile] = f
	e.sealedMu.Unlock()
}

func (e *Executor) RegisterLogFTSRibbon(segmentFile string, f *index.RibbonFilter) {
	e.sealedMu.Lock()
	e.logFTSRibbons[segmentFile] = f
	e.sealedMu.Unlock()
}

func (e *Executor) RegisterSpanRibbon(segmentFile string, f *index.RibbonFilter) {
	e.sealedMu.Lock()
	e.spanRibbons[segmentFile] = f
	e.sealedMu.Unlock()
}

func (e *Executor) RegisterLogPostingList(segmentFile string, pl *index.PostingList) {
	e.logPostingCache.put(segmentFile, pl)
}

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

func (e *Executor) spanBitmap(name string) (*index.MultiFieldIndex, bool) {
	if idx, ok := e.active.LookupSpan(name); ok {
		return idx, true
	}
	if idx, ok := e.spanBitmapCache.get(name); ok {
		return idx, true
	}
	if e.spanDir == "" {
		return nil, false
	}
	idx, err := index.LoadMultiFieldIndex(filepath.Join(e.spanDir, name+".bidx"))
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
	e.sealedMu.RLock()
	f, ok := e.logRibbons[name]
	e.sealedMu.RUnlock()
	return f, ok
}

func (e *Executor) logFTSRibbon(name string) (*index.RibbonFilter, bool) {
	e.sealedMu.RLock()
	f, ok := e.logFTSRibbons[name]
	e.sealedMu.RUnlock()
	return f, ok
}

func (e *Executor) spanRibbon(name string) (*index.RibbonFilter, bool) {
	e.sealedMu.RLock()
	f, ok := e.spanRibbons[name]
	e.sealedMu.RUnlock()
	return f, ok
}

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
	for _, mgr := range []*storage.SegmentManager{e.logManager, e.spanManager} {
		if mgr == nil {
			continue
		}
		activeMeta, ok := mgr.ActiveSegmentMeta()
		if !ok {
			continue
		}
		if _, hasBitmap := e.logBitmap(activeMeta.FileName); hasBitmap {
			continue
		}
		if _, hasBitmap := e.spanBitmap(activeMeta.FileName); hasBitmap {
			continue
		}

		segPath := mgr.SegmentPath(activeMeta)
		hint, _ := mgr.ActiveBlockIndex(activeMeta.FileName)
		sr, err := storage.OpenSegmentReader(segPath, hint)
		if err != nil {
			continue
		}
		_ = sr.Scan(func(data []byte) error {
			var logEntry model.LogEntry
			if _, err := logEntry.ReadFrom(bytes.NewReader(data)); err == nil {
				if logEntry.Service != "" {
					seen[logEntry.Service] = struct{}{}
				}
				return nil
			}
			var spanEntry model.SpanEntry
			if _, err := spanEntry.ReadFrom(bytes.NewReader(data)); err == nil {
				if spanEntry.Service != "" {
					seen[spanEntry.Service] = struct{}{}
				}
			}
			return nil
		})
		_ = sr.Close()
	}
}

func (e *Executor) ExecLog(ctx context.Context, q *LogQuery) (r *LogResult, err error) {
	start := time.Now()
	defer func() {
		metrics.QueryDuration.WithLabelValues("log").Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.QueryErrors.WithLabelValues("log").Inc()
			return
		}
		cache := "miss"
		if r != nil && r.CacheHit {
			cache = "hit"
		}
		metrics.QueryTotal.WithLabelValues("log", cache).Inc()
	}()

	if err := q.Validate(); err != nil {
		return nil, err
	}

	cacheKey := hashLogQuery(q)

	for {
		if cached, ok := e.resultCache.getLog(cacheKey); ok {
			cp := *cached
			cp.CacheHit = true
			return &cp, nil
		}
		wait, done := e.resultCache.waitOrStart(cacheKey)
		if wait {
			continue
		}
		defer done()
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
		// Cursor segment skip: every record in this segment is strictly newer
		// than the cursor TS, so the previous page already included them.
		// Saves a full segment scan per page for the common newest-first walk.
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

	// entries are newest-first. NextCursor is set whenever we filled the heap
	// to capacity — there MAY be more older records to page through. The
	// alternative (totalHits > Limit) is wrong: totalHits only counts records
	// that survived the thresholdID short-circuit, which kicks in after the
	// heap fills, so it always undercounts.
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
	e.resultCache.putLog(cacheKey, result)
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

	var allowedIDs *roaring64.Bitmap

	var allowedSlice []uint64

	if plan.HasStep(StepBitmapFilter) {
		if bm, ok := e.logBitmap(seg.FileName); ok {
			conditions := buildBitmapConditions(q)
			if len(conditions) > 0 {
				allowedIDs, allowedSlice = bm.FilterWithSorted(conditions)
				if allowedIDs.IsEmpty() {
					return 0, nil
				}
			}
		}
	}

	// needScanFTS triggers a per-record body match during the scan. It is set
	// when a full-text query targets a segment that has no FTS index yet —
	// notably the active, unsealed segment, whose index is only built at seal
	// time. Without this fallback, `q` would be silently ignored for the most
	// recent data.
	needScanFTS := false
	if plan.HasStep(StepFTSSearch) {

		if len(ftsTokens) > 0 {
			if ribbon, ok := e.logFTSRibbon(seg.FileName); ok {
				anyHit := false
				for _, tok := range ftsTokens {
					if ribbon.Contains(tok) {
						anyHit = true
						break
					}
				}
				if !anyHit {
					return 0, nil
				}
			}
		}
		if fts, ok := e.fts(seg.FileName); ok {
			ftsIDs, err := fts.Search(ctx, q.FullText, 100_000)
			if err != nil {
				return 0, fmt.Errorf("fts search: %w", err)
			}

			ftsBitmap := roaring64.New()
			ftsBitmap.AddMany(ftsIDs)

			if allowedIDs == nil {
				allowedIDs = ftsBitmap
			} else {

				allowedIDs = roaring64.And(allowedIDs, ftsBitmap)
				allowedSlice = nil
			}

			if allowedIDs.IsEmpty() {
				return 0, nil
			}
		} else if len(ftsTokens) > 0 {
			needScanFTS = true
		}
	}

	if !model.IsZeroTraceID(q.TraceID) {
		// Segment pruning: ribbon filter tells us fast if this segment
		// definitely has no records with this trace_id.
		if ribbon, ok := e.logRibbon(seg.FileName); ok {
			if !ribbon.Contains(q.TraceID[:]) {
				return 0, nil
			}
		}
		// Exact lookup: posting list gives us the record IDs for this
		// trace_id, enabling intersection with other bitmap constraints.
		if pl, ok := e.logPosting(seg.FileName); ok {
			ids := pl.Lookup(q.TraceID[:])
			if len(ids) == 0 {
				return 0, nil
			}
			traceBitmap := roaring64.New()
			for _, id := range ids {
				traceBitmap.Add(id)
			}
			if allowedIDs == nil {
				allowedIDs = traceBitmap
			} else {
				allowedIDs = roaring64.And(allowedIDs, traceBitmap)
				allowedSlice = nil
				if allowedIDs.IsEmpty() {
					return 0, nil
				}
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
			return 0, fmt.Errorf("open segment: %w", err)
		}
		defer e.logReaders.release(cr)
		sr = cr.reader
	}

	matched := 0

	// Scan-time full-text setup. The query tokens are converted to strings
	// once per segment (not per record), and a per-scan memo collapses the
	// repeated work of tokenizing identical bodies — log messages are highly
	// templated, so the same body recurs thousands of times. The memo is
	// capped so a pathologically high-cardinality segment can't blow memory;
	// past the cap we simply recompute.
	var ftsTokenStrs []string
	var ftsMemo map[string]bool
	if needScanFTS {
		ftsTokenStrs = make([]string, len(ftsTokens))
		for i, t := range ftsTokens {
			ftsTokenStrs[i] = string(t)
		}
		ftsMemo = make(map[string]bool)
	}

	thresholdID := func() (uint64, bool) {
		if hp.Len() < k {
			return 0, false
		}
		return model.EntryIDToUint64((*hp)[0].ID), true
	}

	if allowedIDs != nil && allowedSlice == nil {
		allowedSlice = allowedIDs.ToArray()
	}
	skip := func(minID, maxID uint64) bool {
		if allowedSlice != nil {
			i := sort.Search(len(allowedSlice), func(i int) bool {
				return allowedSlice[i] >= minID
			})
			if i == len(allowedSlice) || allowedSlice[i] > maxID {
				return true
			}
		}
		if thresh, ok := thresholdID(); ok && maxID < thresh {
			return true
		}
		return false
	}

	scanFn := func(data []byte) error {
		id, idOK := peekEntryIDUint64(data)
		if allowedIDs != nil && idOK && !allowedIDs.Contains(id) {
			return nil
		}

		if thresh, ok := thresholdID(); ok && idOK && id < thresh {
			return nil
		}

		var entry model.LogEntry
		if err := entry.DecodeBytes(data); err != nil {
			return nil
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

		// Cursor pagination: skip records that are NOT strictly older than the
		// cursor (we paginate newest-first). The cursor itself is excluded.
		if !cursor.IsZero() && !cursor.After(entry.Timestamp.UnixNano(), entry.ID) {
			return nil
		}

		matched++
		if hp.Len() < k {
			heap.Push(hp, entry)
		} else if entry.Timestamp.After((*hp)[0].Timestamp) {
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
		return matched, fmt.Errorf("scan segment: %w", scanErr)
	}

	return matched, nil
}

func (e *Executor) ExecSpan(ctx context.Context, q *SpanQuery) (r *SpanResult, err error) {
	start := time.Now()
	cacheHit := false
	defer func() {
		metrics.QueryDuration.WithLabelValues("span").Observe(time.Since(start).Seconds())
		if err != nil {
			metrics.QueryErrors.WithLabelValues("span").Inc()
			return
		}
		cache := "miss"
		if cacheHit {
			cache = "hit"
		}
		metrics.QueryTotal.WithLabelValues("span", cache).Inc()
	}()

	if err := q.Validate(); err != nil {
		return nil, err
	}

	cacheKey := hashSpanQuery(q)

	for {
		if cached, ok := e.resultCache.getSpan(cacheKey); ok {
			cacheHit = true
			return cached, nil
		}
		wait, done := e.resultCache.waitOrStart(cacheKey)
		if wait {
			continue
		}
		defer done()
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
	e.resultCache.putSpan(cacheKey, result)
	return result, nil
}

func (e *Executor) execSpanSegment(
	_ context.Context,
	q *SpanQuery,
	seg index.SegmentTimeRange,
	cursor Cursor,
	hp *spanMinHeap,
	k int,
) (int, error) {

	var allowedIDs *roaring64.Bitmap

	if !model.IsZeroTraceID(q.TraceID) {
		if ribbon, ok := e.spanRibbon(seg.FileName); ok {
			if !ribbon.Contains(q.TraceID[:]) {
				return 0, nil
			}
		}
		if pl, ok := e.spanPosting(seg.FileName); ok {
			ids := pl.Lookup(q.TraceID[:])
			if len(ids) == 0 {
				return 0, nil
			}
			allowedIDs = roaring64.New()
			for _, id := range ids {
				allowedIDs.Add(id)
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
			return 0, fmt.Errorf("open span segment: %w", err)
		}
		defer e.spanReaders.release(cr)
		sr = cr.reader
	}

	matched := 0

	thresholdID := func() (uint64, bool) {
		if hp.Len() < k {
			return 0, false
		}
		return model.EntryIDToUint64((*hp)[0].ID), true
	}

	scanFn := func(data []byte) error {
		if allowedIDs != nil {
			if id, ok := peekEntryIDUint64(data); ok && !allowedIDs.Contains(id) {
				return nil
			}
		}
		if thresh, ok := thresholdID(); ok {
			if id, idOK := peekEntryIDUint64(data); idOK && id < thresh {
				return nil
			}
		}

		var span model.SpanEntry
		if err := span.DecodeBytes(data); err != nil {
			return nil
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
		} else if span.StartTime.After((*hp)[0].StartTime) {
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

	var allowedSlice []uint64
	if allowedIDs != nil {
		allowedSlice = allowedIDs.ToArray()
	}
	skip := func(minID, maxID uint64) bool {
		if allowedSlice != nil {
			i := sort.Search(len(allowedSlice), func(i int) bool {
				return allowedSlice[i] >= minID
			})
			if i == len(allowedSlice) || allowedSlice[i] > maxID {
				return true
			}
		}
		if thresh, ok := thresholdID(); ok && maxID < thresh {
			return true
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
		return matched, fmt.Errorf("scan span segment: %w", scanErr)
	}

	return matched, nil
}

func buildBitmapConditions(q *LogQuery) map[string]string {
	conditions := make(map[string]string)
	if len(q.Services) == 1 {
		conditions["service"] = q.Services[0]
	}
	if len(q.Hosts) == 1 {
		conditions["host"] = q.Hosts[0]
	}
	if len(q.Levels) == 1 {
		conditions["level"] = q.Levels[0]
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

// ftsMemoCap bounds the per-scan body-match memo so a high-cardinality segment
// (mostly unique bodies) cannot grow it without limit; past the cap we recompute.
const ftsMemoCap = 8192

// bodyMatchesTokens reports whether body contains every query token, using the
// same FTS tokenizer (lowercasing, stemming, stopword removal) as the sealed
// index path so the active-segment fallback yields identical matches. queryToks
// is the already-tokenized query. A linear scan over the (few) body tokens beats
// building a set per call: query terms number one or two in practice.
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
