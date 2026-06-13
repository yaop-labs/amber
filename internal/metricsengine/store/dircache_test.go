package store

import (
	"fmt"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func dirWithSeries(n int) block.Directory {
	d := block.Directory{Series: make([]block.DirectoryEntry, n)}
	for i := range d.Series {
		d.Series[i] = block.DirectoryEntry{
			SeriesID: uint64(i),
			Labels: model.LabelSet{
				{Name: "__name__", Value: "m"},
				{Name: "id", Value: fmt.Sprintf("%d", i)},
			},
		}
	}
	return d
}

func TestDirCacheEvictsToBudget(t *testing.T) {
	// Each directory of 1000 series costs 1000*176 + 2000*48 = 272000 bytes.
	one := directoryRetainedBytes(dirWithSeries(1000))
	c := newDirCache(one * 3) // budget holds at most three

	for i := 0; i < 6; i++ {
		c.put(fmt.Sprintf("block-%d", i), dirWithSeries(1000))
	}
	if c.bytes > c.budget {
		t.Fatalf("bytes %d exceeds budget %d", c.bytes, c.budget)
	}
	if len(c.entries) != 3 {
		t.Fatalf("len(entries) = %d, want 3", len(c.entries))
	}
}

func TestDirCacheGetPutHit(t *testing.T) {
	c := newDirCache(0)
	d := dirWithSeries(10)
	c.put("b", d)
	got, ok := c.get("b")
	if !ok {
		t.Fatal("expected cache hit")
	}
	if len(got.Series) != 10 {
		t.Fatalf("got %d series", len(got.Series))
	}
	if _, ok := c.get("missing"); ok {
		t.Fatal("unexpected hit for missing path")
	}
}

func TestDirCachePutIdempotent(t *testing.T) {
	c := newDirCache(0)
	d := dirWithSeries(100)
	c.put("b", d)
	before := c.bytes
	c.put("b", d) // second put must not double-count
	if c.bytes != before {
		t.Fatalf("bytes changed on repeat put: %d -> %d", before, c.bytes)
	}
	if len(c.entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(c.entries))
	}
}

// TestDirCacheOversizedDirectoryStillCached ensures a single directory larger
// than the whole budget is retained alone, so a query can still make progress.
func TestDirCacheOversizedDirectoryStillCached(t *testing.T) {
	c := newDirCache(1) // 1-byte budget: everything is "oversized"
	c.put("b", dirWithSeries(100))
	if _, ok := c.get("b"); !ok {
		t.Fatal("oversized directory should still be cached")
	}
	c.put("c", dirWithSeries(100)) // evicts b, keeps c
	if len(c.entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(c.entries))
	}
	if _, ok := c.get("c"); !ok {
		t.Fatal("most recent oversized directory should be cached")
	}
}

func TestDirCacheReset(t *testing.T) {
	c := newDirCache(0)
	c.put("a", dirWithSeries(10))
	c.put("b", dirWithSeries(10))
	c.reset()
	if c.bytes != 0 || len(c.entries) != 0 {
		t.Fatalf("reset left bytes=%d entries=%d", c.bytes, len(c.entries))
	}
}
