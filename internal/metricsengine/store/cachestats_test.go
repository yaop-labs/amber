package store

import (
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func TestSplitCacheBudget(t *testing.T) {
	const total = defaultDirCacheBudget + defaultResidentCacheBudget
	tests := []struct {
		name   string
		budget int64
	}{
		{name: "zero keeps defaults", budget: 0},
		{name: "negative keeps defaults", budget: -1},
		{name: "historical total", budget: total},
		{name: "half limit of 800MiB", budget: 400 << 20},
		{name: "tiny", budget: 1 << 20},
		// Regression: the naive budget*defaultDirCacheBudget wrapped int64
		// negative above ~25.6 GiB (memory_limit/2 on a 64+ GiB host).
		{name: "64GiB half (overflow regression)", budget: 32 << 30},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir, resident := splitCacheBudget(tt.budget)
			if tt.budget <= 0 {
				if dir != 0 || resident != 0 {
					t.Fatalf("want (0,0) for budget %d, got (%d,%d)", tt.budget, dir, resident)
				}
				return
			}
			if dir <= 0 || resident <= 0 {
				t.Fatalf("both shares must be positive, got dir=%d resident=%d", dir, resident)
			}
			if dir+resident != tt.budget {
				t.Fatalf("split loses bytes: %d+%d != %d", dir, resident, tt.budget)
			}
			// The split must preserve the historical 320:384 proportion
			// (computed overflow-free, matching the implementation).
			wantDir := tt.budget/total*defaultDirCacheBudget + tt.budget%total*defaultDirCacheBudget/total
			if dir != wantDir {
				t.Fatalf("dir share %d, want %d", dir, wantDir)
			}
			if dir >= resident {
				t.Fatalf("resident must get the larger share: dir=%d resident=%d", dir, resident)
			}
		})
	}
}

func TestCacheBudgetOption(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{CacheBudget: 100 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cs := st.CacheStats()
	if cs.DirBudget+cs.ResidentBudget != 100<<20 {
		t.Fatalf("budgets %d+%d, want total %d", cs.DirBudget, cs.ResidentBudget, 100<<20)
	}
	if cs.DirBudget == defaultDirCacheBudget || cs.ResidentBudget == defaultResidentCacheBudget {
		t.Fatal("configured budget must override the defaults")
	}
}

func TestCacheBudgetDefault(t *testing.T) {
	st, err := OpenWithOptions(t.TempDir(), Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	cs := st.CacheStats()
	if cs.DirBudget != defaultDirCacheBudget || cs.ResidentBudget != defaultResidentCacheBudget {
		t.Fatalf("zero CacheBudget must keep defaults, got dir=%d resident=%d", cs.DirBudget, cs.ResidentBudget)
	}
}

func TestDirCacheCounters(t *testing.T) {
	makeDir := func(series int) block.Directory {
		d := block.Directory{Series: make([]block.DirectoryEntry, series)}
		for i := range d.Series {
			d.Series[i] = block.DirectoryEntry{
				SeriesID: uint64(i + 1),
				Labels:   model.LabelSet{{Name: "__name__", Value: "m"}},
			}
		}
		return d
	}
	// Budget fits one 1000-series directory (~224 KB retained) but not two.
	c := newDirCache(300 << 10)

	if _, ok := c.get("a"); ok {
		t.Fatal("unexpected hit on empty cache")
	}
	c.put("a", makeDir(1000))
	if _, ok := c.get("a"); !ok {
		t.Fatal("expected hit after put")
	}
	c.put("b", makeDir(1000)) // must evict "a"
	if _, ok := c.get("a"); ok {
		t.Fatal("expected `a` to be evicted")
	}

	if got := c.hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1", got)
	}
	if got := c.misses.Load(); got != 2 {
		t.Fatalf("misses = %d, want 2", got)
	}
	if got := c.evictions.Load(); got != 1 {
		t.Fatalf("evictions = %d, want 1", got)
	}
}
