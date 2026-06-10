package selfobs

import (
	"runtime"
	"sync/atomic"
	"time"
)

// Go runtime gauges exposed through the /metrics handler.
// MemStats is cached briefly so one scrape does not call ReadMemStats for every
// runtime gauge.

var (
	cachedMemStats   atomic.Pointer[runtime.MemStats]
	cachedMemStatsAt atomic.Int64 // unix nano
)

// memStatsTTL bounds how stale cached MemStats can be.
const memStatsTTL = 250 * time.Millisecond

func readMemStats() *runtime.MemStats {
	now := time.Now().UnixNano()
	if cached := cachedMemStats.Load(); cached != nil {
		at := cachedMemStatsAt.Load()
		if time.Duration(now-at) < memStatsTTL {
			return cached
		}
	}
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	cachedMemStats.Store(&ms)
	cachedMemStatsAt.Store(now)
	return &ms
}

func init() {
	RegisterGaugeFunc("amber_go_goroutines",
		"Current number of goroutines.",
		func() float64 { return float64(runtime.NumGoroutine()) })

	RegisterGaugeFunc("amber_go_heap_alloc_bytes",
		"Bytes of allocated heap objects (live heap).",
		func() float64 { return float64(readMemStats().HeapAlloc) })

	RegisterGaugeFunc("amber_go_heap_inuse_bytes",
		"Bytes in in-use spans.",
		func() float64 { return float64(readMemStats().HeapInuse) })

	RegisterGaugeFunc("amber_go_heap_sys_bytes",
		"Bytes of heap memory obtained from the OS.",
		func() float64 { return float64(readMemStats().HeapSys) })

	RegisterCounterFunc("amber_go_gc_runs_total",
		"Total number of completed GC cycles.",
		func() float64 { return float64(readMemStats().NumGC) })

	RegisterCounterFunc("amber_go_gc_pause_total_seconds",
		"Cumulative GC stop-the-world pause time.",
		func() float64 { return float64(readMemStats().PauseTotalNs) / 1e9 })

	RegisterGaugeFunc("amber_go_gc_pause_last_seconds",
		"Most recent GC stop-the-world pause duration.",
		func() float64 {
			ms := readMemStats()
			if ms.NumGC == 0 {
				return 0
			}
			// PauseNs is a 256-entry ring buffer indexed by (NumGC+255)%256.
			idx := (ms.NumGC + 255) % 256
			return float64(ms.PauseNs[idx]) / 1e9
		})

	RegisterCounterFunc("amber_go_mallocs_total",
		"Cumulative count of heap object allocations.",
		func() float64 { return float64(readMemStats().Mallocs) })

	RegisterCounterFunc("amber_go_frees_total",
		"Cumulative count of heap object frees.",
		func() float64 { return float64(readMemStats().Frees) })
}
