//go:build linux

package store

import (
	"bufio"
	"context"
	"errors"
	"os"
	"runtime"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestMixedWorkloadRSS runs the campaign fixture under a concurrent query mix
// that exercises both block caches at once: range-step rate, instant rate on
// the resident index, and sum-by-label on the directory cache. It reports
// peak/avg RSS, GC CPU share, per-op throughput and cache stats per phase —
// the memory behavior a single-query benchmark cannot see (`make bench-mixed`).
//
// Gated behind AMBER_MIXED=1 because it runs for minutes and measures RSS,
// which is meaningless under `go test ./...` parallelism.
//
//	AMBER_MIXED=1 go test ./internal/metricsengine/store/ -run TestMixedWorkloadRSS -v
//	AMBER_MIXED_SECS   per-phase workload duration (default 60)
//
// Phase "defaults" reopens the store with the compiled-in cache budgets and no
// memory limit. Phase "800MiB-limit" mirrors a production config with
// runtime.memory_limit=800MiB: the soft limit plus the derived CacheBudget of
// limit/2, matching what internal/runtime wires from that config.
func TestMixedWorkloadRSS(t *testing.T) {
	if os.Getenv("AMBER_MIXED") == "" {
		t.Skip("set AMBER_MIXED=1 to run the mixed-workload RSS measurement")
	}
	// The phases below set a process-wide soft memory limit; restore it so
	// later tests/benchmarks in this binary don't run GC-throttled.
	prevLimit := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prevLimit) })
	f := buildFixture(t)
	duration := 60 * time.Second
	if raw := os.Getenv("AMBER_MIXED_SECS"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			duration = time.Duration(n) * time.Second
		}
	}

	phases := []struct {
		name     string
		budget   int64
		memLimit int64
	}{
		{name: "defaults", budget: 0, memLimit: 0},
		{name: "800MiB-limit", budget: 400 << 20, memLimit: 800 << 20},
	}
	for _, ph := range phases {
		if err := f.st.Close(); err != nil {
			t.Fatal(err)
		}
		debug.FreeOSMemory()
		if ph.memLimit > 0 {
			debug.SetMemoryLimit(ph.memLimit)
		}
		st, err := OpenWithOptions(fixtureDir, Options{CacheBudget: ph.budget})
		if err != nil {
			t.Fatal(err)
		}
		f.st = st // keep TestMain's cleanup pointed at the live store

		res := runMixed(t, st, f, duration)
		cs := st.CacheStats()
		t.Logf("phase=%s budget(dir=%dMiB resident=%dMiB) peakRSS=%dMiB avgRSS=%dMiB gcCPU=%.1f%% numGC=%d ops(qm2=%d qm1=%d qm4=%d sum=%d)",
			ph.name, cs.DirBudget>>20, cs.ResidentBudget>>20,
			res.peakRSS>>20, res.avgRSS>>20, res.gcCPUShare*100, res.numGC,
			res.qm2Ops, res.qm1Ops, res.qm4Ops, res.sumOps)
		t.Logf("phase=%s dircache bytes=%dMiB hit=%d miss=%d evict=%d | residentcache bytes=%dMiB hit=%d miss=%d evict=%d",
			ph.name, cs.DirBytes>>20, cs.DirHits, cs.DirMisses, cs.DirEvictions,
			cs.ResidentBytes>>20, cs.ResidentHits, cs.ResidentMisses, cs.ResidentEvictions)
	}

	// Restore the shared fixture to a defaults-budget store so later tests and
	// benchmarks in this binary don't inherit this test's 400MiB-budget store.
	if err := f.st.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := OpenWithOptions(fixtureDir, Options{})
	if err != nil {
		t.Fatal(err)
	}
	f.st = st
}

type mixedResult struct {
	peakRSS, avgRSS                int64
	gcCPUShare                     float64
	numGC                          uint32
	qm2Ops, qm1Ops, qm4Ops, sumOps int64
}

func runMixed(t *testing.T, st *Store, f *benchFixture, duration time.Duration) mixedResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	rs1m, _ := benchRangeSelector("service", time.Minute)
	span := f.lastTS - f.baseTS
	qm2From := f.baseTS
	qm2To := f.baseTS + span/2
	qm2Step := max((time.Duration(span/2/20) * time.Millisecond).Round(time.Second), time.Second)

	var res mixedResult
	var wg sync.WaitGroup
	worker := func(counter *int64, op func() error) {
		wg.Go(func() {
			for ctx.Err() == nil {
				if err := op(); err != nil {
					t.Error(err)
					return
				}
				atomic.AddInt64(counter, 1)
			}
		})
	}

	gcStart := gcCPUSeconds()
	var msStart runtime.MemStats
	runtime.ReadMemStats(&msStart)
	start := time.Now()

	// Range-step rate over half the span: the resident exact path.
	worker(&res.qm2Ops, func() error {
		_, err := st.RateByLabelRangeSteps(rs1m, qm2From, qm2To, qm2Step, "service")
		return err
	})
	// Instant rate by narrow and wide group key: the resident index.
	worker(&res.qm1Ops, func() error {
		_, err := st.RateByLabelRange(rs1m, f.lastTS, "service")
		return err
	})
	worker(&res.qm4Ops, func() error {
		_, err := st.RateByLabelRange(rs1m, f.lastTS, "route")
		return err
	})
	// sum-by-label with a time filter: the directory-cache scan path.
	worker(&res.sumOps, func() error {
		_, err := st.SumByLabel(rs1m.Selector, rs1m.Options(f.lastTS), "service")
		return err
	})

	// RSS sampler.
	var samples, sumRSS int64
	wg.Go(func() {
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				rss, err := readVmRSS()
				if err != nil {
					t.Errorf("readVmRSS: %v", err)
					return
				}
				samples++
				sumRSS += rss
				if rss > res.peakRSS {
					res.peakRSS = rss
				}
			}
		}
	})

	wg.Wait()
	if samples > 0 {
		res.avgRSS = sumRSS / samples
	}
	var msEnd runtime.MemStats
	runtime.ReadMemStats(&msEnd)
	res.numGC = msEnd.NumGC - msStart.NumGC
	wall := time.Since(start).Seconds()
	res.gcCPUShare = (gcCPUSeconds() - gcStart) / (wall * float64(runtime.GOMAXPROCS(0)))
	return res
}

// gcCPUSeconds reads cumulative GC CPU time from runtime/metrics.
func gcCPUSeconds() float64 {
	sample := []rtmetrics.Sample{{Name: "/cpu/classes/gc/total:cpu-seconds"}}
	rtmetrics.Read(sample)
	if sample[0].Value.Kind() != rtmetrics.KindFloat64 {
		return 0
	}
	return sample[0].Value.Float64()
}

// readVmRSS parses VmRSS (bytes) from /proc/self/status. It returns an error
// rather than calling t.Fatal so the sampler goroutine never trips the
// FailNow-off-the-test-goroutine footgun.
func readVmRSS() (int64, error) {
	file, err := os.Open("/proc/self/status")
	if err != nil {
		return 0, err
	}
	defer file.Close()
	sc := bufio.NewScanner(file)
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			break
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, err
		}
		return kb << 10, nil
	}
	if err := sc.Err(); err != nil {
		return 0, err
	}
	return 0, errors.New("VmRSS not found in /proc/self/status")
}
