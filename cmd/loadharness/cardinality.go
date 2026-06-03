package main

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
)

// catalog holds the active series population: a stable subset whose labels
// never change, plus an ephemeral subset whose labels rotate on demand
// (Rotate()). Series are addressable by integer index — the generator picks
// indices according to a Zipfian-ish skew, then dereferences here. Reads
// from the hot generator path are lock-free via atomic.Pointer; rotations
// briefly take a write lock to swap in a new ephemeral slice.
//
// Label scheme:
//
//   stable: __name__ + service + host + region
//   ephemeral: same + pod (uuid-shaped, rotated)
//
// The pod label gives us a clean knob: rotate it and the index sees a fresh
// series; everything else stays referenceable for the dashboard query.

type catalog struct {
	stableLabels []seriesLabels // immutable after construction

	mu              sync.Mutex
	ephemeralLabels atomic.Pointer[[]seriesLabels]
	rotateSeed      uint64
	rotations       atomic.Uint64
}

type seriesLabels struct {
	Service string
	Host    string
	Region  string
	Pod     string // empty for stable
	Stable  bool
}

var (
	services = []string{"api", "worker", "scheduler", "billing", "auth", "notification", "payment", "search", "ingest", "query"}
	hosts    = []string{"node-01", "node-02", "node-03", "node-04", "node-05", "node-06", "node-07", "node-08"}
	regions  = []string{"eu-west-1", "us-east-1", "ap-south-1"}
)

func newCatalog(totalSeries int, stableFraction float64, seed uint64) *catalog {
	if stableFraction < 0 {
		stableFraction = 0
	}
	if stableFraction > 1 {
		stableFraction = 1
	}
	stableCount := int(float64(totalSeries) * stableFraction)
	ephemeralCount := totalSeries - stableCount

	rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))

	stable := make([]seriesLabels, stableCount)
	for i := range stable {
		stable[i] = seriesLabels{
			Service: services[rng.IntN(len(services))],
			Host:    hosts[rng.IntN(len(hosts))],
			Region:  regions[rng.IntN(len(regions))],
			Stable:  true,
		}
	}

	c := &catalog{
		stableLabels: stable,
		rotateSeed:   seed,
	}
	ephemeral := makeEphemeral(ephemeralCount, c.rotateSeed, 0)
	c.ephemeralLabels.Store(&ephemeral)
	return c
}

func makeEphemeral(n int, baseSeed, generation uint64) []seriesLabels {
	rng := rand.New(rand.NewPCG(baseSeed^generation, generation))
	out := make([]seriesLabels, n)
	for i := range out {
		out[i] = seriesLabels{
			Service: services[rng.IntN(len(services))],
			Host:    hosts[rng.IntN(len(hosts))],
			Region:  regions[rng.IntN(len(regions))],
			Pod:     fmt.Sprintf("pod-%016x", rng.Uint64()),
			Stable:  false,
		}
	}
	return out
}

// StableSeries returns the immutable stable slice. Used by the dashboard
// query selector so it matches the persistent core.
func (c *catalog) StableSeries() []seriesLabels { return c.stableLabels }

// Pick returns the labels for series index i. Wraps modulo total active size.
// Hot path: atomic load + slice index, no lock.
func (c *catalog) Pick(i int) seriesLabels {
	stable := len(c.stableLabels)
	eph := c.ephemeralLabels.Load()
	total := stable + len(*eph)
	if total == 0 {
		// Degenerate: caller asked for series but there are none.
		return seriesLabels{Service: "none", Host: "none", Region: "none"}
	}
	i %= total
	if i < stable {
		return c.stableLabels[i]
	}
	return (*eph)[i-stable]
}

// Rotate rebuilds `fraction` of the ephemeral pool with fresh pod uuids.
// Threadsafe; generator goroutines may keep reading via Pick() concurrently
// (they will see either the old or the new slice atomically).
func (c *catalog) Rotate(fraction float64) {
	if fraction <= 0 {
		return
	}
	if fraction > 1 {
		fraction = 1
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	gen := c.rotations.Add(1)
	cur := *c.ephemeralLabels.Load()
	next := make([]seriesLabels, len(cur))
	copy(next, cur)
	rotateN := int(float64(len(cur)) * fraction)
	rng := rand.New(rand.NewPCG(c.rotateSeed^gen, gen))
	// Replace the first rotateN entries with fresh ones. Random selection
	// (vs first-N) would be more realistic but the dashboard query never
	// selects ephemeral series anyway, so we keep this deterministic.
	for i := 0; i < rotateN; i++ {
		next[i] = seriesLabels{
			Service: services[rng.IntN(len(services))],
			Host:    hosts[rng.IntN(len(hosts))],
			Region:  regions[rng.IntN(len(regions))],
			Pod:     fmt.Sprintf("pod-%016x", rng.Uint64()),
		}
	}
	c.ephemeralLabels.Store(&next)
}

// Rotations is the number of completed rotate calls (for reporting).
func (c *catalog) Rotations() uint64 { return c.rotations.Load() }
