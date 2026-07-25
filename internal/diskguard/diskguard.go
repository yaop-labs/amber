// Package diskguard provides fail-closed disk-space admission for ingest.
package diskguard

import (
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	// ErrLowSpace means ingest stopped before the filesystem reached ENOSPC.
	ErrLowSpace = errors.New("disk admission: free space is below the stop watermark")
	// ErrProbe means free space could not be measured, so ingest failed closed.
	ErrProbe = errors.New("disk admission: filesystem probe failed")
)

// Config defines absolute free-space watermarks. WarningFreeBytes must be
// greater than or equal to StopFreeBytes.
type Config struct {
	WarningFreeBytes int64
	StopFreeBytes    int64
}

// Snapshot is one filesystem measurement and its derived admission state.
type Snapshot struct {
	FreeBytes  int64     `json:"free_bytes"`
	TotalBytes int64     `json:"total_bytes"`
	Warning    bool      `json:"warning"`
	Stopped    bool      `json:"stopped"`
	ProbeError string    `json:"probe_error,omitempty"`
	CheckedAt  time.Time `json:"checked_at"`
}

// Stats contains monotonic guard counters.
type Stats struct {
	ProbeFailures uint64
	Rejected      uint64
}

type probeFunc func(string) (freeBytes, totalBytes int64, err error)

// Guard samples the filesystem containing Path at every admission/readiness
// decision. Synchronous sampling avoids admitting a large request from stale
// low-space state.
type Guard struct {
	path  string
	cfg   Config
	probe probeFunc

	probeFailures atomic.Uint64
	rejected      atomic.Uint64
}

// New creates a filesystem-backed guard.
func New(path string, cfg Config) (*Guard, error) {
	return newWithProbe(path, cfg, statFS)
}

func newWithProbe(path string, cfg Config, probe probeFunc) (*Guard, error) {
	if path == "" {
		return nil, errors.New("disk admission: path is required")
	}
	if cfg.StopFreeBytes <= 0 {
		return nil, errors.New("disk admission: stop watermark must be positive")
	}
	if cfg.WarningFreeBytes < cfg.StopFreeBytes {
		return nil, errors.New("disk admission: warning watermark must be greater than or equal to stop watermark")
	}
	if probe == nil {
		return nil, errors.New("disk admission: probe is required")
	}
	return &Guard{path: path, cfg: cfg, probe: probe}, nil
}

// Sample measures the filesystem and derives warning/stop state. Probe errors
// are represented as stopped snapshots so readiness fails closed.
func (g *Guard) Sample() Snapshot {
	snapshot := Snapshot{CheckedAt: time.Now().UTC()}
	freeBytes, totalBytes, err := g.probe(g.path)
	if err != nil {
		g.probeFailures.Add(1)
		snapshot.Stopped = true
		snapshot.ProbeError = err.Error()
		return snapshot
	}
	snapshot.FreeBytes = freeBytes
	snapshot.TotalBytes = totalBytes
	snapshot.Warning = freeBytes <= g.cfg.WarningFreeBytes
	snapshot.Stopped = freeBytes <= g.cfg.StopFreeBytes
	return snapshot
}

// Admit permits ingest only while the current filesystem sample is above the
// stop watermark. Measurement failure is retryable and fails closed.
func (g *Guard) Admit() error {
	snapshot := g.Sample()
	if snapshot.ProbeError != "" {
		g.rejected.Add(1)
		return fmt.Errorf("%w: %s", ErrProbe, snapshot.ProbeError)
	}
	if snapshot.Stopped {
		g.rejected.Add(1)
		return fmt.Errorf("%w: free=%d stop=%d", ErrLowSpace, snapshot.FreeBytes, g.cfg.StopFreeBytes)
	}
	return nil
}

// Stats returns monotonic counters since process start.
func (g *Guard) Stats() Stats {
	return Stats{
		ProbeFailures: g.probeFailures.Load(),
		Rejected:      g.rejected.Load(),
	}
}

func statFS(path string) (int64, int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, 0, err
	}
	return saturatingMul(stat.Bavail, uint64(stat.Bsize)),
		saturatingMul(stat.Blocks, uint64(stat.Bsize)), nil
}

func saturatingMul(left, right uint64) int64 {
	if left == 0 || right == 0 {
		return 0
	}
	if left > uint64(math.MaxInt64)/right {
		return math.MaxInt64
	}
	return int64(left * right)
}
