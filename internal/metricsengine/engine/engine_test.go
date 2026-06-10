package engine

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/wal"
)

func TestWALReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "head.wal")
	labels := model.LabelSet{{Name: "job", Value: "api"}}

	e, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(labels, model.MetricTypeCounter, 1000, 10); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(labels, model.MetricTypeCounter, 2000, 20); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	recovered, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	blockPath := filepath.Join(dir, "recovered.meb")
	if err := recovered.FlushBlock(blockPath); err != nil {
		t.Fatal(err)
	}
	series, err := block.ReadFile(blockPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(series) != 1 || len(series[0].Values) != 2 {
		t.Fatalf("recovered series = %+v", series)
	}
	if series[0].Values[1] != 20 {
		t.Fatalf("last value = %d, want 20", series[0].Values[1])
	}
}

func TestAppendBatch(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		t.Fatal(err)
	}
	labels := model.LabelSet{{Name: "job", Value: "api"}}
	ids, err := e.AppendBatch([]model.Sample{
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 1000, Value: 10},
		{Labels: labels, Type: model.MetricTypeCounter, Timestamp: 2000, Value: 20},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("ids = %v, want same series id for both samples", ids)
	}
}

func TestPrepareAndCommitFlush(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(model.LabelSet{{Name: "job", Value: "api"}}, model.MetricTypeGauge, 0, 1); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "prepared.meb")
	if err := e.PrepareFlushBlock(path); err != nil {
		t.Fatal(err)
	}
	if e.BufferedSeries() != 1 {
		t.Fatalf("BufferedSeries after prepare = %d, want 1", e.BufferedSeries())
	}
	if err := e.CommitFlush(); err != nil {
		t.Fatal(err)
	}
	if e.BufferedSeries() != 0 {
		t.Fatalf("BufferedSeries after commit = %d, want 0", e.BufferedSeries())
	}
}

func TestAppendScaledFloatRejectsUnencodable(t *testing.T) {
	e := New()
	labels := model.LabelSet{{Name: "job", Value: "api"}}

	cases := []struct {
		name  string
		value float64
		scale int64
	}{
		{"nan", math.NaN(), 1000},
		{"pos_inf", math.Inf(1), 1000},
		{"neg_inf", math.Inf(-1), 1000},
		{"overflow", 1e18, 1000},
		{"neg_overflow", -1e18, 1000},
	}
	for _, tc := range cases {
		if _, err := e.AppendScaledFloat(labels, model.MetricTypeGauge, 1000, tc.value, tc.scale); err == nil {
			t.Errorf("%s: AppendScaledFloat(%v, scale=%d) succeeded, want error", tc.name, tc.value, tc.scale)
		}
	}
	if got := e.BufferedSamples(); got != 0 {
		t.Errorf("BufferedSamples = %d after rejected appends, want 0", got)
	}

	// Sanity: a representable value still works.
	if _, err := e.AppendScaledFloat(labels, model.MetricTypeGauge, 1000, 1.234, 1000); err != nil {
		t.Fatalf("valid AppendScaledFloat: %v", err)
	}
}

// After a flush truncates the WAL, the next append for an existing series
// must re-declare it (KindSeries) or replay would find an unresolvable ID.
func TestWALRedeclaresSeriesAfterFlush(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "head.wal")
	labels := model.LabelSet{{Name: "job", Value: "api"}}

	e, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(labels, model.MetricTypeCounter, 1000, 1); err != nil {
		t.Fatal(err)
	}
	if err := e.FlushBlock(filepath.Join(dir, "b1.meb")); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Append(labels, model.MetricTypeCounter, 2000, 2); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	// Fresh registry — labels must come from the WAL's own series records.
	e2, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if got := e2.UnknownWALSeries(); got != 0 {
		t.Fatalf("UnknownWALSeries = %d, want 0", got)
	}
	if got := e2.BufferedSamples(); got != 1 {
		t.Fatalf("BufferedSamples = %d, want 1 (post-flush sample)", got)
	}
	snap := e2.Snapshot()
	if len(snap) != 1 || len(snap[0].Labels) != 1 || snap[0].Labels[0].Value != "api" {
		t.Fatalf("snapshot = %+v, want labels from WAL series record", snap)
	}
}

// A WAL written by the retired JSON format must replay into the head.
func TestWALReplaysLegacyJSON(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "head.wal")
	if err := writeLegacyWAL(walPath, model.LabelSet{{Name: "job", Value: "api"}}, 1000, 5); err != nil {
		t.Fatal(err)
	}

	e, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := e.BufferedSamples(); got != 1 {
		t.Fatalf("BufferedSamples = %d, want 1", got)
	}
	snap := e.Snapshot()
	if len(snap) != 1 || snap[0].Values[0] != 5 {
		t.Fatalf("snapshot = %+v", snap)
	}
}

// A sample whose ID resolves neither via WAL series records nor the registry
// is skipped and counted, not invented with empty labels.
func TestWALUnknownSeriesSkipped(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "head.wal")
	w, err := wal.Open(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(wal.Record{Kind: wal.KindSample, ID: 99, Type: model.MetricTypeGauge, Timestamp: 1, Value: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	e, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got := e.UnknownWALSeries(); got != 1 {
		t.Fatalf("UnknownWALSeries = %d, want 1", got)
	}
	if got := e.BufferedSamples(); got != 0 {
		t.Fatalf("BufferedSamples = %d, want 0", got)
	}
}

// Concurrent appends racing a flush must never lose an acked sample: every
// sample is either in a flushed block or in the head (and the WAL).
func TestFlushGateNoAckedSampleLoss(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(Options{WALPath: filepath.Join(dir, "head.wal")})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const writers = 4
	const perWriter = 200
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			labels := model.LabelSet{{Name: "job", Value: fmt.Sprintf("w%d", w)}}
			for i := 0; i < perWriter; i++ {
				if _, err := e.Append(labels, model.MetricTypeCounter, int64(i), int64(i)); err != nil {
					t.Errorf("append: %v", err)
					return
				}
			}
		}(w)
	}

	flushDone := make(chan struct{})
	blockSeq := 0
	go func() {
		defer close(flushDone)
		for i := 0; i < 20; i++ {
			if e.BufferedSeries() > 0 {
				if err := e.FlushBlock(filepath.Join(dir, fmt.Sprintf("b%03d.meb", blockSeq))); err != nil {
					t.Errorf("flush: %v", err)
					return
				}
				blockSeq++
			}
		}
	}()
	wg.Wait()
	<-flushDone

	total := e.BufferedSamples()
	blocks, err := filepath.Glob(filepath.Join(dir, "b*.meb"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range blocks {
		series, err := block.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range series {
			total += len(s.Values)
		}
	}
	if want := writers * perWriter; total != want {
		t.Fatalf("samples across head+blocks = %d, want %d", total, want)
	}
}

func writeLegacyWAL(path string, labels model.LabelSet, ts, value int64) error {
	payload, err := json.Marshal(map[string]any{
		"labels":    labels,
		"type":      model.MetricTypeGauge,
		"timestamp": ts,
		"value":     value,
	})
	if err != nil {
		return err
	}
	var header [8]byte
	binary.LittleEndian.PutUint32(header[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(header[4:8], crc32.ChecksumIEEE(payload))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(header[:], payload...)); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// The point of the binary WAL: a sample must cost ~20 bytes, not the ~200 of
// the retired JSON format that repeated the full label set per sample.
func TestWALBinaryBytesPerSample(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "head.wal")
	e, err := Open(Options{WALPath: walPath})
	if err != nil {
		t.Fatal(err)
	}
	labels := model.LabelSet{
		{Name: "__name__", Value: "http_server_request_duration_seconds"},
		{Name: "resource.service.name", Value: "checkout"},
		{Name: "http.route", Value: "/api/v1/orders"},
		{Name: "http.method", Value: "POST"},
	}
	const n = 1000
	base := int64(1_700_000_000_000)
	samples := make([]model.Sample, n)
	for i := range samples {
		samples[i] = model.Sample{Labels: labels, Type: model.MetricTypeCounter, Timestamp: base + int64(i)*10_000, Value: int64(i)}
	}
	if _, err := e.AppendBatch(samples); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	perSample := float64(info.Size()) / n
	t.Logf("WAL bytes/sample = %.1f (file %d bytes, %d samples, labels once)", perSample, info.Size(), n)
	if perSample > 30 {
		t.Errorf("WAL bytes/sample = %.1f, want ≤ 30 (labels must not repeat per sample)", perSample)
	}
}
