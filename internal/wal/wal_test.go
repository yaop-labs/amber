package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestSharedWALInterleavesStreamsAndReclaimsByPresentStreams(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SegmentBytes: 96})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	first, err := w.Append(StreamLog, []byte("log-1"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := w.Append(StreamMetrics, []byte("metric-1"))
	if err != nil {
		t.Fatal(err)
	}
	if second <= first {
		t.Fatalf("global sequence did not advance: %d %d", first, second)
	}
	if _, err := w.Append(StreamSpan, []byte("span-1")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(StreamMetrics, []byte("metric-2")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(StreamLog, []byte("log-2")); err != nil {
		t.Fatal(err)
	}

	checkpoints := w.Checkpoints()
	if checkpoints != (Checkpoints{}) {
		t.Fatalf("unexpected checkpoints before reclaim: %+v", checkpoints)
	}

	if err := w.Checkpoint(StreamLog, 5); err != nil {
		t.Fatal(err)
	}
	if err := w.Checkpoint(StreamSpan, 3); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	segmentsBeforeMetricsCheckpoint := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".awl" {
			segmentsBeforeMetricsCheckpoint++
		}
	}
	if segmentsBeforeMetricsCheckpoint < 2 {
		t.Fatalf("metrics stream should still pin its segment, got %d segments", segmentsBeforeMetricsCheckpoint)
	}
	if err := w.Checkpoint(StreamMetrics, 4); err != nil {
		t.Fatal(err)
	}

	entries, err = os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var segments int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".awl" {
			segments++
		}
	}
	if segments != 1 {
		t.Fatalf("expected reclaim to leave only active segment, got %d", segments)
	}
}

func TestSharedWALReplayFiltersStreamAndRepairsTornTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	seq, err := w.Append(StreamLog, []byte("log"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(StreamMetrics, []byte("metrics")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	files, _ := filepath.Glob(filepath.Join(dir, "*.awl"))
	if len(files) != 1 {
		t.Fatalf("segments=%d", len(files))
	}
	f, err := os.OpenFile(files[0], os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	var bad [7]byte
	binary.LittleEndian.PutUint32(bad[:4], recordMagic)
	if _, err := f.Write(bad[:]); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w, err = Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	var got []string
	stats, err := w.Replay(StreamLog, func(gotSeq uint64, payload []byte) error {
		if gotSeq != seq {
			t.Fatalf("seq=%d want=%d", gotSeq, seq)
		}
		got = append(got, string(payload))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "log" {
		t.Fatalf("got=%v", got)
	}
	info, err := os.Stat(files[0])
	if err != nil {
		t.Fatal(err)
	}
	expected := int64(segmentHeaderSize + headerSize + len("log") + headerSize + len("metrics"))
	if info.Size() != expected {
		t.Fatalf("repaired size=%d want=%d (stats=%+v)", info.Size(), expected, stats)
	}
}

func TestSharedWALCRCInDurablePrefixIsFatal(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := w.Append(StreamLog, []byte("good")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.awl"))
	if len(files) != 1 {
		t.Fatal("expected one segment")
	}
	f, err := os.OpenFile(files[0], os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	// Flip one CRC byte in the first record. Header is 16 bytes, record header is 28.
	if _, err := f.WriteAt([]byte{0xFF}, int64(segmentHeaderSize)+20); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()
	if _, err := Open(dir, Options{SegmentBytes: 1 << 20}); err == nil {
		t.Fatal("expected CRC corruption error")
	}
}

func TestSharedWALPerStreamSequenceSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	logSeq, err := w.Append(StreamLog, []byte("l"))
	if err != nil {
		t.Fatal(err)
	}
	metricSeq, err := w.Append(StreamMetrics, []byte("m"))
	if err != nil {
		t.Fatal(err)
	}
	if logSeq == metricSeq {
		t.Fatal("stream sequences must be globally unique")
	}
	if got := w.LastStreamSeq(StreamLog); got != logSeq {
		t.Fatalf("log last stream seq=%d want=%d", got, logSeq)
	}
	if got := w.LastStreamSeq(StreamMetrics); got != metricSeq {
		t.Fatalf("metrics last stream seq=%d want=%d", got, metricSeq)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = Open(dir, Options{SegmentBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if got := w.LastStreamSeq(StreamLog); got != logSeq {
		t.Fatalf("reopened log last stream seq=%d want=%d", got, logSeq)
	}
	if got := w.LastStreamSeq(StreamMetrics); got != metricSeq {
		t.Fatalf("reopened metrics last stream seq=%d want=%d", got, metricSeq)
	}
}
