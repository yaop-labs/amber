// Package obsbench is the benchmark harness behind benchmarks/METHODOLOGY.md:
// dataset generation, ingest load generation, memory sampling, environment
// preflight, and post-ingest verification. It lives in the amber repo but
// drives every system under test through the same code paths.
package obsbench

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// Record is one dataset log entry. Seq is the loss-accounting identity: the
// generator assigns a dense sequence, ingest reports the acked range, and
// verification compares queryable counts against it (METHODOLOGY.md §4).
// Timestamps are assigned at send time, not generation time, so every system
// ingests a "live" stream the way real clients produce one.
type Record struct {
	Seq     uint64            `json:"seq"`
	Service string            `json:"service"`
	Level   string            `json:"level"`
	Host    string            `json:"host"`
	Body    string            `json:"body"`
	Attrs   map[string]string `json:"attrs,omitempty"`
}

// DatasetWriter streams records into a zstd-compressed NDJSON file. The raw
// dataset never lands on disk uncompressed (METHODOLOGY.md §2: disk budget).
type DatasetWriter struct {
	f   *os.File
	zw  *zstd.Encoder
	bw  *bufio.Writer
	enc *json.Encoder
}

func NewDatasetWriter(path string) (*DatasetWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	zw, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	bw := bufio.NewWriterSize(zw, 1<<20)
	return &DatasetWriter{f: f, zw: zw, bw: bw, enc: json.NewEncoder(bw)}, nil
}

func (w *DatasetWriter) Write(r Record) error {
	return w.enc.Encode(r)
}

func (w *DatasetWriter) Close() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	if err := w.zw.Close(); err != nil {
		return err
	}
	return w.f.Close()
}

// DatasetReader streams records back out of a generated dataset.
type DatasetReader struct {
	f  *os.File
	zr *zstd.Decoder
	sc *bufio.Scanner
}

func OpenDataset(path string) (*DatasetReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	zr, err := zstd.NewReader(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	sc := bufio.NewScanner(zr)
	// Bodies are bounded (~1 KB), but leave generous headroom.
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	return &DatasetReader{f: f, zr: zr, sc: sc}, nil
}

// Next returns the next record or io.EOF.
func (r *DatasetReader) Next() (Record, error) {
	if !r.sc.Scan() {
		if err := r.sc.Err(); err != nil {
			return Record{}, err
		}
		return Record{}, io.EOF
	}
	var rec Record
	if err := json.Unmarshal(r.sc.Bytes(), &rec); err != nil {
		return Record{}, fmt.Errorf("dataset: decode line: %w", err)
	}
	return rec, nil
}

func (r *DatasetReader) Close() error {
	r.zr.Close()
	return r.f.Close()
}
