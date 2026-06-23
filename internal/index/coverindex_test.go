package index

import (
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

type sampleSpan struct {
	id      uint64
	trace   [16]byte
	start   int64
	end     int64
	status  uint8
	op      string
	isRoot  bool
	service string
}

func buildSampleCover(t *testing.T, n int) (*CoverBuilder, []sampleSpan) {
	t.Helper()
	rng := rand.New(rand.NewSource(7))
	services := []string{"api", "db", "cache"}
	ops := []string{"GET", "POST", "PUT"}
	b := NewCoverBuilder()
	spans := make([]sampleSpan, 0, n)
	base := int64(1_700_000_000_000_000_000)
	for i := range n {
		var tr [16]byte
		rng.Read(tr[:])
		s := sampleSpan{
			id:      uint64(i*10 + rng.Intn(7)), // non-monotonic ids
			trace:   tr,
			start:   base + int64(rng.Intn(1_000_000_000)),
			status:  uint8(rng.Intn(3)),
			op:      ops[rng.Intn(len(ops))],
			isRoot:  rng.Intn(4) == 0,
			service: services[rng.Intn(len(services))],
		}
		s.end = s.start + int64(rng.Intn(5_000_000))
		spans = append(spans, s)
		b.Add(s.service, s.id, s.trace, s.start, s.end, s.status, s.op, s.isRoot)
	}
	return b, spans
}

func TestCoverIndexRoundTrip(t *testing.T) {
	b, spans := buildSampleCover(t, 600)
	path := filepath.Join(t.TempDir(), "seg.cidx")
	if err := b.Save(path); err != nil {
		t.Fatal(err)
	}
	ci, err := OpenCoverIndex(path)
	if err != nil {
		t.Fatal(err)
	}

	// Group expected rows per service, id-sorted (the .cidx stores that order).
	for _, svc := range []string{"api", "db", "cache"} {
		var want []sampleSpan
		for _, s := range spans {
			if s.service == svc {
				want = append(want, s)
			}
		}
		// id-sort, matching the builder.
		for i := 1; i < len(want); i++ {
			for j := i; j > 0 && want[j].id < want[j-1].id; j-- {
				want[j], want[j-1] = want[j-1], want[j]
			}
		}

		if !ci.HasService(svc) {
			t.Fatalf("HasService(%q) false", svc)
		}
		p, err := ci.ReadServiceProjection(svc)
		if err != nil {
			t.Fatal(err)
		}
		if p.Len() != len(want) {
			t.Fatalf("%s: got %d rows, want %d", svc, p.Len(), len(want))
		}
		// Resolve ordinals and op ids back; compare every field.
		ords := make([]uint32, p.Len())
		copy(ords, p.TraceOrd)
		traceIDs, err := ci.TraceIDs(ords)
		if err != nil {
			t.Fatal(err)
		}
		for i, w := range want {
			if p.Start[i] != w.start {
				t.Fatalf("%s row %d: start %d != %d", svc, i, p.Start[i], w.start)
			}
			if p.End[i] != w.end {
				t.Fatalf("%s row %d: end %d != %d", svc, i, p.End[i], w.end)
			}
			if p.Status[i] != w.status {
				t.Fatalf("%s row %d: status %d != %d", svc, i, p.Status[i], w.status)
			}
			if p.IsRoot[i] != w.isRoot {
				t.Fatalf("%s row %d: isRoot %v != %v", svc, i, p.IsRoot[i], w.isRoot)
			}
			if ci.Operation(p.OpID[i]) != w.op {
				t.Fatalf("%s row %d: op %q != %q", svc, i, ci.Operation(p.OpID[i]), w.op)
			}
			if traceIDs[i] != w.trace {
				t.Fatalf("%s row %d: trace id mismatch", svc, i)
			}
		}
	}

	if ci.HasService("missing") {
		t.Fatal("HasService(missing) true")
	}
	if p, err := ci.ReadServiceProjection("missing"); err != nil || p != nil {
		t.Fatalf("missing projection: p=%v err=%v", p, err)
	}
	if _, ok := ci.OpID("GET"); !ok {
		t.Fatal("OpID(GET) not found")
	}
	if _, ok := ci.OpID("NOPE"); ok {
		t.Fatal("OpID(NOPE) unexpectedly found")
	}
}

func TestCoverIndexEmpty(t *testing.T) {
	b := NewCoverBuilder()
	if !b.Empty() {
		t.Fatal("new builder should be empty")
	}
	b.Add("api", 1, [16]byte{1}, 100, 200, 1, "GET", true)
	if b.Empty() {
		t.Fatal("builder with a span should not be empty")
	}
}

func TestCoverIndexDetectsCorruption(t *testing.T) {
	b, _ := buildSampleCover(t, 400)
	path := filepath.Join(t.TempDir(), "seg.cidx")
	if err := b.Save(path); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the first service blob (after magic).
	data[40] ^= 0xFF
	corrupt := filepath.Join(t.TempDir(), "corrupt.cidx")
	if err := os.WriteFile(corrupt, data, 0o644); err != nil {
		t.Fatal(err)
	}
	ci, err := OpenCoverIndex(corrupt)
	if err != nil {
		// Corruption may land in the directory/footer too; that's also a detect.
		return
	}
	// If it opened, some service projection read must fail its CRC.
	failed := false
	for svc := range ci.dir {
		if _, err := ci.ReadServiceProjection(svc); err != nil {
			failed = true
		}
	}
	if !failed {
		t.Fatal("expected a CRC failure on a corrupted blob")
	}
}
