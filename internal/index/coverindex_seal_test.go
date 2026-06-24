package index

import (
	"bytes"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

// TestBuildSpanSealIndexesCoverParity writes spans into a real segment, seals
// it, and checks the .cidx projection matches the input spans field-for-field -
// validating the seal wiring end to end (WriteTo->segment->ReadFrom->.cidx).
func TestBuildSpanSealIndexesCoverParity(t *testing.T) {
	dir := t.TempDir()
	segPath := filepath.Join(dir, "seg_00000001.alog")
	sw, err := storage.OpenSegmentWriter(segPath)
	if err != nil {
		t.Fatal(err)
	}

	rng := rand.New(rand.NewSource(3))
	services := []string{"api", "db", "cache"}
	ops := []string{"GET", "POST", "PUT"}
	entropy := ulid.Monotonic(rng, 0)
	base := time.Unix(1_700_000_000, 0)

	type want struct {
		id     uint64
		trace  model.TraceID
		start  int64
		end    int64
		status model.SpanStatus
		op     string
		isRoot bool
	}
	bySvc := map[string][]want{}

	for i := range 900 {
		id := ulid.MustNew(ulid.Timestamp(base.Add(time.Duration(i)*time.Millisecond)), entropy)
		var tr model.TraceID
		// 60 traces, so summaries group meaningfully.
		tr[0] = byte(i % 60)
		tr[1] = byte((i % 60) >> 8)
		svc := services[i%len(services)]
		op := ops[rng.Intn(len(ops))]
		isRoot := i%4 == 0
		start := base.Add(time.Duration(i) * time.Millisecond)
		end := start.Add(time.Duration(rng.Intn(5000)) * time.Microsecond)
		var parent model.SpanID
		if !isRoot {
			parent[0] = 9
		}
		s := model.SpanEntry{
			ID:        id,
			TraceID:   tr,
			SpanID:    model.SpanID{byte(i), byte(i >> 8)},
			ParentID:  parent,
			Service:   svc,
			Operation: op,
			StartTime: start,
			EndTime:   end,
			Status:    model.SpanStatus(rng.Intn(3)),
		}
		var buf bytes.Buffer
		if _, err := s.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		if err := sw.WriteRecord(buf.Bytes(), s.StartTime.UnixNano()); err != nil {
			t.Fatal(err)
		}
		bySvc[svc] = append(bySvc[svc], want{
			id:     model.EntryIDToUint64(id),
			trace:  tr,
			start:  start.UnixNano(),
			end:    end.UnixNano(),
			status: s.Status,
			op:     op,
			isRoot: s.IsRoot(),
		})
	}
	if err := sw.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := BuildSpanSealIndexes(segPath, nil); err != nil {
		t.Fatalf("seal: %v", err)
	}

	ci, err := OpenCoverIndex(segPath + ".cidx")
	if err != nil {
		t.Fatalf("open cidx: %v", err)
	}

	for svc, w := range bySvc {
		sort.Slice(w, func(i, j int) bool { return w[i].id < w[j].id })
		p, err := ci.ReadServiceProjection(svc)
		if err != nil {
			t.Fatal(err)
		}
		if p.Len() != len(w) {
			t.Fatalf("%s: %d rows, want %d", svc, p.Len(), len(w))
		}
		traceIDs, err := ci.TraceIDs(p.TraceOrd)
		if err != nil {
			t.Fatal(err)
		}
		for i := range w {
			if p.Start[i] != w[i].start || p.End[i] != w[i].end {
				t.Fatalf("%s row %d: time (%d,%d) want (%d,%d)", svc, i, p.Start[i], p.End[i], w[i].start, w[i].end)
			}
			if p.Status[i] != uint8(w[i].status) {
				t.Fatalf("%s row %d: status %d want %d", svc, i, p.Status[i], w[i].status)
			}
			if p.IsRoot[i] != w[i].isRoot {
				t.Fatalf("%s row %d: isRoot %v want %v", svc, i, p.IsRoot[i], w[i].isRoot)
			}
			if ci.Operation(p.OpID[i]) != w[i].op {
				t.Fatalf("%s row %d: op %q want %q", svc, i, ci.Operation(p.OpID[i]), w[i].op)
			}
			if model.TraceID(traceIDs[i]) != w[i].trace {
				t.Fatalf("%s row %d: trace mismatch", svc, i)
			}
		}
	}
}
