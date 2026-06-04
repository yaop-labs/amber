package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// Catalog log: codec + lifecycle tests.
//
// E.1 Round-trip:               TestCatalogLogRoundTrip
// E.2 Torn write at EOF:        TestCatalogLogTornAtEOF
// E.3 CRC mismatch -> refuse:   TestCatalogLogCRCMismatchRefuses
// E.4 Snapshot then log:        TestCatalogLogSnapshotThenLog
// E.5 Crash between snap+trunc: TestCatalogLogCrashBetweenSnapshotAndTruncate
// E.6 Full rotation matrix:     TestCatalogLogRotationCrashMatrix
// orphaned .tmp recovery:       TestCatalogLogOrphanedTmpRecovery

// --- E.1 — round-trip --------------------------------------------------------

func TestCatalogLogRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
	mustRegister(t, cl, 2, model.LabelSet{{Name: "job", Value: "worker"}, {Name: "pod", Value: "p-1"}})
	mustRegister(t, cl, 3, model.LabelSet{{Name: "job", Value: "scheduler"}})
	if err := cl.AppendEvict(2, 1717000000000); err != nil {
		t.Fatal(err)
	}
	mustRegister(t, cl, 4, model.LabelSet{{Name: "job", Value: "billing"}})

	cl.Close()

	live, highest, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	wantIDs := map[uint64]string{1: "api", 3: "scheduler", 4: "billing"}
	if len(live) != len(wantIDs) {
		t.Fatalf("live=%v, want ids=%v", live, wantIDs)
	}
	for id, jobVal := range wantIDs {
		ls, ok := live[id]
		if !ok {
			t.Fatalf("missing id %d", id)
		}
		if ls[0].Name != "job" || ls[0].Value != jobVal {
			t.Fatalf("id %d: labels=%v want job=%s", id, ls, jobVal)
		}
	}
	if highest != 4 {
		t.Fatalf("highest=%d want 4", highest)
	}
}

// --- E.2 — torn write at EOF ------------------------------------------------

func TestCatalogLogTornAtEOF(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
	mustRegister(t, cl, 2, model.LabelSet{{Name: "job", Value: "worker"}})
	cl.Close()

	logPath := filepath.Join(dir, catalogLogFileName)
	st, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	// Append 5 bytes of garbage (simulates a partial fsync — header
	// started but body didn't make it). Truncate at that should be the
	// post-recovery state.
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff, 0xff, 0xff, 0xff, 0x00}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	live, highest, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatalf("torn-tail recovery failed: %v", err)
	}
	if len(live) != 2 || highest != 2 {
		t.Fatalf("live=%v highest=%d want 2 entries", live, highest)
	}
	st2, _ := os.Stat(logPath)
	if st2.Size() != st.Size() {
		t.Fatalf("torn tail not truncated: before=%d after=%d", st.Size(), st2.Size())
	}
}

// TestCatalogLogTornTailIsIdempotentAcrossReboots covers the invariant
// the load-bearing replay path relies on: after a torn-tail recovery
// truncates and syncs, a SECOND immediate load yields the same state
// and does no further truncation. Without this property a series of
// crashes could compound, each chopping a few more records.
func TestCatalogLogTornTailIsIdempotentAcrossReboots(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := uint64(1); i <= 5; i++ {
		mustRegister(t, cl, i, model.LabelSet{{Name: "id", Value: strconv.FormatUint(i, 10)}})
	}
	cl.Close()

	// Tear the tail.
	logPath := filepath.Join(dir, catalogLogFileName)
	f, err := os.OpenFile(logPath, os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0xff, 0xff, 0x00, 0x00}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	live1, highest1, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	sizeAfter1, _ := os.Stat(logPath)

	// Second load must yield identical state and the file must not
	// shrink further — the truncate from the first load was final.
	live2, highest2, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	sizeAfter2, _ := os.Stat(logPath)

	if len(live1) != len(live2) || highest1 != highest2 {
		t.Fatalf("second load diverged: live1=%v highest1=%d, live2=%v highest2=%d",
			live1, highest1, live2, highest2)
	}
	if sizeAfter1.Size() != sizeAfter2.Size() {
		t.Fatalf("file shrunk on second load: %d -> %d", sizeAfter1.Size(), sizeAfter2.Size())
	}
}

// --- E.3 — CRC mismatch refuses to proceed ----------------------------------

func TestCatalogLogCRCMismatchRefuses(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
	mustRegister(t, cl, 2, model.LabelSet{{Name: "job", Value: "worker"}})
	cl.Close()

	logPath := filepath.Join(dir, catalogLogFileName)
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	// Flip a bit inside the SECOND record's body (not in total_len —
	// otherwise we hit the "invalid record length" branch instead of
	// CRC failure). Body of record 1 starts at offset catalogHeaderLen.
	rec1Len := int(binary.LittleEndian.Uint32(data[0:4]))
	rec2BodyOff := rec1Len + catalogHeaderLen + 5 // a few bytes into record-2's body
	data[rec2BodyOff] ^= 0x01
	if err := os.WriteFile(logPath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err = loadCatalogLogState(dir)
	if !errors.Is(err, ErrCatalogLogCorrupt) {
		t.Fatalf("err=%v, want ErrCatalogLogCorrupt", err)
	}
}

// --- E.4 — snapshot then log -----------------------------------------------

func TestCatalogLogSnapshotThenLog(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
	mustRegister(t, cl, 2, model.LabelSet{{Name: "job", Value: "worker"}})

	// Compaction: write a snapshot from the current live set, then
	// rotate. This is the production path Store will call.
	series := []liveSeries{
		{ID: 1, Labels: model.LabelSet{{Name: "job", Value: "api"}}},
		{ID: 2, Labels: model.LabelSet{{Name: "job", Value: "worker"}}},
	}
	if err := writeSnapshot(dir, series); err != nil {
		t.Fatal(err)
	}
	if err := cl.rotateLog(); err != nil {
		t.Fatal(err)
	}

	// More records after rotation.
	mustRegister(t, cl, 3, model.LabelSet{{Name: "job", Value: "billing"}})
	if err := cl.AppendEvict(1, 100); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	live, highest, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	// Expected: 2 (from snapshot) + 3 (from new log) - 1 (evicted).
	want := map[uint64]string{2: "worker", 3: "billing"}
	if len(live) != len(want) {
		t.Fatalf("live=%v want %v", live, want)
	}
	for id, job := range want {
		ls, ok := live[id]
		if !ok || ls[0].Value != job {
			t.Fatalf("id %d: ls=%v want job=%s", id, ls, job)
		}
	}
	if highest != 3 {
		t.Fatalf("highest=%d want 3", highest)
	}
}

// --- E.5 — crash between snapshot rename and log truncate -------------------
// (Specifically: the rotation completes step 3a but not 3b; or 3a+3b but not
// 3c. Recovery yields the live set in both cases.)
//
// This is the "old log still present + new snapshot exists" transition —
// recovery's job is to replay snapshot then log.old, which is idempotent
// for any REGISTER that pre-dates the snapshot.

func TestCatalogLogCrashBetweenSnapshotAndTruncate(t *testing.T) {
	dir := t.TempDir()
	cl, err := openCatalogLog(dir)
	if err != nil {
		t.Fatal(err)
	}
	mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
	mustRegister(t, cl, 2, model.LabelSet{{Name: "job", Value: "worker"}})

	series := []liveSeries{
		{ID: 1, Labels: model.LabelSet{{Name: "job", Value: "api"}}},
		{ID: 2, Labels: model.LabelSet{{Name: "job", Value: "worker"}}},
	}
	if err := writeSnapshot(dir, series); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash AFTER 3a (snapshot rename) but BEFORE 3b (log
	// rename) — the live log is still in place, the snapshot is new.
	cl.rotationStop = "post-3a"
	if err := cl.rotateLog(); !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("rotation err=%v, want errSimulatedCrash", err)
	}
	cl.rotationStop = ""
	cl.Close()

	// On-disk state: snapshot exists with {1,2}; log still contains
	// {1,2} entries. Recovery should yield {1,2} (idempotent replay).
	live, highest, err := loadCatalogLogState(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 2 || highest != 2 {
		t.Fatalf("live=%v highest=%d want 2 entries", live, highest)
	}
}

// --- E.6 — full crash matrix + concurrent append survives -------------------

func TestCatalogLogRotationCrashMatrix(t *testing.T) {
	// Test every transition in the lifecycle's crash matrix. For each
	// stop point, we:
	//   1. set up state with a couple of registered series
	//   2. write a snapshot
	//   3. attempt rotation, halted at the stop point
	//   4. simulate a concurrently-arrived record by writing one more
	//      record to whichever log file is "live" at that crash point
	//      (the file the production AppendRegister would target)
	//   5. close, reopen via recovery, assert the live set includes the
	//      concurrently-appended record AND the prior state
	//
	// "Concurrently arrived" here means "arrived in the brief window
	// where ingest could still see the OLD log fd before the rotation
	// swapped it" — which under the WRITE LOCK should not happen in
	// production (the lock blocks ingest for the duration of rotateLog).
	// We exercise the more aggressive scenario where we crash with a
	// dirty log to prove recovery still merges correctly.
	stops := []struct {
		name string
		// after the simulated crash, which file is the "live" log that
		// a concurrently-running AppendRegister would target?
		// "old" = catalog.log (rotation hasn't renamed yet)
		// "new" = catalog.log (rotation has renamed old -> log.old and
		//                      created a fresh log)
		liveLog string
	}{
		{"post-3a", "old"},
		{"post-3b", "old"}, // log was renamed to log.old; no new log yet.
		// We treat this as "concurrent write attempted but had no
		// target" — assert the prior records survive without the
		// concurrent one.
		{"post-3c", "new"},
		{"post-3d", "new"},
	}
	for _, tc := range stops {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cl, err := openCatalogLog(dir)
			if err != nil {
				t.Fatal(err)
			}

			// Seed with two registered series.
			mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
			mustRegister(t, cl, 2, model.LabelSet{{Name: "job", Value: "worker"}})

			series := []liveSeries{
				{ID: 1, Labels: model.LabelSet{{Name: "job", Value: "api"}}},
				{ID: 2, Labels: model.LabelSet{{Name: "job", Value: "worker"}}},
			}
			if err := writeSnapshot(dir, series); err != nil {
				t.Fatal(err)
			}

			cl.rotationStop = tc.name
			err = cl.rotateLog()
			cl.rotationStop = ""
			if !errors.Is(err, errSimulatedCrash) {
				t.Fatalf("rotation err=%v want errSimulatedCrash", err)
			}

			// After the simulated crash we may have stale fd state.
			// Close any descriptors the catalogLog still holds.
			cl.Close()

			// Concurrent write: append a third REGISTER to the file
			// the production writer would have targeted at this
			// instant. For post-3a, the writer still has the old log
			// fd, which is still named catalog.log. For post-3b, the
			// log has been renamed to catalog.log.old — a writer with
			// the old fd would still be writing to that inode. For
			// post-3c onward, the new log fd is live.
			var targetFile string
			switch tc.liveLog {
			case "old":
				// At post-3a, catalog.log still exists; at post-3b it
				// has been renamed to catalog.log.old. Find whichever.
				if _, err := os.Stat(filepath.Join(dir, catalogLogFileName)); err == nil {
					targetFile = filepath.Join(dir, catalogLogFileName)
				} else {
					targetFile = filepath.Join(dir, catalogLogOldFileName)
				}
			case "new":
				targetFile = filepath.Join(dir, catalogLogFileName)
			}
			if targetFile != "" {
				f, err := os.OpenFile(targetFile, os.O_APPEND|os.O_WRONLY, 0o644)
				if err != nil {
					t.Fatalf("opening %s for concurrent append: %v", targetFile, err)
				}
				rec := encodeRegister(3, model.LabelSet{{Name: "job", Value: "billing"}})
				if _, err := f.Write(rec); err != nil {
					t.Fatal(err)
				}
				if err := f.Sync(); err != nil {
					t.Fatal(err)
				}
				f.Close()
			}

			// Recover and assert.
			live, highest, err := loadCatalogLogState(dir)
			if err != nil {
				t.Fatalf("recovery: %v", err)
			}
			// Prior state must survive.
			if _, ok := live[1]; !ok {
				t.Fatalf("[%s] lost series 1: live=%v", tc.name, live)
			}
			if _, ok := live[2]; !ok {
				t.Fatalf("[%s] lost series 2: live=%v", tc.name, live)
			}
			// Concurrent write must survive.
			if targetFile != "" {
				if ls, ok := live[3]; !ok || ls[0].Value != "billing" {
					t.Fatalf("[%s] lost concurrently-appended series 3: live=%v", tc.name, live)
				}
				if highest != 3 {
					t.Fatalf("[%s] highest=%d want 3", tc.name, highest)
				}
			}
		})
	}
}

// --- orphaned .tmp recovery -------------------------------------------------

func TestCatalogLogOrphanedTmpRecovery(t *testing.T) {
	// Two scenarios:
	//   a) .tmp exists, snapshot does NOT — writeSnapshot crashed
	//      mid-flight. Recovery removes the .tmp and proceeds with
	//      whatever the log says.
	//   b) .tmp exists, snapshot ALSO exists — rename succeeded but
	//      dir-fsync didn't get to flush the tmp's directory-entry
	//      removal. Recovery removes the .tmp; snapshot is authoritative.

	t.Run("orphan_tmp_no_snapshot", func(t *testing.T) {
		dir := t.TempDir()
		cl, err := openCatalogLog(dir)
		if err != nil {
			t.Fatal(err)
		}
		mustRegister(t, cl, 1, model.LabelSet{{Name: "job", Value: "api"}})
		cl.Close()

		// Plant an orphan .tmp with garbage content.
		if err := os.WriteFile(filepath.Join(dir, catalogSnapshotTmpFileName),
			bytes.Repeat([]byte{0xab}, 100), 0o644); err != nil {
			t.Fatal(err)
		}

		live, _, err := loadCatalogLogState(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(live) != 1 {
			t.Fatalf("live=%v want 1 entry from log", live)
		}
		if _, err := os.Stat(filepath.Join(dir, catalogSnapshotTmpFileName)); !os.IsNotExist(err) {
			t.Fatalf("orphan .tmp not cleaned: %v", err)
		}
	})

	t.Run("orphan_tmp_with_snapshot", func(t *testing.T) {
		dir := t.TempDir()

		// Make a real snapshot.
		if err := writeSnapshot(dir, []liveSeries{
			{ID: 1, Labels: model.LabelSet{{Name: "job", Value: "api"}}},
		}); err != nil {
			t.Fatal(err)
		}
		// Promote it to .snapshot by hand (no rotate call here — we want
		// to leave a .tmp in place after).
		if err := os.Rename(
			filepath.Join(dir, catalogSnapshotTmpFileName),
			filepath.Join(dir, catalogSnapshotFileName)); err != nil {
			t.Fatal(err)
		}
		// Plant another .tmp on top (= the crash-between-rename-and-
		// dir-fsync scenario, where the new .tmp from a SECOND attempted
		// snapshot got partway through).
		if err := os.WriteFile(filepath.Join(dir, catalogSnapshotTmpFileName),
			bytes.Repeat([]byte{0xcd}, 50), 0o644); err != nil {
			t.Fatal(err)
		}

		live, highest, err := loadCatalogLogState(dir)
		if err != nil {
			t.Fatal(err)
		}
		if len(live) != 1 || highest != 1 {
			t.Fatalf("live=%v highest=%d want 1 entry", live, highest)
		}
		if _, err := os.Stat(filepath.Join(dir, catalogSnapshotTmpFileName)); !os.IsNotExist(err) {
			t.Fatalf("orphan .tmp not cleaned")
		}
	})
}

// --- codec direct tests (small, fast) ---------------------------------------

func TestCatalogLogCodecRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		enc  []byte
	}{
		{"register-simple", encodeRegister(42, model.LabelSet{{Name: "job", Value: "api"}})},
		{"register-many-labels", encodeRegister(99,
			model.LabelSet{
				{Name: "__name__", Value: "http_requests_total"},
				{Name: "method", Value: "GET"},
				{Name: "path", Value: "/api/v1/metrics"},
				{Name: "status", Value: "200"},
			})},
		{"evict", encodeEvict(7, 1717000000000)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec, _, err := readRecord(bytes.NewReader(tc.enc))
			if err != nil {
				t.Fatal(err)
			}
			switch tc.name {
			case "register-simple":
				if rec.seriesID != 42 || rec.typ != catalogRecordRegister || len(rec.labels) != 1 {
					t.Fatalf("got %+v", rec)
				}
			case "evict":
				if rec.seriesID != 7 || rec.typ != catalogRecordEvict || rec.ts != 1717000000000 {
					t.Fatalf("got %+v", rec)
				}
			}
		})
	}
}

func TestCatalogLogCodecCRCDetectsLengthFlip(t *testing.T) {
	// Specifically: if total_len is corrupted, the CRC (which covers
	// total_len) must catch it. Without CRC over total_len, recovery
	// would seek to the wrong boundary and either cascade failures or
	// silently validate garbage.
	enc := encodeRegister(42, model.LabelSet{{Name: "job", Value: "api"}})
	// Flip a bit in total_len[0..4].
	enc[0] ^= 0x10
	_, _, err := readRecord(bytes.NewReader(enc))
	if err == nil {
		t.Fatal("CRC did not catch total_len corruption")
	}
	// May surface as ErrCatalogLogCorrupt (CRC fail) OR
	// ErrCatalogLogCorrupt-wrapped-invalid-length depending on whether
	// the flip landed inside the legal range. Both are acceptable.
	if !errors.Is(err, ErrCatalogLogCorrupt) {
		t.Fatalf("err=%v want ErrCatalogLogCorrupt-derived", err)
	}
}

func TestCatalogLogCodecPartialHeaderIsTorn(t *testing.T) {
	enc := encodeRegister(42, model.LabelSet{{Name: "job", Value: "api"}})
	short := enc[:5] // first 5 bytes of an 8-byte header
	_, _, err := readRecord(bytes.NewReader(short))
	if !errors.Is(err, ErrCatalogLogTorn) {
		t.Fatalf("err=%v want ErrCatalogLogTorn", err)
	}
}

func TestCatalogLogCodecCleanEOF(t *testing.T) {
	_, _, err := readRecord(bytes.NewReader(nil))
	if err != io.EOF {
		t.Fatalf("err=%v want io.EOF", err)
	}
}

// --- helpers ----------------------------------------------------------------

func mustRegister(t *testing.T, cl *catalogLog, id uint64, labels model.LabelSet) {
	t.Helper()
	if err := cl.AppendRegister(id, labels); err != nil {
		t.Fatalf("AppendRegister(%d): %v", id, err)
	}
}

// silence unused-import warnings when only some tests build.
var _ = strings.Builder{}
