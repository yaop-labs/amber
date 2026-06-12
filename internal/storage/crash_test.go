package storage

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// crashWriterEnv is set in the subprocess that writes records until killed.
// Its value is the data directory path.
const crashWriterEnv = "AMBER_CRASH_WRITER_DIR"

// crashWriterMaxRecEnv overrides the writer's rotation threshold. Small
// values force a rotation every few hundred milliseconds, so a random kill
// lands inside the seal/saveMeta/WAL-truncate window with high probability.
const crashWriterMaxRecEnv = "AMBER_CRASH_WRITER_MAXREC"

// markerFile is written atomically (write+rename) after every successful WAL
// sync. It records how many records are guaranteed durable at subprocess death.
const markerFile = "durable_count"

// TestMain detects subprocess mode (crashWriterEnv set) and runs the writer.
// Otherwise runs all tests normally.
func TestMain(m *testing.M) {
	if dir := os.Getenv(crashWriterEnv); dir != "" {
		runCrashWriter(dir)
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runCrashWriter opens a SegmentManager and writes records until killed.
// After each successful Write(), it atomically updates markerFile with the
// current durable count. The parent reads this file after SIGKILL.
func runCrashWriter(dir string) {
	maxRecords := uint64(10_000)
	if v := os.Getenv(crashWriterMaxRecEnv); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			fmt.Fprintf(os.Stderr, "crash writer: bad %s=%q: %v\n", crashWriterMaxRecEnv, v, err)
			os.Exit(1)
		}
		maxRecords = n
	}
	sm, err := OpenSegmentManager(dir, RotationPolicy{
		MaxRecords: maxRecords,
		MaxBytes:   0,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "crash writer: open: %v\n", err)
		os.Exit(1)
	}

	base := time.Now().UnixNano()
	for i := 1; ; i++ {
		data := fmt.Appendf(nil, "crash-rec-%08d", i)
		ts := base + int64(i)*int64(time.Microsecond)
		if err := sm.Write(data, ts); err != nil {
			// Write returns error only if WAL fsync failed — that record is
			// not durable. Stop here so the marker stays honest.
			fmt.Fprintf(os.Stderr, "crash writer: write %d: %v\n", i, err)
			return
		}
		// Write succeeded — record is in WAL and fsync'd. Update marker.
		writeMarker(dir, i)
	}
}

func writeMarker(dir string, count int) {
	tmp := filepath.Join(dir, markerFile+".tmp")
	final := filepath.Join(dir, markerFile)

	data, _ := json.Marshal(count)
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return
	}
	// Rename is atomic on Linux: marker is always a valid count.
	_ = os.Rename(tmp, final)
}

func readMarker(dir string) (int, error) {
	data, err := os.ReadFile(filepath.Join(dir, markerFile))
	if err != nil {
		return 0, err
	}
	var n int
	if err := json.Unmarshal(data, &n); err != nil {
		return 0, fmt.Errorf("parse marker: %w", err)
	}
	return n, nil
}

// TestSegmentManager_CrashDurability spawns itself as a crash writer subprocess,
// SIGKILLs it after a short interval, then reopens the SegmentManager and
// verifies that every record the subprocess confirmed durable (via markerFile)
// is present exactly once — no loss, no duplicates.
func TestSegmentManager_CrashDurability(t *testing.T) {
	if testing.Short() {
		t.Skip("crash durability test skipped in short mode")
	}

	dir := t.TempDir()

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	// Spawn self in crash-writer mode. -test.run limits to this test's helper
	// so TestMain exits after runCrashWriter returns (or is killed).
	cmd := exec.Command(exe, "-test.run=^TestSegmentManager_CrashDurability$")
	cmd.Env = append(os.Environ(), crashWriterEnv+"="+dir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start subprocess: %v", err)
	}

	// Give it time to write a meaningful number of records.
	time.Sleep(300 * time.Millisecond)

	// SIGKILL — no cleanup, no deferred Close.
	if err := cmd.Process.Kill(); err != nil {
		t.Fatalf("kill subprocess: %v", err)
	}
	_ = cmd.Wait()

	verifyCrashDurability(t, dir)
}

// verifyCrashDurability recovers the killed writer's data dir and checks the
// core invariant: every record the subprocess confirmed durable (markerFile)
// is present exactly once — no loss, no duplicates.
func verifyCrashDurability(t *testing.T, dir string) {
	t.Helper()

	durableCount, err := readMarker(dir)
	if err != nil {
		t.Fatalf("read durable marker: %v (subprocess may not have written any records)", err)
	}
	if durableCount == 0 {
		t.Fatal("marker is 0 — subprocess was killed before writing anything")
	}
	t.Logf("subprocess confirmed %d durable records before SIGKILL", durableCount)

	// Reopen and force-seal the active segment so we can scan everything.
	sm, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	if err := sm.Rotate(); err != nil {
		t.Fatalf("rotate after recovery: %v", err)
	}
	if err := sm.Close(); err != nil {
		t.Fatalf("close after recovery: %v", err)
	}

	// Scan all sealed segments.
	sm2, err := OpenSegmentManager(dir, DefaultRotationPolicy)
	if err != nil {
		t.Fatalf("final reopen: %v", err)
	}
	defer sm2.Close()

	seen := make(map[int]int, durableCount)
	for _, seg := range sm2.Segments() {
		t.Logf("segment %s: records=%d sealed=%v", seg.FileName, seg.RecordCount, seg.Sealed)
		if seg.RecordCount == 0 {
			// Empty trailing segment created by Rotate() before Close() — no user data.
			continue
		}
		path := filepath.Join(dir, seg.FileName)
		sr, err := OpenSegmentReader(path, nil)
		if err != nil {
			t.Fatalf("open reader %s: %v", seg.FileName, err)
		}
		scanErr := sr.Scan(func(data []byte) error {
			// payload format: "crash-rec-XXXXXXXX"
			const prefix = "crash-rec-"
			if len(data) <= len(prefix) {
				return nil
			}
			n, err := strconv.Atoi(string(data[len(prefix):]))
			if err != nil {
				return nil
			}
			seen[n]++
			return nil
		})
		_ = sr.Close()
		if scanErr != nil {
			t.Fatalf("scan %s: %v", seg.FileName, scanErr)
		}
	}

	// Every record 1..durableCount must appear exactly once.
	lost, duped := 0, 0
	for i := 1; i <= durableCount; i++ {
		switch seen[i] {
		case 0:
			lost++
			if lost <= 5 {
				t.Errorf("record %d lost", i)
			}
		case 1:
			// OK
		default:
			duped++
			if duped <= 5 {
				t.Errorf("record %d duplicated ×%d", i, seen[i])
			}
		}
	}
	if lost > 5 {
		t.Errorf("... and %d more lost records", lost-5)
	}
	if duped > 5 {
		t.Errorf("... and %d more duplicated records", duped-5)
	}

	t.Logf("result: %d durable, %d recovered, %d lost, %d duped",
		durableCount, len(seen), lost, duped)
}

// TestSegmentManager_CrashDurability_RotationStorm kills the writer while it
// rotates every 100 records, so SIGKILL lands inside the seal protocol —
// footer write, saveMeta, WAL truncate, createNewSegment — instead of the
// steady write loop. Several kills at randomized offsets sample different
// points of that window; the invariant is the same: no loss, no duplicates.
func TestSegmentManager_CrashDurability_RotationStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("crash durability test skipped in short mode")
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}

	for iter := range 3 {
		t.Run(fmt.Sprintf("kill_%d", iter), func(t *testing.T) {
			dir := t.TempDir()

			cmd := exec.Command(exe, "-test.run=^TestSegmentManager_CrashDurability_RotationStorm$")
			cmd.Env = append(os.Environ(),
				crashWriterEnv+"="+dir,
				crashWriterMaxRecEnv+"=100",
			)
			cmd.Stdout = os.Stdout
			cmd.Stderr = os.Stderr
			if err := cmd.Start(); err != nil {
				t.Fatalf("start subprocess: %v", err)
			}

			sleep := time.Duration(150+rand.IntN(250)) * time.Millisecond
			t.Logf("killing writer after %v", sleep)
			time.Sleep(sleep)

			if err := cmd.Process.Kill(); err != nil {
				t.Fatalf("kill subprocess: %v", err)
			}
			_ = cmd.Wait()

			verifyCrashDurability(t, dir)
		})
	}
}
