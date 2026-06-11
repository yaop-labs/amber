package obsbench

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// TestE2E_Kill9Amber drives the durability-loss orchestration against a real
// amber binary: build, start, steady ingest, SIGKILL mid-stream, restart,
// recovery count. It validates the harness, not amber's loss numbers — amber
// acks after enqueue by design, so LostAcked > 0 is an expected outcome
// here, not a failure.
func TestE2E_Kill9Amber(t *testing.T) {
	if testing.Short() {
		t.Skip("builds and kills a real amber binary")
	}

	tmp := t.TempDir()
	bin := filepath.Join(tmp, "amber")
	build := exec.Command("go", "build", "-o", bin, "github.com/yaop-labs/amber/cmd/amber")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build amber: %v\n%s", err, out)
	}

	port := freePort(t)
	dataDir := filepath.Join(tmp, "data")
	cfgPath := filepath.Join(tmp, "config.yaml")
	cfg := fmt.Sprintf("storage:\n  data_dir: %s\napi:\n  http_addr: 127.0.0.1:%d\n", dataDir, port)
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	dataset := genDataset(t, 300_000, 21)
	base := fmt.Sprintf("http://127.0.0.1:%d", port)

	res, err := RunKill9(context.Background(), Kill9Options{
		ServerCmd:    []string{bin, cfgPath},
		ReadyURL:     base + "/readyz",
		DataDir:      dataDir,
		ServerLogDir: filepath.Join(tmp, "logs"),
		Target:       TargetConfig{Kind: "amber", BaseURL: base, Protocol: "native"},
		TargetName:   "amber",
		DatasetPath:  dataset,
		// ~30k rec/s for ~10s of runway; kill lands at 2-3s.
		Ingest:       IngestOptions{Workers: 4, BatchSize: 500, Rate: 30_000},
		Runs:         1,
		KillAfterMin: 2 * time.Second,
		KillAfterMax: 3 * time.Second,
		Seed:         1,
		Logf:         t.Logf,
	})
	if err != nil {
		t.Fatalf("RunKill9: %v", err)
	}

	if len(res.Runs) != 1 {
		t.Fatalf("got %d runs, want 1", len(res.Runs))
	}
	rr := res.Runs[0]
	if rr.RecoverError != "" {
		t.Fatalf("recovery failed: %s", rr.RecoverError)
	}
	if rr.Acked == 0 {
		t.Fatal("nothing acked before the kill — kill window or rate misconfigured")
	}
	if rr.Queryable == 0 {
		t.Fatal("nothing queryable after restart — recovery produced an empty store")
	}
	// Loss accounting must be internally consistent: lost = max(0, acked-queryable).
	wantLost := int64(0)
	if rr.Acked > rr.Queryable {
		wantLost = int64(rr.Acked - rr.Queryable)
	}
	if rr.LostAcked != wantLost {
		t.Fatalf("loss accounting inconsistent: acked=%d queryable=%d lost=%d", rr.Acked, rr.Queryable, rr.LostAcked)
	}
	t.Logf("kill9: acked=%d queryable=%d lost>=%d (in-flight bound %d)",
		rr.Acked, rr.Queryable, rr.LostAcked, rr.InFlightMax)
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}
