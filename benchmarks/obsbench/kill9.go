package obsbench

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// Kill9Options drives the durability-loss test (METHODOLOGY.md): steady
// ingest, SIGKILL the server at a random point, restart, and compare the
// post-recovery real-query ID count against what the server acknowledged.
type Kill9Options struct {
	// ServerCmd starts the system under test; the harness owns the process
	// (its own process group) so the kill hits the whole tree.
	ServerCmd []string
	// ReadyURL is polled for HTTP 200 after each start.
	ReadyURL string
	// DataDir is wiped before each run so runs are independent.
	DataDir string
	// ServerLogDir receives per-run server stdout/stderr captures.
	ServerLogDir string

	Target      TargetConfig
	TargetName  string
	DatasetPath string
	Ingest      IngestOptions

	Runs int
	// Kill point is uniform in [KillAfterMin, KillAfterMax] from ingest start.
	KillAfterMin time.Duration
	KillAfterMax time.Duration
	Seed         uint64

	Logf func(format string, args ...any)
}

// Kill9RunResult is one run's outcome. LostAcked is the lower bound on
// acked-but-lost records: in-flight unacked batches that happened to land
// can mask up to InFlightBound losses, which is why the bound is published
// alongside the number instead of being silently ignored.
type Kill9RunResult struct {
	Run          int           `json:"run"`
	KillAfter    time.Duration `json:"kill_after_ns"`
	Acked        uint64        `json:"acked"`
	Queryable    uint64        `json:"queryable"`
	LostAcked    int64         `json:"lost_acked_lower_bound"`
	InFlightMax  uint64        `json:"in_flight_bound"`
	RecoverError string        `json:"recover_error,omitempty"`
}

type Kill9Result struct {
	Target string           `json:"target"`
	Runs   []Kill9RunResult `json:"runs"`
	// WorstLost is the headline number: max lower-bound loss across runs.
	WorstLost int64 `json:"worst_lost_acked"`
}

func (o *Kill9Options) logf(format string, args ...any) {
	if o.Logf != nil {
		o.Logf(format, args...)
	}
}

// RunKill9 executes opts.Runs independent kill cycles.
func RunKill9(ctx context.Context, opts Kill9Options) (Kill9Result, error) {
	if err := validateWipeDir(opts.DataDir); err != nil {
		return Kill9Result{}, err
	}
	if opts.Runs <= 0 {
		opts.Runs = 5
	}
	if opts.KillAfterMin <= 0 {
		opts.KillAfterMin = 5 * time.Second
	}
	if opts.KillAfterMax < opts.KillAfterMin {
		opts.KillAfterMax = opts.KillAfterMin
	}
	rng := rand.New(rand.NewPCG(opts.Seed, 0xdead))

	out := Kill9Result{Target: opts.TargetName}
	for run := 1; run <= opts.Runs; run++ {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		rr, err := runKill9Once(ctx, opts, run, rng)
		if err != nil {
			return out, fmt.Errorf("run %d: %w", run, err)
		}
		out.Runs = append(out.Runs, rr)
		if rr.LostAcked > out.WorstLost {
			out.WorstLost = rr.LostAcked
		}
		opts.logf("run %d: killed at %s, acked=%d queryable=%d lost>=%d",
			run, rr.KillAfter.Round(time.Millisecond), rr.Acked, rr.Queryable, rr.LostAcked)
	}
	return out, nil
}

func runKill9Once(ctx context.Context, opts Kill9Options, run int, rng *rand.Rand) (Kill9RunResult, error) {
	if err := os.RemoveAll(opts.DataDir); err != nil {
		return Kill9RunResult{}, fmt.Errorf("wipe data dir: %w", err)
	}

	srv, err := startServer(opts, fmt.Sprintf("run%d-a", run))
	if err != nil {
		return Kill9RunResult{}, err
	}
	if err := waitReady(ctx, opts.ReadyURL, 60*time.Second); err != nil {
		srv.stop()
		return Kill9RunResult{}, fmt.Errorf("server not ready: %w", err)
	}

	killAfter := opts.KillAfterMin
	if span := opts.KillAfterMax - opts.KillAfterMin; span > 0 {
		killAfter += time.Duration(rng.Int64N(int64(span)))
	}

	// Ingest runs in the background; the kill makes it start failing, then
	// the context cancel stops the read loop and yields the acked total.
	ingestCtx, cancelIngest := context.WithCancel(ctx)
	resCh := make(chan IngestResult, 1)
	errCh := make(chan error, 1)
	go func() {
		res, err := Ingest(ingestCtx, opts.TargetName, opts.Target, opts.DatasetPath, opts.Ingest)
		if err != nil {
			errCh <- err
			return
		}
		resCh <- res
	}()

	select {
	case <-time.After(killAfter):
	case err := <-errCh:
		cancelIngest()
		srv.stop()
		return Kill9RunResult{}, fmt.Errorf("ingest failed before kill: %w", err)
	case res := <-resCh:
		// Dataset ran out before the kill point - the run is void; the
		// caller should use a larger dataset or an earlier kill window.
		cancelIngest()
		srv.stop()
		return Kill9RunResult{}, fmt.Errorf("ingest finished (%d acked) before kill at %s; enlarge dataset or shorten kill window",
			res.AckedRecords, killAfter)
	}

	if err := srv.kill9(); err != nil {
		cancelIngest()
		return Kill9RunResult{}, fmt.Errorf("kill: %w", err)
	}
	// Let in-flight requests fail against the dead server before stopping
	// the loadgen, so "acked" can no longer grow.
	time.Sleep(500 * time.Millisecond)
	cancelIngest()

	var acked uint64
	select {
	case res := <-resCh:
		acked = res.AckedRecords
	case err := <-errCh:
		return Kill9RunResult{}, fmt.Errorf("ingest: %w", err)
	case <-time.After(30 * time.Second):
		return Kill9RunResult{}, fmt.Errorf("ingest did not stop after cancel")
	}

	rr := Kill9RunResult{
		Run:         run,
		KillAfter:   killAfter,
		Acked:       acked,
		InFlightMax: uint64(opts.Ingest.Workers) * uint64(opts.Ingest.BatchSize),
	}

	srv2, err := startServer(opts, fmt.Sprintf("run%d-b", run))
	if err != nil {
		return rr, fmt.Errorf("restart: %w", err)
	}
	defer func() { srv2.stop() }()
	if err := waitReady(ctx, opts.ReadyURL, 120*time.Second); err != nil {
		rr.RecoverError = err.Error()
		return rr, nil
	}

	queryable, err := CountQueryable(ctx, opts.Target)
	if err != nil {
		rr.RecoverError = err.Error()
		return rr, nil
	}
	rr.Queryable = queryable
	if acked > queryable {
		rr.LostAcked = int64(acked - queryable)
	}
	return rr, nil
}

// validateWipeDir refuses data dirs that look like someone's real path: the
// harness recursively deletes it every run.
func validateWipeDir(dir string) error {
	if dir == "" {
		return fmt.Errorf("kill9: data dir is required")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	if strings.Count(abs, string(filepath.Separator)) < 2 {
		return fmt.Errorf("kill9: refusing to wipe %s (path too shallow)", abs)
	}
	return nil
}

type serverProc struct {
	cmd *exec.Cmd
	log *os.File
}

func startServer(opts Kill9Options, label string) (*serverProc, error) {
	if len(opts.ServerCmd) == 0 {
		return nil, fmt.Errorf("kill9: server command is required")
	}
	cmd := exec.Command(opts.ServerCmd[0], opts.ServerCmd[1:]...)
	// Own process group: SIGKILL must hit the whole tree, not just the
	// shell or wrapper at the top.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	var logFile *os.File
	if opts.ServerLogDir != "" {
		if err := os.MkdirAll(opts.ServerLogDir, 0o755); err != nil {
			return nil, err
		}
		f, err := os.Create(filepath.Join(opts.ServerLogDir, "server-"+label+".log"))
		if err != nil {
			return nil, err
		}
		logFile = f
		cmd.Stdout = f
		cmd.Stderr = f
	}

	if err := cmd.Start(); err != nil {
		if logFile != nil {
			_ = logFile.Close()
		}
		return nil, fmt.Errorf("start server: %w", err)
	}
	return &serverProc{cmd: cmd, log: logFile}, nil
}

func (s *serverProc) kill9() error {
	err := syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
	_, _ = s.cmd.Process.Wait()
	if s.log != nil {
		_ = s.log.Close()
	}
	return err
}

// stop shuts the server down gracefully, escalating to SIGKILL.
func (s *serverProc) stop() {
	_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		_, _ = s.cmd.Process.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		_ = syscall.Kill(-s.cmd.Process.Pid, syscall.SIGKILL)
		<-done
	}
	if s.log != nil {
		_ = s.log.Close()
	}
}

func waitReady(ctx context.Context, readyURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		resp, err := client.Get(readyURL)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("not ready after %s", timeout)
}
