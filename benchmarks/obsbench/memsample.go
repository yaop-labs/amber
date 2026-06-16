package obsbench

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// MemSampler externally samples RSS/PSS of matching processes at a fixed
// interval via /proc/<pid>/smaps_rollup. External sampling (not the
// runtime's own stats) is the methodology's defense against self-flattering
// memory numbers (METHODOLOGY.md §3). PIDs are re-resolved every tick so the
// sampler survives restarts and the kill -9 durability test.
type MemSampler struct {
	Match    *regexp.Regexp // against /proc/<pid>/cmdline
	Interval time.Duration
	Out      io.Writer
}

type memSample struct {
	pid   int
	comm  string
	rssKB uint64
	pssKB uint64
}

// Run samples until ctx is canceled. CSV columns:
// unix_ms,pid,comm,rss_kb,pss_kb — plus one "total" row per tick so stack
// scenarios (several processes) aggregate without post-processing.
func (s *MemSampler) Run(ctx context.Context) error {
	w := bufio.NewWriter(s.Out)
	defer w.Flush()
	if _, err := fmt.Fprintln(w, "unix_ms,pid,comm,rss_kb,pss_kb"); err != nil {
		return err
	}

	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return w.Flush()
		case now := <-ticker.C:
			samples := collectSamples(s.Match)
			ms := now.UnixMilli()
			var totalRSS, totalPSS uint64
			for _, sm := range samples {
				totalRSS += sm.rssKB
				totalPSS += sm.pssKB
				fmt.Fprintf(w, "%d,%d,%s,%d,%d\n", ms, sm.pid, sm.comm, sm.rssKB, sm.pssKB)
			}
			fmt.Fprintf(w, "%d,0,total,%d,%d\n", ms, totalRSS, totalPSS)
			if err := w.Flush(); err != nil {
				return err
			}
		}
	}
}

func collectSamples(match *regexp.Regexp) []memSample {
	procs, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	out := make([]memSample, 0, len(procs))
	self := os.Getpid()
	for _, p := range procs {
		pid, err := strconv.Atoi(p.Name())
		if err != nil || pid == self {
			continue
		}
		cmdline, err := os.ReadFile(filepath.Join("/proc", p.Name(), "cmdline"))
		if err != nil || len(cmdline) == 0 {
			continue
		}
		cmd := strings.ReplaceAll(string(cmdline), "\x00", " ")
		if !match.MatchString(cmd) {
			continue
		}
		rss, pss, err := readSmapsRollup(pid)
		if err != nil {
			continue // process exited between scan and read
		}
		comm, _ := os.ReadFile(filepath.Join("/proc", p.Name(), "comm"))
		out = append(out, memSample{
			pid:   pid,
			comm:  strings.TrimSpace(string(comm)),
			rssKB: rss,
			pssKB: pss,
		})
	}
	return out
}

func readSmapsRollup(pid int) (rssKB, pssKB uint64, err error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/smaps_rollup", pid))
	if err != nil {
		return 0, 0, err
	}
	for line := range strings.Lines(string(data)) {
		switch {
		case strings.HasPrefix(line, "Rss:"):
			rssKB = parseKBLine(line)
		case strings.HasPrefix(line, "Pss:"):
			pssKB = parseKBLine(line)
		}
	}
	return rssKB, pssKB, nil
}

func parseKBLine(line string) uint64 {
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0
	}
	v, _ := strconv.ParseUint(fields[1], 10, 64)
	return v
}
