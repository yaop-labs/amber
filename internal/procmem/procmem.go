// Package procmem parses resident-set-size (VmRSS) from Linux /proc status
// files. It is the single home for the VmRSS parsing that the bench tools and
// the metric store's RSS test previously each reimplemented. On non-linux
// hosts the readers simply fail (no /proc); callers treat that as "unknown".
package procmem

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

// VmRSSBytes parses the VmRSS line (reported in kB) from the contents of a
// /proc/<pid>/status file and returns it in bytes. ok is false when the field
// is absent or malformed.
func VmRSSBytes(status string) (int64, bool) {
	for line := range strings.SplitSeq(status, "\n") {
		if !strings.HasPrefix(line, "VmRSS:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0, false
		}
		kb, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return 0, false
		}
		return kb << 10, true
	}
	return 0, false
}

// SelfVmRSS returns this process's resident set size in bytes, read from
// /proc/self/status.
func SelfVmRSS() (int64, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0, err
	}
	rss, ok := VmRSSBytes(string(data))
	if !ok {
		return 0, errors.New("procmem: VmRSS not found in /proc/self/status")
	}
	return rss, nil
}

// ProcessVmRSS scans /proc for the first process whose cmdline contains match
// and returns its resident set size in bytes. ok is false when no such process
// is found or its VmRSS cannot be read. Used by the bench tools to sample an
// external server's memory.
func ProcessVmRSS(match string) (int64, bool) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return 0, false
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		if !strings.Contains(string(cmdline), match) {
			continue
		}
		status, err := os.ReadFile("/proc/" + e.Name() + "/status")
		if err != nil {
			continue
		}
		if rss, ok := VmRSSBytes(string(status)); ok {
			return rss, true
		}
	}
	return 0, false
}
