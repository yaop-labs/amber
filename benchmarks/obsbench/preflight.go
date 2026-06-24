package obsbench

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// PreflightResult is one environment check. Fatal failures abort the run;
// non-fatal ones are recorded into the run report so a reviewer can judge.
type PreflightResult struct {
	Name  string
	OK    bool
	Fatal bool
	Note  string
}

// Preflight enforces the workstation rules of METHODOLOGY.md: enough
// available memory and disk, swap off, performance governor. dataDir is the
// filesystem the systems under test will write to.
func Preflight(dataDir string) []PreflightResult {
	var out []PreflightResult

	availKB := meminfoKB("MemAvailable")
	const wantMemKB = 8 << 20 // 8 GiB
	out = append(out, PreflightResult{
		Name:  "mem_available>=8GiB",
		OK:    availKB >= wantMemKB,
		Fatal: true,
		Note:  fmt.Sprintf("%d MiB available", availKB/1024),
	})

	var st syscall.Statfs_t
	if err := syscall.Statfs(dataDir, &st); err == nil {
		freeB := st.Bavail * uint64(st.Bsize)
		const wantDiskB = 40 << 30 // 40 GiB
		out = append(out, PreflightResult{
			Name:  "disk_free>=40GiB",
			OK:    freeB >= wantDiskB,
			Fatal: true,
			Note:  fmt.Sprintf("%d GiB free at %s", freeB>>30, dataDir),
		})
	} else {
		out = append(out, PreflightResult{Name: "disk_free>=40GiB", OK: false, Fatal: true, Note: err.Error()})
	}

	swapKB := meminfoKB("SwapTotal")
	out = append(out, PreflightResult{
		Name:  "swap_off",
		OK:    swapKB == 0,
		Fatal: true,
		Note:  fmt.Sprintf("SwapTotal=%d MiB (zram absorbs memory pressure and corrupts RSS and tail latencies)", swapKB/1024),
	})

	gov := readTrim("/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor")
	out = append(out, PreflightResult{
		Name:  "governor=performance",
		OK:    gov == "performance",
		Fatal: false,
		Note:  "governor=" + gov,
	})

	return out
}

func meminfoKB(key string) uint64 {
	data, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for line := range strings.Lines(string(data)) {
		if rest, ok := strings.CutPrefix(line, key+":"); ok {
			fields := strings.Fields(rest)
			if len(fields) >= 1 {
				v, _ := strconv.ParseUint(fields[0], 10, 64)
				return v
			}
		}
	}
	return 0
}

func readTrim(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(data))
}
