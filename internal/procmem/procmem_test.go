package procmem

import "testing"

func TestVmRSSBytes(t *testing.T) {
	tests := []struct {
		name   string
		status string
		want   int64
		ok     bool
	}{
		{"typical", "VmPeak:\t 100 kB\nVmRSS:\t  2048 kB\nVmData:\t 50 kB\n", 2048 << 10, true},
		{"first line", "VmRSS:\t512 kB\n", 512 << 10, true},
		{"no trailing newline", "VmRSS:\t7 kB", 7 << 10, true},
		{"absent", "VmPeak:\t100 kB\nVmSize:\t200 kB\n", 0, false},
		{"malformed value", "VmRSS:\tnotanumber kB\n", 0, false},
		{"missing value", "VmRSS:\n", 0, false},
		{"empty", "", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := VmRSSBytes(tt.status)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("VmRSSBytes(%q) = (%d, %v), want (%d, %v)", tt.status, got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestSelfVmRSS(t *testing.T) {
	// Succeeds on linux (this process has a VmRSS); errors where /proc is
	// absent. Both are acceptable, but a success must be a positive size.
	rss, err := SelfVmRSS()
	if err != nil {
		t.Skipf("SelfVmRSS unavailable on this platform: %v", err)
	}
	if rss <= 0 {
		t.Fatalf("SelfVmRSS = %d, want > 0", rss)
	}
}
