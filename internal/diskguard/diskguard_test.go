package diskguard

import (
	"errors"
	"testing"
)

func TestGuardWatermarksAndAdmission(t *testing.T) {
	var free int64 = 300
	guard, err := newWithProbe("/data", Config{
		WarningFreeBytes: 200,
		StopFreeBytes:    100,
	}, func(string) (int64, int64, error) {
		return free, 1000, nil
	})
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}

	if err := guard.Admit(); err != nil {
		t.Fatalf("healthy admission: %v", err)
	}
	free = 150
	if snapshot := guard.Sample(); !snapshot.Warning || snapshot.Stopped {
		t.Fatalf("warning snapshot = %+v", snapshot)
	}
	if err := guard.Admit(); err != nil {
		t.Fatalf("warning admission: %v", err)
	}
	free = 100
	if err := guard.Admit(); !errors.Is(err, ErrLowSpace) {
		t.Fatalf("stop admission error = %v", err)
	}
	if stats := guard.Stats(); stats.Rejected != 1 {
		t.Fatalf("rejected = %d, want 1", stats.Rejected)
	}
}

func TestGuardProbeFailureFailsClosed(t *testing.T) {
	guard, err := newWithProbe("/data", Config{
		WarningFreeBytes: 200,
		StopFreeBytes:    100,
	}, func(string) (int64, int64, error) {
		return 0, 0, errors.New("stat failed")
	})
	if err != nil {
		t.Fatalf("new guard: %v", err)
	}

	if err := guard.Admit(); !errors.Is(err, ErrProbe) {
		t.Fatalf("admission error = %v", err)
	}
	stats := guard.Stats()
	if stats.ProbeFailures != 1 || stats.Rejected != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestGuardRejectsInvalidWatermarks(t *testing.T) {
	for _, cfg := range []Config{
		{},
		{WarningFreeBytes: 99, StopFreeBytes: 100},
	} {
		if _, err := newWithProbe("/data", cfg, statFS); err == nil {
			t.Fatalf("New(%+v) succeeded", cfg)
		}
	}
}
