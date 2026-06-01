package query

import (
	"testing"
	"time"
)

func TestStepMillis(t *testing.T) {
	steps, err := StepMillis(1000, 3000, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{1000, 2000, 3000}
	if len(steps) != len(want) {
		t.Fatalf("len(steps) = %d, want %d", len(steps), len(want))
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("steps[%d] = %d, want %d", i, steps[i], want[i])
		}
	}
}

func TestStepMillisRejectsInvalidInput(t *testing.T) {
	if _, err := StepMillis(0, 1000, 0); err == nil {
		t.Fatal("expected invalid step error")
	}
	if _, err := StepMillis(1000, 0, time.Second); err == nil {
		t.Fatal("expected invalid range error")
	}
}
