package gyreadapter

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/yaop-labs/gyre"
)

func TestStackComponentConformsWithoutStartingStorage(t *testing.T) {
	component := NewStackComponent(nil)
	if err := gyre.ConformanceCheck(context.Background(), component); err != nil {
		t.Fatal(err)
	}
	if got := component.Status(); got.State != gyre.StateFailed {
		t.Fatalf("nil stack state = %q, want failed", got.State)
	}
}

func TestStackComponentReloadReportsRestartRequirement(t *testing.T) {
	component := NewStackComponent(nil)
	result, err := component.Reload(context.Background(), gyre.Envelope{
		APIVersion: "gyre/v1", Kind: "AmberStorage", Generation: 7,
		Spec: json.RawMessage(`{"storage":{}}`),
	})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if result.Generation != 7 || len(result.RestartRequired) != 1 || result.RestartRequired[0] != "storage" {
		t.Fatalf("reload result = %+v", result)
	}
	if _, err := component.Reload(context.Background(), gyre.Envelope{APIVersion: "gyre/v1", Kind: "Wrong", Generation: 8, Spec: json.RawMessage(`{}`)}); err == nil {
		t.Fatal("unsupported reload kind accepted")
	}
}
