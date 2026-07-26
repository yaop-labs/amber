// Package gyreadapter maps Amber's storage runtime to the shared Gyre
// operational component contract. Network listeners remain separate adapters;
// this component owns the data dependency and its bounded shutdown.
package gyreadapter

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/yaop-labs/amber/internal/runtime"
	"github.com/yaop-labs/gyre"
)

const stackVersion = "0.4.0"

// StackComponent exposes an already-open Amber stack to a Gyre Runtime. The
// standalone binary constructs the stack before registering components, so
// Start is intentionally idempotent and only validates ownership.
type StackComponent struct {
	stack      *runtime.Stack
	generation atomic.Uint64
}

func NewStackComponent(stack *runtime.Stack) *StackComponent {
	return &StackComponent{stack: stack}
}

func (c *StackComponent) Name() string    { return "amber-storage" }
func (c *StackComponent) Version() string { return stackVersion }

func (c *StackComponent) Start(context.Context) error {
	if c == nil || c.stack == nil {
		return gyre.E(gyre.CodeInternal, "amber-storage", "start", false, errors.New("stack is nil"))
	}
	return nil
}

func (c *StackComponent) Ready(ctx context.Context) error {
	if c == nil || c.stack == nil {
		return gyre.E(gyre.CodeDependency, "amber-storage", "ready", true, errors.New("stack is nil"))
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !c.stack.IsReady() {
		return gyre.E(gyre.CodeDependency, "amber-storage", "ready", true, errors.New("sealed indexes are loading"))
	}
	return nil
}

func (c *StackComponent) Status() gyre.Snapshot {
	now := time.Now().UTC()
	snapshot := gyre.Snapshot{Name: c.Name(), Version: c.Version(), Since: now}
	if c == nil || c.stack == nil {
		snapshot.State = gyre.StateFailed
		snapshot.Conditions = []gyre.Condition{{Type: "available", Status: false, Reason: "nil_stack", LastTransition: now}}
		return snapshot
	}
	status := c.stack.Status()
	snapshot.State = gyre.StateReady
	if status.Closing {
		snapshot.State = gyre.StateStopping
	} else if status.Degraded {
		snapshot.State = gyre.StateDegraded
	} else if !status.Ready {
		snapshot.State = gyre.StateStarting
	}
	snapshot.Conditions = []gyre.Condition{
		{Type: "ready", Status: status.Ready, Reason: reasons(status), LastTransition: now},
		{Type: "degraded", Status: status.Degraded, Reason: reasons(status), LastTransition: now},
	}
	return snapshot
}

func (c *StackComponent) Close(ctx context.Context) error {
	if c == nil || c.stack == nil {
		return nil
	}
	return c.stack.Close(ctx)
}

// Reload validates the Gyre envelope and records its generation. Storage
// topology changes are deliberately reported as restart-required: silently
// applying them would invalidate open WAL/index handles.
func (c *StackComponent) Reload(ctx context.Context, envelope gyre.Envelope) (gyre.ReloadResult, error) {
	if err := envelope.Validate(); err != nil {
		return gyre.ReloadResult{}, err
	}
	if envelope.Kind != "AmberStorage" {
		return gyre.ReloadResult{}, gyre.E(gyre.CodeConfigInvalid, envelope.Kind, "reload", false, errors.New("unsupported component kind"))
	}
	if err := ctx.Err(); err != nil {
		return gyre.ReloadResult{}, err
	}
	c.generation.Store(envelope.Generation)
	return gyre.ReloadResult{Generation: envelope.Generation, RestartRequired: []string{"storage"}}, nil
}

func reasons(status runtime.Status) string {
	if len(status.Reasons) == 0 {
		return "none"
	}
	return status.Reasons[0].Code
}
