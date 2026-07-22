package gyreadapter

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/yaop-labs/gyre"
	"google.golang.org/grpc"
)

func TestHTTPComponentBindsSynchronouslyAndCloses(t *testing.T) {
	component := NewHTTPComponent("127.0.0.1:0", &http.Server{Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})}, nil)
	if err := component.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	if err := component.Ready(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := component.Status().State; got != gyre.StateStopped {
		t.Fatalf("state after close = %q", got)
	}
}

func TestHTTPComponentReadinessTransitions(t *testing.T) {
	component := NewHTTPComponent("127.0.0.1:0", &http.Server{}, nil)
	if err := component.Ready(context.Background()); err == nil {
		t.Fatal("unstarted HTTP component reported ready")
	}
	if err := component.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	if err := component.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Ready(context.Background()); err == nil {
		t.Fatal("closed HTTP component reported ready")
	}
}

func TestHTTPComponentServeFailureTurnsReadinessRed(t *testing.T) {
	component := NewHTTPComponent("127.0.0.1:0", &http.Server{}, nil)
	if err := component.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	// Inject the same failure mode as an externally closed listener or a
	// process-level socket failure. Serve records the error asynchronously.
	if err := component.listener.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := component.Ready(context.Background()); err != nil {
			if component.Status().State != gyre.StateFailed {
				t.Fatalf("state after serve failure = %q, want failed", component.Status().State)
			}
			_ = component.Close(context.Background())
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("HTTP component did not report injected serve failure")
}

func TestListenerBindFailureIsReturnedFromStart(t *testing.T) {
	first := NewHTTPComponent("127.0.0.1:0", &http.Server{}, nil)
	if err := first.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	addr := first.listener.Addr().String()
	defer func() { _ = first.Close(context.Background()) }()
	second := NewHTTPComponent(addr, &http.Server{}, nil)
	if err := second.Start(context.Background()); err == nil {
		t.Fatal("expected bind failure")
	}
}

func TestGRPCComponentGracefulCloseHonorsContext(t *testing.T) {
	component := NewGRPCComponent("127.0.0.1:0", grpc.NewServer())
	if err := component.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := component.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestGRPCComponentReadinessTransitions(t *testing.T) {
	component := NewGRPCComponent("127.0.0.1:0", grpc.NewServer())
	if err := component.Ready(context.Background()); err == nil {
		t.Fatal("unstarted gRPC component reported ready")
	}
	if err := component.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	if err := component.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := component.Ready(context.Background()); err == nil {
		t.Fatal("closed gRPC component reported ready")
	}
}

func TestGRPCComponentServeFailureTurnsReadinessRed(t *testing.T) {
	component := NewGRPCComponent("127.0.0.1:0", grpc.NewServer())
	if err := component.Start(context.Background()); err != nil {
		skipIfNetworkDenied(t, err)
		t.Fatal(err)
	}
	if err := component.listener.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if err := component.Ready(context.Background()); err != nil {
			if component.Status().State != gyre.StateFailed {
				t.Fatalf("state after gRPC serve failure = %q, want failed", component.Status().State)
			}
			_ = component.Close(context.Background())
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("gRPC component did not report injected serve failure")
}

func skipIfNetworkDenied(t *testing.T, err error) {
	t.Helper()
	if strings.Contains(err.Error(), "operation not permitted") {
		t.Skip("sandbox denies loopback listeners")
	}
}
