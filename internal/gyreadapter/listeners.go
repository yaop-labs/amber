package gyreadapter

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/yaop-labs/gyre"
	"google.golang.org/grpc"
)

const listenerVersion = "0.4.0-dev"

// HTTPComponent owns one already-materialized HTTP handler and binds its
// listener synchronously in Start. Serve errors are retained in Status.
type HTTPComponent struct {
	server *http.Server
	addr   string
	tls    *tls.Config

	mu       sync.Mutex
	listener net.Listener
	serveErr error
	started  bool
	closed   bool
}

func NewHTTPComponent(addr string, server *http.Server, tlsConfig *tls.Config) *HTTPComponent {
	return &HTTPComponent{addr: addr, server: server, tls: tlsConfig}
}

func (c *HTTPComponent) Name() string    { return "amber-http" }
func (c *HTTPComponent) Version() string { return listenerVersion }

func (c *HTTPComponent) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	if c.server == nil {
		return errors.New("gyreadapter: HTTP server is nil")
	}
	listener, err := net.Listen("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("gyreadapter: bind HTTP %s: %w", c.addr, err)
	}
	if c.tls != nil {
		listener = tls.NewListener(listener, c.tls)
	}
	c.listener = listener
	c.server.Addr = c.addr
	c.started = true
	go func() {
		err := c.server.Serve(listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			c.mu.Lock()
			c.serveErr = err
			c.mu.Unlock()
		}
	}()
	return nil
}

func (c *HTTPComponent) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.serveErr != nil {
		return c.serveErr
	}
	if !c.started || c.closed || c.listener == nil {
		return errors.New("HTTP listener is not serving")
	}
	return nil
}

func (c *HTTPComponent) Status() gyre.Snapshot {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()
	state := gyre.StateStarting
	ready := c.started && !c.closed && c.listener != nil && c.serveErr == nil
	if c.closed {
		state = gyre.StateStopped
	} else if c.serveErr != nil {
		state = gyre.StateFailed
	} else if ready {
		state = gyre.StateReady
	}
	return gyre.Snapshot{Name: c.Name(), Version: c.Version(), State: state, Since: now,
		Conditions: []gyre.Condition{{Type: "ready", Status: ready, LastTransition: now}}}
}

func (c *HTTPComponent) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	if c.server == nil {
		return nil
	}
	if err := c.server.Shutdown(ctx); err != nil {
		_ = c.server.Close()
		return err
	}
	return nil
}

// GRPCComponent owns one gRPC server and performs bounded graceful shutdown,
// falling back to Stop when the caller's deadline expires.
type GRPCComponent struct {
	server *grpc.Server
	addr   string

	mu       sync.Mutex
	listener net.Listener
	serveErr error
	started  bool
	closed   bool
}

func NewGRPCComponent(addr string, server *grpc.Server) *GRPCComponent {
	return &GRPCComponent{addr: addr, server: server}
}

func (c *GRPCComponent) Name() string    { return "amber-grpc" }
func (c *GRPCComponent) Version() string { return listenerVersion }

func (c *GRPCComponent) Start(context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.started {
		return nil
	}
	if c.server == nil {
		return errors.New("gyreadapter: gRPC server is nil")
	}
	listener, err := net.Listen("tcp", c.addr)
	if err != nil {
		return fmt.Errorf("gyreadapter: bind gRPC %s: %w", c.addr, err)
	}
	c.listener = listener
	c.started = true
	go func() {
		err := c.server.Serve(listener)
		if err != nil {
			c.mu.Lock()
			c.serveErr = err
			c.mu.Unlock()
		}
	}()
	return nil
}

func (c *GRPCComponent) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.serveErr != nil || !c.started || c.closed || c.listener == nil {
		return errors.New("gRPC listener is not serving")
	}
	return nil
}

func (c *GRPCComponent) Status() gyre.Snapshot {
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()
	state := gyre.StateStarting
	ready := c.started && !c.closed && c.listener != nil && c.serveErr == nil
	if c.closed {
		state = gyre.StateStopped
	} else if c.serveErr != nil {
		state = gyre.StateFailed
	} else if ready {
		state = gyre.StateReady
	}
	return gyre.Snapshot{Name: c.Name(), Version: c.Version(), State: state, Since: now,
		Conditions: []gyre.Condition{{Type: "ready", Status: ready, LastTransition: now}}}
}

func (c *GRPCComponent) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	if c.server == nil {
		return nil
	}
	done := make(chan struct{})
	go func() {
		c.server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		c.server.Stop()
		return ctx.Err()
	}
}
