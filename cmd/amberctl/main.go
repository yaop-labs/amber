// Command amberctl is the command-line and terminal-UI client for an amber
// instance. It speaks only amber's HTTP read API, so it works the same against
// a local dev server (the default http://localhost:8080, no auth) or a remote
// one (--addr / --api-key, or the AMBER_ADDR / AMBER_API_KEY env vars).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/yaop-labs/amber/internal/cli"
	"github.com/yaop-labs/amber/internal/client"
	"github.com/yaop-labs/amber/internal/tui"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "amberctl:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	args := os.Args[1:]
	if len(args) > 0 && args[0] == "tui" {
		return runTUI(ctx, args[1:])
	}
	return cli.Run(ctx, args, os.Stdout)
}

func runTUI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	addr := fs.String("addr", envOr("AMBER_ADDR", client.DefaultAddr), "amber server address")
	apiKey := fs.String("api-key", os.Getenv("AMBER_API_KEY"), "bearer API key")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c := client.New(*addr, client.WithAPIKey(*apiKey))
	return tui.Run(ctx, c)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
