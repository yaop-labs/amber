package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/yaop-labs/amber/internal/otlpv4"
)

func main() {
	os.Exit(runMain())
}

func runMain() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func run(ctx context.Context, args []string, output io.Writer) error {
	if len(args) != 3 || args[0] != "otlp-v4" {
		return errors.New("usage: amber-migrate otlp-v4 <source-root> <target-root>")
	}
	result, err := otlpv4.MigrateLegacyV3(ctx, args[1], args[2])
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(output)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("encode migration result: %w", err)
	}
	return nil
}
