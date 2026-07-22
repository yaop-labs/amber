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
	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/tlsconf"
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
	tokenFile := fs.String("token-file", os.Getenv("AMBER_TOKEN_FILE"), "bearer token file")
	caFile := fs.String("tls-ca", os.Getenv("AMBER_TLS_CA"), "custom CA file")
	certFile := fs.String("tls-cert", os.Getenv("AMBER_TLS_CERT"), "client certificate file")
	keyFile := fs.String("tls-key", os.Getenv("AMBER_TLS_KEY"), "client private key file")
	serverName := fs.String("tls-server-name", os.Getenv("AMBER_TLS_SERVER_NAME"), "TLS server name override")
	insecure := fs.Bool("insecure", os.Getenv("AMBER_INSECURE") == "1", "allow plaintext transport")
	danger := fs.Bool("danger-allow-bearer-over-plaintext", os.Getenv("AMBER_DANGER_ALLOW_BEARER_OVER_PLAINTEXT") == "1", "allow bearer credentials over plaintext")
	if err := fs.Parse(args); err != nil {
		return err
	}
	localDev := len(*addr) >= len("http://localhost") && (*addr)[:len("http://localhost")] == "http://localhost" && *apiKey == "" && *tokenFile == ""
	c := client.New(*addr, client.WithAPIKey(*apiKey), client.WithEdgeConfig(client.EdgeConfig{
		TLS:      tlsconf.ClientConfig{Enabled: *caFile != "" || *certFile != "" || *keyFile != "", CAFile: *caFile, CertFile: *certFile, KeyFile: *keyFile, ServerName: *serverName},
		Auth:     bearer.ClientConfig{Token: *apiKey, TokenFile: *tokenFile},
		Insecure: localDev || *insecure, DangerAllowBearerOverPlaintext: *danger,
	}))
	defer func() { _ = c.Close() }()
	return tui.Run(ctx, c)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
