package client

import (
	"context"
	"strings"
	"testing"

	"github.com/yaop-labs/reef/bearer"
	"github.com/yaop-labs/reef/tlsconf"
)

func TestWithEdgeConfigFailsClosedOnInvalidTLSFiles(t *testing.T) {
	c := New("https://amber.example",
		WithEdgeConfig(EdgeConfig{
			TLS:  tlsconf.ClientConfig{Enabled: true, CAFile: "/does/not/exist/ca.pem"},
			Auth: bearer.ClientConfig{Token: "secret"},
		}),
	)
	_, err := c.Stats(context.Background())
	if err == nil || !strings.Contains(err.Error(), "configure Reef edge") {
		t.Fatalf("Stats error = %v, want fail-closed Reef configuration error", err)
	}
}
