package dbmeta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreatePersistsIdentity(t *testing.T) {
	root := t.TempDir()
	first, err := LoadOrCreate(root)
	if err != nil {
		t.Fatalf("create identity: %v", err)
	}
	second, err := LoadOrCreate(root)
	if err != nil {
		t.Fatalf("reload identity: %v", err)
	}
	if first != second {
		t.Fatalf("identity changed: first=%+v second=%+v", first, second)
	}
	if first.FormatVersion != FormatVersion || len(first.ID) != 32 {
		t.Fatalf("unexpected identity: %+v", first)
	}
}

func TestLoadRejectsUnsupportedOrMalformedIdentity(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "version", payload: `{"format_version":2,"id":"00112233445566778899aabbccddeeff"}`},
		{name: "id", payload: `{"format_version":1,"id":"not-an-id"}`},
		{name: "json", payload: `{`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, IdentityFileName), []byte(tt.payload), 0o600); err != nil {
				t.Fatalf("write identity: %v", err)
			}
			if _, err := LoadOrCreate(root); err == nil {
				t.Fatal("expected invalid identity error")
			}
		})
	}
}
