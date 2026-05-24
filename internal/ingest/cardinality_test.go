package ingest

import (
	"strings"
	"testing"

	"github.com/yaop-labs/amber/internal/model"
)

func TestCardinalityGuard_AttrsPerEntry(t *testing.T) {
	g := NewCardinalityGuard(2, 0, 0)
	ok := []model.Attr{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}}
	if r := g.Check("svc", ok); r != "" {
		t.Fatalf("at-limit must pass, got %q", r)
	}
	too := []model.Attr{{Key: "a", Value: "1"}, {Key: "b", Value: "2"}, {Key: "c", Value: "3"}}
	if r := g.Check("svc", too); r != "attrs_per_entry" {
		t.Fatalf("over-limit reason = %q, want attrs_per_entry", r)
	}
}

func TestCardinalityGuard_ValueLength(t *testing.T) {
	g := NewCardinalityGuard(0, 4, 0)
	if r := g.Check("svc", []model.Attr{{Key: "k", Value: "abcd"}}); r != "" {
		t.Fatalf("at-limit must pass, got %q", r)
	}
	long := strings.Repeat("x", 5)
	if r := g.Check("svc", []model.Attr{{Key: "k", Value: long}}); r != "attr_value_too_long" {
		t.Fatalf("over-limit reason = %q, want attr_value_too_long", r)
	}
}

func TestCardinalityGuard_KeysPerService(t *testing.T) {
	g := NewCardinalityGuard(0, 0, 2)
	if r := g.Check("svc", []model.Attr{{Key: "a"}}); r != "" {
		t.Fatal("first key must pass")
	}
	if r := g.Check("svc", []model.Attr{{Key: "a"}, {Key: "b"}}); r != "" {
		t.Fatal("second distinct key at-limit must pass")
	}
	if r := g.Check("svc", []model.Attr{{Key: "c"}}); r != "key_cardinality" {
		t.Fatalf("over-limit reason = %q, want key_cardinality", r)
	}
	if r := g.Check("svc", []model.Attr{{Key: "a"}, {Key: "b"}}); r != "" {
		t.Fatal("known keys must keep passing after limit")
	}
	if r := g.Check("other", []model.Attr{{Key: "x"}, {Key: "y"}}); r != "" {
		t.Fatal("limit is per-service; other service must have its own budget")
	}
}

func TestCardinalityGuard_ZeroLimitsDisabled(t *testing.T) {
	g := NewCardinalityGuard(0, 0, 0)
	huge := make([]model.Attr, 1000)
	for i := range huge {
		huge[i] = model.Attr{Key: string(rune('a' + i%26)), Value: strings.Repeat("v", 1<<16)}
	}
	if r := g.Check("svc", huge); r != "" {
		t.Fatalf("zero limits must disable all checks, got %q", r)
	}
}

func TestCardinalityGuard_NilReceiverPasses(t *testing.T) {
	var g *CardinalityGuard
	if r := g.Check("svc", []model.Attr{{Key: "k", Value: "v"}}); r != "" {
		t.Fatalf("nil guard must pass, got %q", r)
	}
}
