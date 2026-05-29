package query

import (
	"strings"
	"testing"

	"github.com/yaop-labs/amber/internal/model"
)

func TestEncodeCursor_ZeroIsEmpty(t *testing.T) {
	if got := EncodeCursor(Cursor{}); got != "" {
		t.Errorf("zero cursor encodes to %q, want empty", got)
	}
}

func TestDecodeCursor_EmptyIsZero(t *testing.T) {
	c, err := DecodeCursor("")
	if err != nil {
		t.Fatalf("empty: %v", err)
	}
	if !c.IsZero() {
		t.Errorf("empty decodes to non-zero %+v", c)
	}
}

func TestCursor_RoundTrip(t *testing.T) {
	id := model.MustNewEntryID()
	c := Cursor{Timestamp: 1_700_000_000_123_456_789, EntryID: id}
	encoded := EncodeCursor(c)
	if encoded == "" {
		t.Fatal("non-zero cursor encoded to empty")
	}
	if strings.ContainsAny(encoded, "+/=") {
		t.Errorf("encoded uses non-URL-safe chars: %q", encoded)
	}
	got, err := DecodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != c {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, c)
	}
}

func TestDecodeCursor_RejectsBadLength(t *testing.T) {
	// 8 bytes -> "AAAAAAAAAAA" (11 chars base64-no-padding), too short.
	_, err := DecodeCursor("AAAAAAAAAAA")
	if err == nil {
		t.Fatal("expected error for short cursor")
	}
}

func TestDecodeCursor_RejectsPadding(t *testing.T) {
	_, err := DecodeCursor("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err == nil {
		t.Fatal("expected error for padded cursor")
	}
}

func TestDecodeCursor_RejectsGarbage(t *testing.T) {
	_, err := DecodeCursor("!!!not-base64!!!")
	if err == nil {
		t.Fatal("expected error for non-base64 cursor")
	}
}

func TestCursor_After_DifferentTimestamps(t *testing.T) {
	id := model.MustNewEntryID()
	c := Cursor{Timestamp: 1000, EntryID: id}

	// Older record (smaller ts) is after the cursor in newest-first order.
	if !c.After(500, id) {
		t.Error("older ts not reported as after")
	}
	// Newer record is not after.
	if c.After(2000, id) {
		t.Error("newer ts incorrectly reported as after")
	}
}

func TestCursor_After_TimestampTies(t *testing.T) {
	cur := Cursor{Timestamp: 1000}
	// All-zero ID as cursor.
	var smaller, larger model.EntryID
	larger[0] = 1
	// (ts=1000, id=larger) is NOT after (ts=1000, id=zero) because larger > zero
	// in our newest-first ordering means larger came earlier, but cursor points
	// to the last shown record so we need strictly older.
	if cur.After(1000, larger) {
		t.Error("equal ts with larger id reported as after; cursor sees larger as newer")
	}
	// Equal both -> not strictly after.
	if cur.After(1000, smaller) {
		t.Error("equal cursor reported as after itself")
	}
}

func TestCursor_After_SameTimestampSmallerIDIsAfter(t *testing.T) {
	cur := Cursor{Timestamp: 1000}
	cur.EntryID[0] = 5

	// Smaller ID at same TS is the "next older" record per our ordering.
	var smaller model.EntryID
	smaller[0] = 2
	if !cur.After(1000, smaller) {
		t.Error("smaller id at equal ts not reported as after")
	}
}
