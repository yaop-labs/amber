package model

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestNewEntryID_Unique(t *testing.T) {
	ids := make(map[string]struct{}, 1000)
	for range 1000 {
		id, err := NewEntryID()
		if err != nil {
			t.Fatalf("NewEntryID: %v", err)
		}
		s := id.String()
		if _, exists := ids[s]; exists {
			t.Fatalf("duplicate EntryID: %s", s)
		}
		ids[s] = struct{}{}
	}
}

func TestEntryIDFromString_RoundTrip(t *testing.T) {
	id := MustNewEntryID()
	s := id.String()

	parsed, err := EntryIDFromString(s)
	if err != nil {
		t.Fatalf("EntryIDFromString: %v", err)
	}
	if parsed != id {
		t.Errorf("round-trip failed: got %s, want %s", parsed, id)
	}
}

func TestEntryIDToUint64_Deterministic(t *testing.T) {
	id := MustNewEntryID()
	u1 := EntryIDToUint64(id)
	u2 := EntryIDToUint64(id)
	if u1 != u2 {
		t.Errorf("EntryIDToUint64 not deterministic: %d != %d", u1, u2)
	}
}

func TestEncodedSizeMatchesWriteTo(t *testing.T) {
	logEntry := LogEntry{
		ID: MustNewEntryID(), Timestamp: time.Now(), Level: LevelWarn,
		Service: "api", Host: "node", Body: "hello",
		Attrs: []Attr{{Key: "env", Value: "test"}, {Key: "zone", Value: "a"}},
	}
	wantLog, err := logEntry.EncodedSize()
	if err != nil {
		t.Fatal(err)
	}
	var logBuf bytes.Buffer
	if _, err := logEntry.WriteTo(&logBuf); err != nil {
		t.Fatal(err)
	}
	if uint64(logBuf.Len()) != wantLog {
		t.Fatalf("log EncodedSize = %d, WriteTo = %d", wantLog, logBuf.Len())
	}
	logPrefix := []byte("prefix")
	appendedLog, err := logEntry.AppendTo(append([]byte(nil), logPrefix...))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(appendedLog[:len(logPrefix)], logPrefix) {
		t.Fatalf("log AppendTo changed prefix: got %q", appendedLog[:len(logPrefix)])
	}
	if !bytes.Equal(appendedLog[len(logPrefix):], logBuf.Bytes()) {
		t.Fatal("log AppendTo encoding differs from WriteTo")
	}

	span := SpanEntry{
		ID: MustNewEntryID(), Service: "api", Operation: "GET /", StartTime: time.Now(), EndTime: time.Now().Add(time.Millisecond),
		Attrs: []Attr{{Key: "http.method", Value: "GET"}},
	}
	wantSpan, err := span.EncodedSize()
	if err != nil {
		t.Fatal(err)
	}
	var spanBuf bytes.Buffer
	if _, err := span.WriteTo(&spanBuf); err != nil {
		t.Fatal(err)
	}
	if uint64(spanBuf.Len()) != wantSpan {
		t.Fatalf("span EncodedSize = %d, WriteTo = %d", wantSpan, spanBuf.Len())
	}
	spanPrefix := []byte("prefix")
	appendedSpan, err := span.AppendTo(append([]byte(nil), spanPrefix...))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(appendedSpan[:len(spanPrefix)], spanPrefix) {
		t.Fatalf("span AppendTo changed prefix: got %q", appendedSpan[:len(spanPrefix)])
	}
	if !bytes.Equal(appendedSpan[len(spanPrefix):], spanBuf.Bytes()) {
		t.Fatal("span AppendTo encoding differs from WriteTo")
	}
}

func TestAppendToLeavesDestinationUnchangedOnValidationError(t *testing.T) {
	prefix := []byte("keep")
	logEntry := LogEntry{Service: strings.Repeat("x", maxSmallStringBytes+1)}
	got, err := logEntry.AppendTo(append([]byte(nil), prefix...))
	if err == nil {
		t.Fatal("LogEntry.AppendTo accepted oversized service")
	}
	if !bytes.Equal(got, prefix) {
		t.Fatalf("LogEntry.AppendTo changed destination on error: got %q", got)
	}

	span := SpanEntry{Operation: strings.Repeat("x", maxSmallStringBytes+1)}
	got, err = span.AppendTo(append([]byte(nil), prefix...))
	if err == nil {
		t.Fatal("SpanEntry.AppendTo accepted oversized operation")
	}
	if !bytes.Equal(got, prefix) {
		t.Fatalf("SpanEntry.AppendTo changed destination on error: got %q", got)
	}
}

func TestEntryIDToUint64_Unique(t *testing.T) {
	seen := make(map[uint64]struct{}, 1000)
	for i := range 1000 {
		id := MustNewEntryID()
		u := EntryIDToUint64(id)
		if _, exists := seen[u]; exists {
			t.Fatalf("duplicate uint64 for EntryID at iteration %d", i)
		}
		seen[u] = struct{}{}
	}
}

func TestEntryIDTime(t *testing.T) {
	before := time.Now().Truncate(time.Millisecond)
	id := MustNewEntryID()
	after := time.Now().Add(time.Millisecond)

	ts := EntryIDTime(id)
	if ts.Before(before) || ts.After(after) {
		t.Errorf("EntryIDTime out of range: got %v, want between %v and %v", ts, before, after)
	}
}

func TestLogEntry_WriteTo_ReadFrom_RoundTrip(t *testing.T) {
	original := LogEntry{
		ID:        MustNewEntryID(),
		Timestamp: time.Now().Truncate(time.Nanosecond),
		Level:     LevelError,
		Service:   "api-gateway",
		Host:      "pod-xyz-123",
		Body:      "connection refused to postgres:5432",
		Attrs: []Attr{
			{Key: "db_host", Value: "postgres:5432"},
			{Key: "retry", Value: "3"},
		},
	}
	original.TraceID[0] = 0xAB
	original.SpanID[0] = 0xCD

	var buf bytes.Buffer
	_, err := original.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	var restored LogEntry
	_, err = restored.ReadFrom(&buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	assertLogEntryEqual(t, original, restored)
}

func TestLogEntry_RoundTrip_Empty(t *testing.T) {
	original := LogEntry{
		ID:        MustNewEntryID(),
		Timestamp: time.Now().Truncate(time.Nanosecond),
		Level:     LevelInfo,
		Service:   "",
		Host:      "",
		Body:      "",
		Attrs:     nil,
	}

	var buf bytes.Buffer
	if _, err := original.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	var restored LogEntry
	if _, err := restored.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	assertLogEntryEqual(t, original, restored)
}

func TestLogEntry_RoundTrip_AllLevels(t *testing.T) {
	levels := []Level{LevelTrace, LevelDebug, LevelInfo, LevelWarn, LevelError, LevelFatal}
	for _, level := range levels {
		entry := LogEntry{
			ID:        MustNewEntryID(),
			Timestamp: time.Now().Truncate(time.Nanosecond),
			Level:     level,
			Service:   "svc",
			Body:      "test",
		}

		var buf bytes.Buffer
		if _, err := entry.WriteTo(&buf); err != nil {
			t.Fatalf("level %s: WriteTo: %v", level, err)
		}

		var restored LogEntry
		if _, err := restored.ReadFrom(&buf); err != nil {
			t.Fatalf("level %s: ReadFrom: %v", level, err)
		}

		if restored.Level != level {
			t.Errorf("level mismatch: got %s, want %s", restored.Level, level)
		}
	}
}

func TestLogEntry_RoundTrip_WithTraceContext(t *testing.T) {
	original := LogEntry{
		ID:        MustNewEntryID(),
		Timestamp: time.Now().Truncate(time.Nanosecond),
		Level:     LevelInfo,
		Service:   "api",
		Body:      "request processed",
	}
	// Заполняем TraceID и SpanID
	for i := range original.TraceID {
		original.TraceID[i] = byte(i)
	}
	for i := range original.SpanID {
		original.SpanID[i] = byte(i + 16)
	}

	var buf bytes.Buffer
	if _, err := original.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	var restored LogEntry
	if _, err := restored.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	if restored.TraceID != original.TraceID {
		t.Errorf("TraceID mismatch: got %v, want %v", restored.TraceID, original.TraceID)
	}
	if restored.SpanID != original.SpanID {
		t.Errorf("SpanID mismatch: got %v, want %v", restored.SpanID, original.SpanID)
	}
}

func TestLogEntry_RoundTrip_ManyEntries(t *testing.T) {
	const count = 1000
	entries := make([]LogEntry, count)
	for i := range entries {
		entries[i] = LogEntry{
			ID:        MustNewEntryID(),
			Timestamp: time.Now().Truncate(time.Nanosecond),
			Level:     Level(i % 6),
			Service:   "service-" + string(rune('A'+i%26)),
			Host:      "host-001",
			Body:      "log message number",
			Attrs:     []Attr{{Key: "index", Value: "val"}},
		}
	}

	var buf bytes.Buffer
	for i := range entries {
		if _, err := entries[i].WriteTo(&buf); err != nil {
			t.Fatalf("entry %d WriteTo: %v", i, err)
		}
	}

	for i := range entries {
		var restored LogEntry
		if _, err := restored.ReadFrom(&buf); err != nil {
			t.Fatalf("entry %d ReadFrom: %v", i, err)
		}
		if restored.ID != entries[i].ID {
			t.Errorf("entry %d: ID mismatch", i)
		}
		if restored.Level != entries[i].Level {
			t.Errorf("entry %d: Level mismatch", i)
		}
	}
}

func TestSpanEntry_WriteTo_ReadFrom_RoundTrip(t *testing.T) {
	var traceID TraceID
	var spanID SpanID
	var parentID SpanID
	traceID[0] = 0x01
	spanID[0] = 0x02
	parentID[0] = 0x03

	original := SpanEntry{
		ID:        MustNewEntryID(),
		TraceID:   traceID,
		SpanID:    spanID,
		ParentID:  parentID,
		Service:   "checkout",
		Operation: "processPayment",
		StartTime: time.Now().Truncate(time.Nanosecond),
		EndTime:   time.Now().Add(100 * time.Millisecond).Truncate(time.Nanosecond),
		Status:    SpanStatusOK,
		Attrs: []Attr{
			{Key: "payment.method", Value: "card"},
			{Key: "payment.amount", Value: "99.99"},
		},
	}

	var buf bytes.Buffer
	if _, err := original.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}

	var restored SpanEntry
	if _, err := restored.ReadFrom(&buf); err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}

	assertSpanEntryEqual(t, original, restored)
}

func TestSpanEntry_RootSpan(t *testing.T) {
	span := SpanEntry{
		ID:       MustNewEntryID(),
		ParentID: ZeroSpanID(),
	}
	if !span.IsRoot() {
		t.Error("span with zero ParentID should be root")
	}
}

func TestSpanEntry_Duration(t *testing.T) {
	start := time.Now()
	end := start.Add(250 * time.Millisecond)
	span := SpanEntry{
		StartTime: start,
		EndTime:   end,
	}
	if span.Duration() != 250*time.Millisecond {
		t.Errorf("unexpected duration: %v", span.Duration())
	}
}

func TestLevelFromString(t *testing.T) {
	cases := []struct {
		input string
		want  Level
	}{
		{"TRACE", LevelTrace},
		{"trace", LevelTrace},
		{"DEBUG", LevelDebug},
		{"INFO", LevelInfo},
		{"WARN", LevelWarn},
		{"WARNING", LevelWarn},
		{"ERROR", LevelError},
		{"FATAL", LevelFatal},
	}

	for _, c := range cases {
		got, err := LevelFromString(c.input)
		if err != nil {
			t.Errorf("LevelFromString(%q): unexpected error: %v", c.input, err)
			continue
		}
		if got != c.want {
			t.Errorf("LevelFromString(%q): got %s, want %s", c.input, got, c.want)
		}
	}
}

func TestLevelFromString_Unknown(t *testing.T) {
	_, err := LevelFromString("UNKNOWN_LEVEL")
	if err == nil {
		t.Error("expected error for unknown level")
	}
}

func assertLogEntryEqual(t *testing.T, want, got LogEntry) {
	t.Helper()

	if got.ID != want.ID {
		t.Errorf("ID: got %s, want %s", got.ID, want.ID)
	}
	if !got.Timestamp.Equal(want.Timestamp) {
		t.Errorf("Timestamp: got %v, want %v", got.Timestamp, want.Timestamp)
	}
	if got.Level != want.Level {
		t.Errorf("Level: got %s, want %s", got.Level, want.Level)
	}
	if got.Service != want.Service {
		t.Errorf("Service: got %q, want %q", got.Service, want.Service)
	}
	if got.Host != want.Host {
		t.Errorf("Host: got %q, want %q", got.Host, want.Host)
	}
	if got.TraceID != want.TraceID {
		t.Errorf("TraceID: got %v, want %v", got.TraceID, want.TraceID)
	}
	if got.SpanID != want.SpanID {
		t.Errorf("SpanID: got %v, want %v", got.SpanID, want.SpanID)
	}
	if got.Body != want.Body {
		t.Errorf("Body: got %q, want %q", got.Body, want.Body)
	}
	if len(got.Attrs) != len(want.Attrs) {
		t.Errorf("Attrs len: got %d, want %d", len(got.Attrs), len(want.Attrs))
		return
	}
	for i := range want.Attrs {
		if got.Attrs[i] != want.Attrs[i] {
			t.Errorf("Attrs[%d]: got %+v, want %+v", i, got.Attrs[i], want.Attrs[i])
		}
	}
}

func assertSpanEntryEqual(t *testing.T, want, got SpanEntry) {
	t.Helper()

	if got.ID != want.ID {
		t.Errorf("ID: got %s, want %s", got.ID, want.ID)
	}
	if got.TraceID != want.TraceID {
		t.Errorf("TraceID mismatch")
	}
	if got.SpanID != want.SpanID {
		t.Errorf("SpanID mismatch")
	}
	if got.ParentID != want.ParentID {
		t.Errorf("ParentID mismatch")
	}
	if got.Service != want.Service {
		t.Errorf("Service: got %q, want %q", got.Service, want.Service)
	}
	if got.Operation != want.Operation {
		t.Errorf("Operation: got %q, want %q", got.Operation, want.Operation)
	}
	if !got.StartTime.Equal(want.StartTime) {
		t.Errorf("StartTime mismatch")
	}
	if !got.EndTime.Equal(want.EndTime) {
		t.Errorf("EndTime mismatch")
	}
	if got.Status != want.Status {
		t.Errorf("Status: got %s, want %s", got.Status, want.Status)
	}
	if len(got.Attrs) != len(want.Attrs) {
		t.Errorf("Attrs len: got %d, want %d", len(got.Attrs), len(want.Attrs))
		return
	}
	for i := range want.Attrs {
		if got.Attrs[i] != want.Attrs[i] {
			t.Errorf("Attrs[%d]: got %+v, want %+v", i, got.Attrs[i], want.Attrs[i])
		}
	}
}
