package tui

import (
	"context"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/amber/internal/client"
)

func testModel() model {
	m := New(context.Background(), client.New("http://localhost:0"))
	step(&m, tea.WindowSizeMsg{Width: 120, Height: 40})
	return m
}

// step applies a message and writes the resulting model back through the
// tea.Model interface, the same path the runtime takes.
func step(m *model, msg tea.Msg) tea.Cmd {
	next, cmd := m.Update(msg)
	*m = next.(model)
	return cmd
}

func sampleLogs() *client.LogResult {
	now := time.Date(2026, 5, 31, 12, 0, 0, 0, time.UTC)
	return &client.LogResult{
		TotalHits: 3,
		Entries: []client.LogEntry{
			{ID: "a", Timestamp: now, Level: "INFO", Service: "api", Body: "first", Host: "h1", Attrs: []client.Attr{{Key: "k", Value: "v"}}},
			{ID: "b", Timestamp: now.Add(time.Second), Level: "WARN", Service: "api", Body: "second"},
			{ID: "c", Timestamp: now.Add(2 * time.Second), Level: "ERROR", Service: "web", Body: "third", TraceID: "deadbeefcafe0000"},
		},
	}
}

func TestLogsLoadAndRender(t *testing.T) {
	m := testModel()
	step(&m, logsMsg{res: sampleLogs()})
	if len(m.logs) != 3 || m.logSel != 0 {
		t.Fatalf("logs=%d sel=%d", len(m.logs), m.logSel)
	}
	out := m.View()
	for _, want := range []string{"amber", "first", "third", "3 hits", "trace:deadbeef"} {
		if !strings.Contains(out, want) {
			t.Errorf("view missing %q", want)
		}
	}
}

func TestLogsNavigationAndExpand(t *testing.T) {
	m := testModel()
	step(&m, logsMsg{res: sampleLogs()})

	step(&m, tea.KeyMsg{Type: tea.KeyDown})
	if m.logSel != 1 {
		t.Fatalf("after down sel=%d, want 1", m.logSel)
	}
	// Select the first entry and expand it; its host/attr should appear.
	step(&m, tea.KeyMsg{Type: tea.KeyUp})
	step(&m, tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune(" ")})
	if !m.expanded["a"] {
		t.Fatal("entry a should be expanded")
	}
	out := m.View()
	if !strings.Contains(out, "host") || !strings.Contains(out, "h1") {
		t.Errorf("expanded view missing host detail:\n%s", out)
	}
}

func TestEnterOpensTraceWhenPresent(t *testing.T) {
	m := testModel()
	step(&m, logsMsg{res: sampleLogs()})
	step(&m, tea.KeyMsg{Type: tea.KeyEnd}) // jump to last (has trace)
	cmd := step(&m, tea.KeyMsg{Type: tea.KeyEnter})
	if m.view != viewTrace {
		t.Fatalf("view=%d, want viewTrace", m.view)
	}
	if m.prev != viewLogs {
		t.Errorf("prev=%d, want viewLogs", m.prev)
	}
	if cmd == nil {
		t.Error("expected a fetch command for the trace")
	}
}

func TestEnterExpandsWhenNoTrace(t *testing.T) {
	m := testModel()
	step(&m, logsMsg{res: sampleLogs()})
	// First entry has no trace: enter falls back to inline expand.
	step(&m, tea.KeyMsg{Type: tea.KeyEnter})
	if m.view != viewLogs {
		t.Fatalf("view changed unexpectedly to %d", m.view)
	}
	if !m.expanded["a"] {
		t.Error("entry a should be expanded")
	}
}

func TestClickOpensTrace(t *testing.T) {
	m := testModel()
	step(&m, logsMsg{res: sampleLogs()})
	// Rows start at screen y=2; the third entry (with a trace) is at y=4.
	step(&m, tea.MouseMsg{Action: tea.MouseActionPress, Button: tea.MouseButtonLeft, Y: 4})
	if m.logSel != 2 {
		t.Fatalf("click selected %d, want 2", m.logSel)
	}
	if m.view != viewTrace {
		t.Errorf("click on trace row should open trace, view=%d", m.view)
	}
}

func TestSwitchView(t *testing.T) {
	m := testModel()
	step(&m, logsMsg{res: sampleLogs()})
	step(&m, tea.KeyMsg{Type: tea.KeyTab})
	if m.view != viewTraces {
		t.Fatalf("tab should switch to traces, view=%d", m.view)
	}
	step(&m, tea.KeyMsg{Type: tea.KeyTab})
	if m.view != viewLogs {
		t.Fatalf("tab should switch back to logs, view=%d", m.view)
	}
}

func TestTracesRenderAndOpen(t *testing.T) {
	m := testModel()
	m.view = viewTraces
	step(&m, tracesMsg{res: &client.TraceList{
		Total: 1,
		Traces: []client.TraceSummary{
			{TraceID: "deadbeefcafe0000", Service: "web", Operation: "GET /x", DurationMs: 12, SpanCount: 3, HasErrors: true},
		},
	}})
	out := m.View()
	if !strings.Contains(out, "GET /x") || !strings.Contains(out, "deadbeef") {
		t.Errorf("traces view missing content:\n%s", out)
	}
	cmd := step(&m, tea.KeyMsg{Type: tea.KeyEnter})
	if m.view != viewTrace || cmd == nil {
		t.Errorf("enter should open trace detail (view=%d cmd=%v)", m.view, cmd)
	}
}

func TestWaterfallRender(t *testing.T) {
	now := time.Now()
	tr := &client.Trace{
		TraceID: "deadbeefcafe0000", SpanCount: 2, LogCount: 1, TookMs: 5,
		Tree: []*client.SpanNode{{
			Span: client.Span{SpanID: "1", Service: "web", Operation: "root", StartTime: now, EndTime: now.Add(10 * time.Millisecond)},
			Logs: []client.LogEntry{{Timestamp: now, Level: "INFO", Body: "hello"}},
			Children: []*client.SpanNode{{
				Span: client.Span{SpanID: "2", ParentID: "1", Service: "db", Operation: "query", StartTime: now.Add(time.Millisecond), EndTime: now.Add(4 * time.Millisecond), Status: client.SpanStatusError},
			}},
		}},
	}
	out := renderWaterfall(tr)
	for _, want := range []string{"root", "query", "db", "hello", "ERROR"} {
		if !strings.Contains(out, want) {
			t.Errorf("waterfall missing %q:\n%s", want, out)
		}
	}
}
