package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/yaop-labs/amber/internal/client"
)

// writef / writeln wrap fmt.Fprint* and intentionally drop the error: amberctl
// renders to stdout, where a write failure is neither recoverable nor worth
// surfacing. Mirrors the same pattern in internal/metrics.
func writef(w io.Writer, format string, a ...any) { _, _ = fmt.Fprintf(w, format, a...) }
func writeln(w io.Writer, a ...any)               { _, _ = fmt.Fprintln(w, a...) }

// Level colours mirror the legacy web UI palette so the two faces feel the
// same. lipgloss strips these automatically when the output is not a TTY, so
// piped output stays plain.
var levelStyle = map[string]lipgloss.Style{
	"TRACE": lipgloss.NewStyle().Foreground(lipgloss.Color("245")),
	"DEBUG": lipgloss.NewStyle().Foreground(lipgloss.Color("250")),
	"INFO":  lipgloss.NewStyle().Foreground(lipgloss.Color("39")),
	"WARN":  lipgloss.NewStyle().Foreground(lipgloss.Color("214")),
	"ERROR": lipgloss.NewStyle().Foreground(lipgloss.Color("203")),
	"FATAL": lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true),
}

var (
	dimStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	traceStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("39"))
	errStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("203"))
)

const tsLayout = "2006-01-02 15:04:05.000"

func styleLevel(level string) string {
	padded := fmt.Sprintf("%-5s", level)
	if s, ok := levelStyle[level]; ok {
		return s.Render(padded)
	}
	return padded
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// --- logs ---

func renderLogLine(w io.Writer, e client.LogEntry) {
	ts := dimStyle.Render(e.Timestamp.Format(tsLayout))
	svc := fmt.Sprintf("%-18s", truncate(e.Service, 18))
	line := fmt.Sprintf("%s  %s  %s  %s", ts, styleLevel(e.Level), svc, e.Body)
	if e.TraceID != "" {
		line += "  " + traceStyle.Render("trace:"+shortID(e.TraceID))
	}
	writeln(w, line)
}

func renderLogs(w io.Writer, res *client.LogResult) {
	for _, e := range res.Entries {
		renderLogLine(w, e)
	}
	summary := fmt.Sprintf("\n%d hits in %dms", res.TotalHits, res.TookMs)
	if res.Truncated {
		summary += " (truncated)"
	}
	if res.NextCursor != "" {
		summary += "  next-cursor: " + res.NextCursor
	}
	writeln(w, dimStyle.Render(summary))
}

// --- traces ---

func renderTraces(w io.Writer, res *client.TraceList) {
	for _, t := range res.Traces {
		marker := " "
		op := t.Operation
		if t.HasErrors {
			marker = errStyle.Render("!")
			op = errStyle.Render(op)
		}
		writef(w, "%s %s  %s  %-18s  %s  %s\n",
			marker,
			dimStyle.Render(t.StartTime.Format(tsLayout)),
			traceStyle.Render(shortID(t.TraceID)),
			truncate(t.Service, 18),
			op,
			dimStyle.Render(fmt.Sprintf("%dms · %d spans", t.DurationMs, t.SpanCount)),
		)
	}
	writeln(w, dimStyle.Render(fmt.Sprintf("\n%d traces", res.Total)))
}

// --- single trace waterfall ---

func renderTrace(w io.Writer, tr *client.Trace) {
	writef(w, "%s  %s\n\n",
		traceStyle.Render(tr.TraceID),
		dimStyle.Render(fmt.Sprintf("%d spans · %d logs · %dms", tr.SpanCount, tr.LogCount, tr.TookMs)),
	)
	var root time.Time
	for _, n := range tr.Tree {
		if root.IsZero() || n.Span.StartTime.Before(root) {
			root = n.Span.StartTime
		}
	}
	for _, n := range tr.Tree {
		renderSpanNode(w, n, 0, root)
	}
}

func renderSpanNode(w io.Writer, n *client.SpanNode, depth int, root time.Time) {
	indent := strings.Repeat("  ", depth)
	dur := n.Span.EndTime.Sub(n.Span.StartTime)
	offset := n.Span.StartTime.Sub(root)

	op := n.Span.Operation
	status := ""
	if n.Span.Status == client.SpanStatusError {
		op = errStyle.Render(op)
		status = errStyle.Render(" ERROR")
	}
	writef(w, "%s%s %s  %s%s\n",
		indent,
		op,
		dimStyle.Render(n.Span.Service),
		dimStyle.Render(fmt.Sprintf("+%s · %s", roundMs(offset), roundMs(dur))),
		status,
	)
	for _, lg := range n.Logs {
		writef(w, "%s  %s %s %s\n",
			indent,
			dimStyle.Render(lg.Timestamp.Format("15:04:05.000")),
			styleLevel(lg.Level),
			lg.Body,
		)
	}
	for _, child := range n.Children {
		renderSpanNode(w, child, depth+1, root)
	}
}

// --- services / stats ---

func renderServices(w io.Writer, svcs []string) {
	for _, s := range svcs {
		writeln(w, s)
	}
}

func renderStats(w io.Writer, s *client.Stats) {
	writef(w, "segments   sealed=%d records=%d size=%dMB\n",
		s.Segments.SealedCount, s.Segments.TotalRecords, s.Segments.TotalMB)
	if s.Segments.Active.Exists {
		writef(w, "active     %s id=%d records=%d\n",
			s.Segments.Active.File, s.Segments.Active.ID, s.Segments.Active.RecordCount)
	}
	writef(w, "index      segments=%d\n", s.SparseIndex.Segments)
	writef(w, "memory     heap_alloc=%dMB heap_inuse=%dMB objects=%d\n",
		s.Memory.HeapAllocMB, s.Memory.HeapInuseMB, s.Memory.HeapObjects)
}

// --- json helpers ---

func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	return enc.Encode(v)
}

func writeNDJSON(w io.Writer, items []client.LogEntry) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for i := range items {
		if err := enc.Encode(items[i]); err != nil {
			return err
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return s[:n]
	}
	return s[:n-1] + "…"
}

func roundMs(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}
