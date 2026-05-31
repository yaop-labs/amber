package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/client"
)

func (m model) renderTraceView() string {
	if m.trace == nil {
		return padTo(dimStyle.Render("  loading trace…"), m.contentHeight())
	}
	return m.vp.View()
}

// renderWaterfall renders a trace's span tree as an indented waterfall with
// attached logs, for display inside a scrollable viewport.
func renderWaterfall(tr *client.Trace) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s  %s\n\n",
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
		writeSpan(&b, n, 0, root)
	}
	return b.String()
}

func writeSpan(b *strings.Builder, n *client.SpanNode, depth int, root time.Time) {
	indent := strings.Repeat("  ", depth)
	dur := n.Span.EndTime.Sub(n.Span.StartTime)
	offset := n.Span.StartTime.Sub(root)

	op := n.Span.Operation
	status := ""
	if n.Span.Status == client.SpanStatusError {
		op = errSpanStyle.Render(op)
		status = errSpanStyle.Render(" ERROR")
	}
	fmt.Fprintf(b, "%s%s %s  %s%s\n",
		indent,
		bodyStyle.Render(op),
		dimStyle.Render(n.Span.Service),
		dimStyle.Render(fmt.Sprintf("+%s · %s", roundMs(offset), roundMs(dur))),
		status,
	)
	for _, lg := range n.Logs {
		fmt.Fprintf(b, "%s  %s %s %s\n",
			indent,
			dimStyle.Render(lg.Timestamp.Format(logTsLayout)),
			styleLevel(lg.Level),
			lg.Body,
		)
	}
	for _, child := range n.Children {
		writeSpan(b, child, depth+1, root)
	}
}

func roundMs(d time.Duration) string {
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}
