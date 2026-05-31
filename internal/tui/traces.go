package tui

import (
	"fmt"
	"strings"
)

func (m model) traceRowsHeight() int {
	h := m.h - 3 // nav, header, footer
	if h < 1 {
		return 1
	}
	return h
}

func (m *model) clampTraceSel() {
	if m.traceSel < 0 {
		m.traceSel = 0
	}
	if m.traceSel >= len(m.traces) {
		m.traceSel = len(m.traces) - 1
	}
	if m.traceSel < 0 {
		m.traceSel = 0
	}
	budget := m.traceRowsHeight()
	if m.traceSel < m.traceTop {
		m.traceTop = m.traceSel
	}
	if m.traceSel >= m.traceTop+budget {
		m.traceTop = m.traceSel - budget + 1
	}
	if m.traceTop < 0 {
		m.traceTop = 0
	}
}

func (m model) renderTracesView() string {
	rng := timeRanges[m.rangeIdx].label
	header := dimStyle.Render(fmt.Sprintf("%d traces  [%s]", len(m.traces), rng))

	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')

	if len(m.traces) == 0 {
		b.WriteString(dimStyle.Render("  no traces"))
		return padTo(b.String(), m.traceRowsHeight()+1)
	}

	budget := m.traceRowsHeight()
	end := min(m.traceTop+budget, len(m.traces))
	for i := m.traceTop; i < end; i++ {
		t := m.traces[i]
		cursor := "  "
		if i == m.traceSel {
			cursor = keyStyle.Render("▸ ")
		}
		marker := " "
		op := t.Operation
		if t.HasErrors {
			marker = errSpanStyle.Render("!")
			op = errSpanStyle.Render(op)
		}
		line := fmt.Sprintf("%s%s %s %s %s %s",
			cursor,
			marker,
			traceStyle.Render(short(t.TraceID)),
			dimStyle.Render(fmt.Sprintf("%-16s", truncate(t.Service, 16))),
			bodyStyle.Render(truncate(op, m.w-50)),
			dimStyle.Render(fmt.Sprintf("%dms · %d spans", t.DurationMs, t.SpanCount)),
		)
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return padTo(strings.TrimRight(b.String(), "\n"), m.traceRowsHeight()+1)
}
