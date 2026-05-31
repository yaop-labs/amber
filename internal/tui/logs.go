package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/amber/internal/client"
)

const logTsLayout = "15:04:05.000"

// logRow is one entry's rendered block: a header line plus any expanded detail
// lines. Both View and mouse hit-testing build rows the same way so the
// clickable geometry never drifts from what is drawn.
type logRow struct {
	idx   int
	lines []string
}

func (m model) logRowsHeight() int {
	h := m.h - 3 // nav, filter bar, footer
	if h < 1 {
		return 1
	}
	return h
}

func (m model) buildLogRow(idx int, selected bool) logRow {
	e := m.logs[idx]
	ts := dimStyle.Render(e.Timestamp.Format(logTsLayout))
	svc := fmt.Sprintf("%-16s", truncate(e.Service, 16))
	lvl := fmt.Sprintf("%-5s", e.Level)
	if s, ok := levelStyle[e.Level]; ok {
		lvl = s.Render(lvl)
	}
	head := fmt.Sprintf("%s %s %s %s", ts, lvl, dimStyle.Render(svc), bodyStyle.Render(e.Body))
	if e.TraceID != "" {
		head += " " + traceStyle.Render("trace:"+short(e.TraceID))
	}
	cursor := "  "
	if selected {
		cursor = keyStyle.Render("▸ ")
	}
	lines := []string{cursor + head}

	if m.expanded[e.ID] {
		add := func(k, v string) {
			lines = append(lines, "    "+keyStyle.Render(fmt.Sprintf("%-9s", k))+v)
		}
		if e.TraceID != "" {
			add("trace_id", traceStyle.Render(e.TraceID))
		}
		if e.SpanID != "" {
			add("span_id", e.SpanID)
		}
		if e.Host != "" {
			add("host", e.Host)
		}
		for _, a := range e.Attrs {
			add(a.Key, a.Value)
		}
	}
	return logRow{idx: idx, lines: lines}
}

// visibleLogRows returns the rows starting at logTop that fit in the row
// budget for the current terminal height.
func (m model) visibleLogRows() []logRow {
	budget := m.logRowsHeight()
	var rows []logRow
	used := 0
	for i := m.logTop; i < len(m.logs) && used < budget; i++ {
		r := m.buildLogRow(i, i == m.logSel)
		if used+len(r.lines) > budget && used > 0 {
			break
		}
		rows = append(rows, r)
		used += len(r.lines)
	}
	return rows
}

func (m *model) ensureLogVisible() {
	if m.logSel < m.logTop {
		m.logTop = m.logSel
		return
	}
	// Scroll down until the selected entry is within the visible window.
	for {
		rows := m.visibleLogRows()
		last := m.logTop
		if len(rows) > 0 {
			last = rows[len(rows)-1].idx
		}
		if m.logSel <= last || m.logTop >= m.logSel {
			return
		}
		m.logTop++
	}
}

func (m model) renderLogsView() string {
	// Filter bar.
	var bar string
	if m.search.Focused() {
		bar = m.search.View()
	} else {
		q := m.search.Value()
		if q == "" {
			q = dimStyle.Render("(no filter — press / to search)")
		}
		bar = dimStyle.Render("/ ") + q
	}
	rng := dimStyle.Render(" [" + timeRanges[m.rangeIdx].label + "]")
	hits := dimStyle.Render(fmt.Sprintf("  %d hits", m.logTotal))
	if m.follow {
		hits += keyStyle.Render(" ●live")
	}
	gap := max(m.w-lipglossWidth(bar)-lipglossWidth(rng)-lipglossWidth(hits), 1)
	header := bar + strings.Repeat(" ", gap) + rng + hits

	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')

	if len(m.logs) == 0 {
		b.WriteString(dimStyle.Render("  no results"))
		return padTo(b.String(), m.logRowsHeight()+1)
	}
	rows := m.visibleLogRows()
	for _, r := range rows {
		b.WriteString(strings.Join(r.lines, "\n"))
		b.WriteByte('\n')
	}
	return padTo(strings.TrimRight(b.String(), "\n"), m.logRowsHeight()+1)
}

func (m model) onLogs(msg logsMsg) (tea.Model, tea.Cmd) {
	m.loading = false
	if msg.follow {
		return m.appendFollowLogs(msg.res)
	}
	m.logs = msg.res.Entries
	m.logTotal = msg.res.TotalHits
	m.logSel = 0
	m.logTop = 0
	return m, nil
}

// followQuery asks only for entries at or after the last seen timestamp.
func (m model) followQuery() client.LogQuery {
	q := m.logQuery()
	if !m.followFrom.IsZero() {
		q.From = m.followFrom
	} else {
		q.From = time.Now()
	}
	return q
}

// appendFollowLogs merges a follow poll into the buffer, de-duplicating the
// boundary timestamp the same way the CLI's follow loop does.
func (m model) appendFollowLogs(res *client.LogResult) (tea.Model, tea.Cmd) {
	fresh := res.Entries
	var maxTS time.Time
	for _, e := range fresh {
		if m.followBound[e.ID] {
			continue
		}
		m.logs = append(m.logs, e)
		if e.Timestamp.After(maxTS) {
			maxTS = e.Timestamp
		}
	}
	if !maxTS.IsZero() {
		next := make(map[string]bool)
		for _, e := range fresh {
			if e.Timestamp.Equal(maxTS) {
				next[e.ID] = true
			}
		}
		m.followBound = next
		m.followFrom = maxTS
	}
	// Keep the buffer bounded and follow the tail.
	const maxBuf = 1000
	if len(m.logs) > maxBuf {
		m.logs = m.logs[len(m.logs)-maxBuf:]
	}
	m.logSel = len(m.logs) - 1
	m.ensureLogVisible()
	return m, tick(followInterval)
}

func (m *model) clampLogSel() {
	if m.logSel < 0 {
		m.logSel = 0
	}
	if m.logSel >= len(m.logs) {
		m.logSel = len(m.logs) - 1
	}
	if m.logSel < 0 {
		m.logSel = 0
	}
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
