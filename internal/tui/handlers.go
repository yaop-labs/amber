package tui

import (
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
)

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	// While the search box is focused it owns the keyboard, except for the
	// keys that submit or cancel it.
	if m.search.Focused() {
		switch msg.String() {
		case "enter":
			m.search.Blur()
			m.loading = true
			m.err = nil
			m.follow = false
			return m, fetchLogs(m.ctx, m.c, m.logQuery(), false)
		case "esc":
			m.search.Blur()
			return m, nil
		default:
			var cmd tea.Cmd
			m.search, cmd = m.search.Update(msg)
			return m, cmd
		}
	}

	switch msg.String() {
	case "ctrl+c", "q":
		return m, tea.Quit
	case "tab":
		return m.switchView()
	}

	switch m.view {
	case viewLogs:
		return m.logsKey(msg)
	case viewTraces:
		return m.tracesKey(msg)
	case viewTrace:
		return m.traceKey(msg)
	}
	return m, nil
}

// switchView toggles between the logs and traces lists; from the trace detail
// it returns to the traces list.
func (m model) switchView() (tea.Model, tea.Cmd) {
	switch m.view {
	case viewLogs:
		m.view = viewTraces
		if len(m.traces) == 0 {
			m.loading = true
			return m, fetchTraces(m.ctx, m.c, m.traceQuery())
		}
	default:
		m.view = viewLogs
	}
	return m, nil
}

func (m model) logsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		m.logSel--
		m.clampLogSel()
		m.ensureLogVisible()
	case "down", "j":
		m.logSel++
		m.clampLogSel()
		m.ensureLogVisible()
	case "g", "home":
		m.logSel, m.logTop = 0, 0
	case "G", "end":
		m.logSel = len(m.logs) - 1
		m.clampLogSel()
		m.ensureLogVisible()
	case " ":
		m.toggleExpand()
	case "enter":
		return m.openSelectedTrace()
	case "/":
		m.search.Focus()
		return m, textinput.Blink
	case "t":
		m.rangeIdx = (m.rangeIdx + 1) % len(timeRanges)
		m.loading = true
		return m, fetchLogs(m.ctx, m.c, m.logQuery(), false)
	case "r":
		m.loading = true
		return m, fetchLogs(m.ctx, m.c, m.logQuery(), false)
	case "f":
		return m.toggleFollow()
	}
	return m, nil
}

func (m model) tracesKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		m.traceSel--
		m.clampTraceSel()
	case "down", "j":
		m.traceSel++
		m.clampTraceSel()
	case "g", "home":
		m.traceSel, m.traceTop = 0, 0
	case "G", "end":
		m.traceSel = len(m.traces) - 1
		m.clampTraceSel()
	case "enter":
		if len(m.traces) == 0 {
			return m, nil
		}
		m.prev = viewTraces
		m.view = viewTrace
		m.trace = nil
		m.loading = true
		return m, fetchTrace(m.ctx, m.c, m.traces[m.traceSel].TraceID)
	case "t":
		m.rangeIdx = (m.rangeIdx + 1) % len(timeRanges)
		m.loading = true
		return m, fetchTraces(m.ctx, m.c, m.traceQuery())
	case "r":
		m.loading = true
		return m, fetchTraces(m.ctx, m.c, m.traceQuery())
	}
	return m, nil
}

func (m model) traceKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	if msg.String() == "esc" {
		m.view = m.prev
		return m, nil
	}
	var cmd tea.Cmd
	m.vp, cmd = m.vp.Update(msg)
	return m, cmd
}

func (m *model) toggleExpand() {
	if m.logSel < 0 || m.logSel >= len(m.logs) {
		return
	}
	id := m.logs[m.logSel].ID
	if m.expanded[id] {
		delete(m.expanded, id)
	} else {
		m.expanded[id] = true
	}
}

// openSelectedTrace opens the waterfall for the selected log's trace, or, if
// the entry has no trace, falls back to toggling its inline detail.
func (m model) openSelectedTrace() (tea.Model, tea.Cmd) {
	if m.logSel < 0 || m.logSel >= len(m.logs) {
		return m, nil
	}
	e := m.logs[m.logSel]
	if e.TraceID == "" {
		m.toggleExpand()
		return m, nil
	}
	m.prev = viewLogs
	m.view = viewTrace
	m.trace = nil
	m.loading = true
	return m, fetchTrace(m.ctx, m.c, e.TraceID)
}

func (m model) toggleFollow() (tea.Model, tea.Cmd) {
	m.follow = !m.follow
	if !m.follow {
		return m, nil
	}
	m.followFrom = time.Now()
	m.followBound = nil
	return m, fetchLogs(m.ctx, m.c, m.followQuery(), true)
}

func (m model) handleMouse(msg tea.MouseMsg) (tea.Model, tea.Cmd) {
	switch msg.Action {
	case tea.MouseActionPress:
		switch msg.Button {
		case tea.MouseButtonWheelUp:
			return m.scroll(-1)
		case tea.MouseButtonWheelDown:
			return m.scroll(1)
		case tea.MouseButtonLeft:
			return m.click(msg.Y)
		}
	}
	return m, nil
}

func (m model) scroll(delta int) (tea.Model, tea.Cmd) {
	switch m.view {
	case viewLogs:
		m.logSel += delta
		m.clampLogSel()
		m.ensureLogVisible()
	case viewTraces:
		m.traceSel += delta
		m.clampTraceSel()
	case viewTrace:
		if delta < 0 {
			m.vp.ScrollUp(1)
		} else {
			m.vp.ScrollDown(1)
		}
	}
	return m, nil
}

// click maps a screen row to a list item. Content rows begin at screen y=2
// (y=0 nav, y=1 the view's header/filter bar).
func (m model) click(y int) (tea.Model, tea.Cmd) {
	rel := y - 2
	if rel < 0 {
		return m, nil
	}
	switch m.view {
	case viewLogs:
		idx, ok := m.logIndexAtLine(rel)
		if !ok {
			return m, nil
		}
		m.logSel = idx
		// Click-to-open mirrors the web UI's trace badge: a log with a trace
		// jumps straight to its waterfall; otherwise it expands inline.
		return m.openSelectedTrace()
	case viewTraces:
		idx := m.traceTop + rel
		if idx >= len(m.traces) {
			return m, nil
		}
		m.traceSel = idx
		m.prev = viewTraces
		m.view = viewTrace
		m.trace = nil
		m.loading = true
		return m, fetchTrace(m.ctx, m.c, m.traces[idx].TraceID)
	}
	return m, nil
}

// logIndexAtLine walks the visible rows to find which entry occupies screen
// line rel (0-based within the content area).
func (m model) logIndexAtLine(rel int) (int, bool) {
	rows := m.visibleLogRows()
	line := 0
	for _, r := range rows {
		if rel < line+len(r.lines) {
			return r.idx, true
		}
		line += len(r.lines)
	}
	return 0, false
}
