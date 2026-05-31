// Package tui is amberctl's interactive terminal UI, built on bubbletea. It is
// the full-screen replacement for the legacy web front end: a logs explorer, a
// trace list, and a span waterfall, all driven over internal/client. Mouse and
// keyboard are both supported, mirroring the click-to-expand / click-to-open
// interactions of the old browser UI.
package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/textinput"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/amber/internal/client"
)

type view int

const (
	viewLogs view = iota
	viewTraces
	viewTrace
)

// timeRange is a relative window preset; a zero dur means "all time".
type timeRange struct {
	label string
	dur   time.Duration
}

var timeRanges = []timeRange{
	{"all", 0},
	{"15m", 15 * time.Minute},
	{"1h", time.Hour},
	{"6h", 6 * time.Hour},
	{"24h", 24 * time.Hour},
	{"7d", 7 * 24 * time.Hour},
}

const followInterval = 2 * time.Second

type model struct {
	ctx context.Context
	c   *client.Client

	w, h int
	view view
	prev view // view to return to from the trace detail

	// shared
	loading bool
	err     error
	status  string

	// logs
	search      textinput.Model
	logs        []client.LogEntry
	logTotal    int
	logSel      int
	logTop      int // index of first visible entry
	expanded    map[string]bool
	follow      bool
	rangeIdx    int
	followBound map[string]bool
	followFrom  time.Time

	// traces
	traces   []client.TraceSummary
	traceSel int
	traceTop int

	// trace detail
	trace *client.Trace
	vp    viewport.Model
}

// New builds the root model for an amber instance reachable through c.
func New(ctx context.Context, c *client.Client) model {
	ti := textinput.New()
	ti.Placeholder = "full-text search…"
	ti.Prompt = "/ "
	ti.CharLimit = 256

	return model{
		ctx:      ctx,
		c:        c,
		view:     viewLogs,
		search:   ti,
		expanded: make(map[string]bool),
		rangeIdx: 0,
	}
}

// Run launches the TUI against c and blocks until the user quits.
func Run(ctx context.Context, c *client.Client) error {
	p := tea.NewProgram(New(ctx, c), tea.WithAltScreen(), tea.WithMouseCellMotion())
	_, err := p.Run()
	return err
}

func (m model) Init() tea.Cmd {
	return tea.Batch(
		fetchServices(m.ctx, m.c),
		fetchLogs(m.ctx, m.c, m.logQuery(), false),
	)
}

// logQuery builds the current log filter from the search box and time range.
func (m model) logQuery() client.LogQuery {
	q := client.LogQuery{FullText: m.search.Value(), Limit: 200}
	if r := timeRanges[m.rangeIdx]; r.dur > 0 {
		q.From = time.Now().Add(-r.dur)
	}
	return q
}

func (m model) traceQuery() client.TraceQuery {
	q := client.TraceQuery{Limit: 100}
	if r := timeRanges[m.rangeIdx]; r.dur > 0 {
		q.From = time.Now().Add(-r.dur)
	}
	return q
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.w, m.h = msg.Width, msg.Height
		m.search.Width = msg.Width - 4
		m.vp.Width = msg.Width
		m.vp.Height = m.contentHeight()
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case tea.MouseMsg:
		return m.handleMouse(msg)

	case servicesMsg:
		m.status = fmt.Sprintf("%d services", len(msg.services))
		return m, nil

	case logsMsg:
		return m.onLogs(msg)

	case tracesMsg:
		m.loading = false
		m.traces = msg.res.Traces
		m.clampTraceSel()
		return m, nil

	case traceMsg:
		m.loading = false
		m.trace = msg.trace
		m.vp = viewport.New(m.w, m.contentHeight())
		m.vp.SetContent(renderWaterfall(msg.trace))
		return m, nil

	case errMsg:
		m.loading = false
		m.err = msg.err
		return m, nil

	case tickMsg:
		if m.follow && m.view == viewLogs {
			return m, fetchLogs(m.ctx, m.c, m.followQuery(), true)
		}
		return m, nil
	}
	return m, nil
}

func (m model) View() string {
	if m.w == 0 {
		return "loading…"
	}
	var b strings.Builder
	b.WriteString(m.renderNav())
	b.WriteByte('\n')

	switch m.view {
	case viewLogs:
		b.WriteString(m.renderLogsView())
	case viewTraces:
		b.WriteString(m.renderTracesView())
	case viewTrace:
		b.WriteString(m.renderTraceView())
	}

	b.WriteByte('\n')
	b.WriteString(m.renderFooter())
	return b.String()
}

// contentHeight is the room left for the active view: total minus nav, filter
// bar, and footer.
func (m model) contentHeight() int {
	h := m.h - 4
	if h < 1 {
		return 1
	}
	return h
}

func (m model) renderNav() string {
	tab := func(label string, v view) string {
		if m.view == v || (v == viewTraces && m.view == viewTrace) {
			return navActive.Render(label)
		}
		return navInactive.Render(label)
	}
	left := logoStyle.Render("amber") + "  " + tab("Logs", viewLogs) + " " + tab("Traces", viewTraces)
	right := statusStyle.Render(m.status)
	gap := max(m.w-lipglossWidth(left)-lipglossWidth(right), 1)
	return left + strings.Repeat(" ", gap) + right
}

func (m model) renderFooter() string {
	var keys string
	switch m.view {
	case viewLogs:
		keys = "↑/↓ move · enter expand/open · / search · t time · f follow · r refresh · tab traces · q quit"
	case viewTraces:
		keys = "↑/↓ move · enter open · t time · r refresh · tab logs · q quit"
	case viewTrace:
		keys = "↑/↓ scroll · esc back · q quit"
	}
	if m.err != nil {
		return errorStyle.Render("error: " + m.err.Error())
	}
	return dimStyle.Render(keys)
}
