package tui

import (
	"context"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"github.com/yaop-labs/amber/internal/client"
)

// Messages flowing back into Update from async client calls.
type (
	servicesMsg struct{ services []string }
	logsMsg     struct {
		res    *client.LogResult
		follow bool // result is for an incremental follow poll
	}
	tracesMsg struct{ res *client.TraceList }
	traceMsg  struct{ trace *client.Trace }
	errMsg    struct{ err error }
	tickMsg   time.Time
)

func fetchServices(ctx context.Context, c *client.Client) tea.Cmd {
	return func() tea.Msg {
		s, err := c.Services(ctx)
		if err != nil {
			return errMsg{err}
		}
		return servicesMsg{s}
	}
}

func fetchLogs(ctx context.Context, c *client.Client, q client.LogQuery, follow bool) tea.Cmd {
	return func() tea.Msg {
		res, err := c.Logs(ctx, q)
		if err != nil {
			return errMsg{err}
		}
		return logsMsg{res: res, follow: follow}
	}
}

func fetchTraces(ctx context.Context, c *client.Client, q client.TraceQuery) tea.Cmd {
	return func() tea.Msg {
		res, err := c.Traces(ctx, q)
		if err != nil {
			return errMsg{err}
		}
		return tracesMsg{res}
	}
}

func fetchTrace(ctx context.Context, c *client.Client, id string) tea.Cmd {
	return func() tea.Msg {
		tr, err := c.Trace(ctx, id)
		if err != nil {
			return errMsg{err}
		}
		return traceMsg{tr}
	}
}

func tick(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg { return tickMsg(t) })
}
