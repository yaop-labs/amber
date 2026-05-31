package tui

import "github.com/charmbracelet/lipgloss"

// Palette echoes the legacy web UI so the TUI feels like the same product.
var (
	amber = lipgloss.Color("214")
	dim   = lipgloss.Color("241")
	fg    = lipgloss.Color("252")
	blue  = lipgloss.Color("39")
	red   = lipgloss.Color("203")

	navActive    = lipgloss.NewStyle().Foreground(lipgloss.Color("16")).Background(amber).Bold(true).Padding(0, 1)
	navInactive  = lipgloss.NewStyle().Foreground(dim).Padding(0, 1)
	logoStyle    = lipgloss.NewStyle().Foreground(amber).Bold(true)
	statusStyle  = lipgloss.NewStyle().Foreground(dim)
	errorStyle   = lipgloss.NewStyle().Foreground(red)
	dimStyle     = lipgloss.NewStyle().Foreground(dim)
	traceStyle   = lipgloss.NewStyle().Foreground(blue)
	keyStyle     = lipgloss.NewStyle().Foreground(amber)
	bodyStyle    = lipgloss.NewStyle().Foreground(fg)
	errSpanStyle = lipgloss.NewStyle().Foreground(red)
)

var levelStyle = map[string]lipgloss.Style{
	"TRACE": lipgloss.NewStyle().Foreground(lipgloss.Color("245")),
	"DEBUG": lipgloss.NewStyle().Foreground(lipgloss.Color("250")),
	"INFO":  lipgloss.NewStyle().Foreground(blue),
	"WARN":  lipgloss.NewStyle().Foreground(amber),
	"ERROR": lipgloss.NewStyle().Foreground(red),
	"FATAL": lipgloss.NewStyle().Foreground(lipgloss.Color("196")).Bold(true),
}

func styleLevel(level string) string {
	if s, ok := levelStyle[level]; ok {
		return s.Render(level)
	}
	return level
}
