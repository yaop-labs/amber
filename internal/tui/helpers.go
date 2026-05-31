package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// lipglossWidth is the printable width of s, ignoring ANSI styling.
func lipglossWidth(s string) int { return lipgloss.Width(s) }

// padTo pads s with blank lines so it occupies exactly n screen lines, keeping
// the footer anchored to the bottom of the terminal.
func padTo(s string, n int) string {
	lines := strings.Count(s, "\n") + 1
	if lines >= n {
		return s
	}
	return s + strings.Repeat("\n", n-lines)
}

func truncate(s string, n int) string {
	if lipgloss.Width(s) <= n {
		return s
	}
	if n <= 1 {
		return string([]rune(s)[:n])
	}
	r := []rune(s)
	if len(r) > n {
		r = r[:n-1]
	}
	return string(r) + "…"
}
