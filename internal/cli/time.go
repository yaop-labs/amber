package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// parseSince turns a relative span like "15m", "6h", "7d", "2w" into a
// duration. Go's time.ParseDuration has no day/week unit, so those are handled
// here; everything else falls through to the stdlib parser.
func parseSince(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	switch s[len(s)-1] {
	case 'd', 'w':
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		unit := 24 * time.Hour
		if s[len(s)-1] == 'w' {
			unit = 7 * 24 * time.Hour
		}
		return time.Duration(n) * unit, nil
	default:
		return time.ParseDuration(s)
	}
}

// timeLayouts accepted for --from / --to, tried in order. RFC3339 first since
// it is what scripts will pass; the looser forms are for interactive use and
// are interpreted in the local zone.
var timeLayouts = []string{
	time.RFC3339,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
}

func parseTime(s string) (time.Time, error) {
	s = strings.TrimSpace(s)
	for _, layout := range timeLayouts {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognised time %q (want RFC3339 or 'YYYY-MM-DD HH:MM:SS')", s)
}

// resolveRange folds --since / --from / --to into an absolute [from, to].
// since is mutually exclusive with from.
func resolveRange(since, from, to string, now time.Time) (fromT, toT time.Time, err error) {
	if since != "" && from != "" {
		return fromT, toT, fmt.Errorf("--since and --from are mutually exclusive")
	}
	if since != "" {
		d, e := parseSince(since)
		if e != nil {
			return fromT, toT, e
		}
		fromT = now.Add(-d)
	} else if from != "" {
		if fromT, err = parseTime(from); err != nil {
			return fromT, toT, fmt.Errorf("--from: %w", err)
		}
	}
	if to != "" {
		if toT, err = parseTime(to); err != nil {
			return fromT, toT, fmt.Errorf("--to: %w", err)
		}
	}
	return fromT, toT, nil
}
