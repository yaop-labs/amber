package main

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
)

func parseDebugSelector(input string) (index.Selector, error) {
	input = strings.TrimSpace(input)
	if input == "" || input == "{}" {
		return index.Selector{}, nil
	}
	if !strings.HasPrefix(input, "{") {
		return parseDebugMetricSelector(input)
	}
	if !strings.HasSuffix(input, "}") {
		return index.Selector{}, errors.New("selector must be wrapped in braces")
	}
	body := strings.TrimSpace(input[1 : len(input)-1])
	if body == "" {
		return index.Selector{}, nil
	}
	parts, err := splitDebugSelectorParts(body)
	if err != nil {
		return index.Selector{}, err
	}
	matchers := make([]index.Matcher, 0, len(parts))
	for _, part := range parts {
		name, op, value, err := parseDebugMatcher(part)
		if err != nil {
			return index.Selector{}, err
		}
		matchers = append(matchers, index.Matcher{Name: name, Op: op, Value: value})
	}
	return index.Selector{Matchers: matchers}, nil
}

func parseDebugMetricSelector(input string) (index.Selector, error) {
	input = strings.TrimSpace(input)
	brace := strings.Index(input, "{")
	if brace < 0 {
		if !validDebugMetricName(input) {
			return index.Selector{}, fmt.Errorf("invalid metric name %q", input)
		}
		return index.Selector{Matchers: []index.Matcher{{Name: "__name__", Op: index.MatchEqual, Value: input}}}, nil
	}
	name := strings.TrimSpace(input[:brace])
	if !validDebugMetricName(name) {
		return index.Selector{}, fmt.Errorf("invalid metric name %q", name)
	}
	selector, err := parseDebugSelector(input[brace:])
	if err != nil {
		return index.Selector{}, err
	}
	selector.Matchers = append([]index.Matcher{{Name: "__name__", Op: index.MatchEqual, Value: name}}, selector.Matchers...)
	return selector, nil
}

func parseDebugRangeSelector(input string) (query.RangeSelector, error) {
	input = strings.TrimSpace(input)
	open := strings.LastIndex(input, "[")
	if open < 0 || !strings.HasSuffix(input, "]") {
		return query.RangeSelector{}, fmt.Errorf("range selector %q must end with [duration]", input)
	}
	selector, err := parseDebugSelector(input[:open])
	if err != nil {
		return query.RangeSelector{}, err
	}
	window, err := parseDebugDuration(input[open+1 : len(input)-1])
	if err != nil {
		return query.RangeSelector{}, err
	}
	return query.RangeSelector{Selector: selector, Window: window}, nil
}

func splitDebugSelectorParts(body string) ([]string, error) {
	var parts []string
	var current strings.Builder
	inQuote := false
	escaped := false
	for _, r := range body {
		switch {
		case escaped:
			current.WriteRune(r)
			escaped = false
		case r == '\\' && inQuote:
			current.WriteRune(r)
			escaped = true
		case r == '"':
			current.WriteRune(r)
			inQuote = !inQuote
		case r == ',' && !inQuote:
			part := strings.TrimSpace(current.String())
			if part == "" {
				return nil, errors.New("empty matcher")
			}
			parts = append(parts, part)
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	if inQuote {
		return nil, errors.New("unterminated quoted value")
	}
	part := strings.TrimSpace(current.String())
	if part == "" {
		return nil, errors.New("empty matcher")
	}
	parts = append(parts, part)
	return parts, nil
}

func parseDebugMatcher(part string) (string, index.MatchOp, string, error) {
	name, op, rawValue, ok := splitDebugMatcher(part)
	if !ok {
		return "", 0, "", fmt.Errorf("matcher %q is missing operator", part)
	}
	if name == "" {
		return "", 0, "", errors.New("empty label name")
	}
	value, err := strconv.Unquote(rawValue)
	if err != nil {
		return "", 0, "", err
	}
	if op == index.MatchRegexp || op == index.MatchNotRegexp {
		if _, err := regexp.Compile(value); err != nil {
			return "", 0, "", err
		}
	}
	return name, op, value, nil
}

func splitDebugMatcher(part string) (string, index.MatchOp, string, bool) {
	for _, candidate := range []struct {
		token string
		op    index.MatchOp
	}{
		{"!~", index.MatchNotRegexp},
		{"!=", index.MatchNotEqual},
		{"=~", index.MatchRegexp},
		{"=", index.MatchEqual},
	} {
		idx := strings.Index(part, candidate.token)
		if idx >= 0 {
			return strings.TrimSpace(part[:idx]), candidate.op, strings.TrimSpace(part[idx+len(candidate.token):]), true
		}
	}
	return "", 0, "", false
}

func parseDebugDuration(input string) (time.Duration, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return 0, errors.New("empty duration")
	}
	unit := input[len(input)-1]
	value, err := strconv.ParseInt(input[:len(input)-1], 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("invalid duration %q", input)
	}
	switch unit {
	case 's':
		return time.Duration(value) * time.Second, nil
	case 'm':
		return time.Duration(value) * time.Minute, nil
	case 'h':
		return time.Duration(value) * time.Hour, nil
	case 'd':
		return time.Duration(value) * 24 * time.Hour, nil
	default:
		return 0, fmt.Errorf("unsupported duration unit %q", unit)
	}
}

func validDebugMetricName(name string) bool {
	if name == "" {
		return false
	}
	for i, r := range name {
		valid := r == '_' || r == ':' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r))
		if !valid {
			return false
		}
	}
	return true
}
