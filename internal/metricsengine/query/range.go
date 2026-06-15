package query

import (
	"errors"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
)

// RangeSelector pairs a label selector with a trailing window and staleness
// gap, the input to rate/increase range queries.
type RangeSelector struct {
	Selector     index.Selector
	Window       time.Duration
	MaxSampleGap time.Duration
}

// Options returns the per-sample bounds for the window ending at endMillis.
func (r RangeSelector) Options(endMillis int64) Options {
	opts := TimeWindow(endMillis, r.Window)
	if r.MaxSampleGap > 0 {
		opts = opts.WithMaxSampleGap(r.MaxSampleGap)
	}
	return opts
}

// FloatStep is one range step: its timestamp and the per-group float values
// (e.g. rate). IntStep and AggregateStep are the integer and aggregate analogues.
type FloatStep struct {
	TimestampMillis int64
	Values          map[string]float64
}

type IntStep struct {
	TimestampMillis int64
	Values          map[string]int64
}

type AggregateStep struct {
	TimestampMillis int64
	Values          map[string]Aggregate
}

// StepMillis returns the evaluation timestamps from startMillis to endMillis
// inclusive, spaced by step.
func StepMillis(startMillis int64, endMillis int64, step time.Duration) ([]int64, error) {
	if step <= 0 {
		return nil, errors.New("query: step must be positive")
	}
	stepMillis := step.Milliseconds()
	if stepMillis <= 0 {
		return nil, errors.New("query: step must be at least 1ms")
	}
	if endMillis < startMillis {
		return nil, errors.New("query: end must be >= start")
	}
	steps := make([]int64, 0, 1+(endMillis-startMillis)/stepMillis)
	for ts := startMillis; ts <= endMillis; ts += stepMillis {
		steps = append(steps, ts)
		if endMillis-ts < stepMillis {
			break
		}
	}
	return steps, nil
}
