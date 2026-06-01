package query

import (
	"errors"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/index"
)

type RangeSelector struct {
	Selector     index.Selector
	Window       time.Duration
	MaxSampleGap time.Duration
}

func (r RangeSelector) Options(endMillis int64) Options {
	opts := TimeWindow(endMillis, r.Window)
	if r.MaxSampleGap > 0 {
		opts = opts.WithMaxSampleGap(r.MaxSampleGap)
	}
	return opts
}

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
