package query

import "time"

// TimeRange builds Options restricting results to [startMillis, endMillis].
func TimeRange(startMillis int64, endMillis int64) Options {
	return Options{StartMillis: &startMillis, EndMillis: &endMillis}
}

// TimeWindow builds Options for the window of length window ending at endMillis.
func TimeWindow(endMillis int64, window time.Duration) Options {
	startMillis := endMillis - window.Milliseconds()
	return TimeRange(startMillis, endMillis)
}

// ValueRange builds Options restricting results to values in [min, max].
func ValueRange(min int64, max int64) Options {
	return Options{MinValue: &min, MaxValue: &max}
}

// The With* methods return a copy of o with one constraint set, for chaining.

func (o Options) WithTimeRange(startMillis int64, endMillis int64) Options {
	o.StartMillis = &startMillis
	o.EndMillis = &endMillis
	return o
}

func (o Options) WithTimeWindow(endMillis int64, window time.Duration) Options {
	startMillis := endMillis - window.Milliseconds()
	return o.WithTimeRange(startMillis, endMillis)
}

func (o Options) WithValueRange(min int64, max int64) Options {
	o.MinValue = &min
	o.MaxValue = &max
	return o
}

// WithMaxSampleGap sets the staleness gap: a rate or increase window spanning a
// sample gap larger than this is suppressed rather than extrapolated across it.
func (o Options) WithMaxSampleGap(gap time.Duration) Options {
	gapMillis := gap.Milliseconds()
	o.MaxSampleGapMillis = &gapMillis
	return o
}
