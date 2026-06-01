package query

import "time"

func TimeRange(startMillis int64, endMillis int64) Options {
	return Options{StartMillis: &startMillis, EndMillis: &endMillis}
}

func TimeWindow(endMillis int64, window time.Duration) Options {
	startMillis := endMillis - window.Milliseconds()
	return TimeRange(startMillis, endMillis)
}

func ValueRange(min int64, max int64) Options {
	return Options{MinValue: &min, MaxValue: &max}
}

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

func (o Options) WithMaxSampleGap(gap time.Duration) Options {
	gapMillis := gap.Milliseconds()
	o.MaxSampleGapMillis = &gapMillis
	return o
}
