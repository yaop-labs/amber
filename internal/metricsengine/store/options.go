package store

import "time"

type Options struct {
	FlushInterval       time.Duration
	MaxBufferedSeries   int
	MaxBufferedSamples  int
	MaxActiveSeries     int
	MaxLabelsPerSeries  int
	MaxLabelNameBytes   int
	MaxLabelValueBytes  int
	Retention           time.Duration
	CompactionMinBlocks int
	Clock               func() time.Time
}

type Config struct {
	Dir string
	Options
}
