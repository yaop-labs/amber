package store

import "time"

// Options configures a Store: flush and compaction triggers, cardinality and
// label limits, retention, and an injectable clock. The zero value disables the
// optional limits and background work; Clock defaults to time.Now.
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

// Config is a Store directory paired with its Options, for OpenConfigured.
type Config struct {
	Dir string
	Options
}
