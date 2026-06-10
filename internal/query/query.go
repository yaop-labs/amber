package query

import (
	"fmt"
	"time"

	"github.com/yaop-labs/amber/internal/model"
)

const (
	DefaultLimit = 100
	MaxLogLimit  = 100_000
	MaxSpanLimit = 100_000
)

type LogQuery struct {
	From     time.Time
	To       time.Time
	Services []string
	Hosts    []string
	Levels   []string
	Attrs    map[string]string
	FullText string
	TraceID  model.TraceID
	Limit    int

	// Cursor, if non-empty, resumes pagination after the (timestamp, entry_id)
	// pair it encodes. Decoded via DecodeCursor. The empty cursor means "start
	// from the newest matching record".
	Cursor string
}

func (q *LogQuery) Validate() error {
	if q.Limit < 0 {
		return fmt.Errorf("query: limit cannot be negative")
	}
	if !q.From.IsZero() && !q.To.IsZero() && q.From.After(q.To) {
		return fmt.Errorf("query: from cannot be after to")
	}
	if q.Limit == 0 {
		q.Limit = DefaultLimit
	}
	if q.Limit > MaxLogLimit {
		return fmt.Errorf("query: limit cannot exceed %d", MaxLogLimit)
	}
	if _, err := DecodeCursor(q.Cursor); err != nil {
		return fmt.Errorf("query: %w", err)
	}
	return nil
}

func (q *LogQuery) HasTimeRange() bool {
	return !q.From.IsZero() || !q.To.IsZero()
}

func (q *LogQuery) HasFieldFilters() bool {
	return len(q.Services) > 0 || len(q.Hosts) > 0 || len(q.Levels) > 0 || len(q.Attrs) > 0
}

func (q *LogQuery) HasFullText() bool {
	return q.FullText != ""
}

func (q *LogQuery) FromUnixNano() int64 {
	if q.From.IsZero() {
		return 0
	}
	return q.From.UnixNano()
}

func (q *LogQuery) ToUnixNano() int64 {
	if q.To.IsZero() {
		return int64(^uint64(0) >> 1)
	}
	return q.To.UnixNano()
}

type LogResult struct {
	Entries   []model.LogEntry
	TotalHits int
	Truncated bool

	// NextCursor, if non-empty, is the opaque token to pass back as
	// LogQuery.Cursor to fetch the next page. Empty when fewer results
	// were available than Limit (last page).
	NextCursor string `json:"next_cursor,omitempty"`

	SegTotal   int  `json:"seg_total,omitempty"`
	SegScanned int  `json:"seg_scanned,omitempty"`
	CacheHit   bool `json:"cache_hit,omitempty"`
}

type SpanQuery struct {
	From        time.Time
	To          time.Time
	Services    []string
	Operations  []string
	TraceID     model.TraceID
	MinDuration time.Duration
	MaxDuration time.Duration
	Statuses    []model.SpanStatus
	Limit       int

	// Cursor, if non-empty, resumes pagination after the (start_time, entry_id)
	// pair it encodes. See DecodeCursor.
	Cursor string
}

func (q *SpanQuery) Validate() error {
	if q.Limit < 0 {
		return fmt.Errorf("query: span limit cannot be negative")
	}
	if !q.From.IsZero() && !q.To.IsZero() && q.From.After(q.To) {
		return fmt.Errorf("query: span from cannot be after to")
	}
	if q.Limit == 0 {
		q.Limit = DefaultLimit
	}
	if q.Limit > MaxSpanLimit {
		return fmt.Errorf("query: span limit cannot exceed %d", MaxSpanLimit)
	}
	if _, err := DecodeCursor(q.Cursor); err != nil {
		return fmt.Errorf("query: %w", err)
	}
	return nil
}

type SpanResult struct {
	Spans     []model.SpanEntry
	TotalHits int
	Truncated bool

	NextCursor string `json:"next_cursor,omitempty"`
}
