package model

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	mrand "math/rand/v2"
	"time"

	"github.com/oklog/ulid/v2"
)

// EntryID is a ULID: a 16-byte, lexicographically time-ordered record ID.
// TraceID and SpanID are the raw OTLP identifier byte arrays.
type EntryID = ulid.ULID
type TraceID [16]byte
type SpanID [8]byte

// NewEntryID builds a ULID whose first 6 bytes are the current Unix time in
// milliseconds (big-endian) and whose last 10 bytes are random. The time
// prefix makes IDs sort by creation time. The error return is kept for
// interface compatibility; this implementation never fails.
func NewEntryID() (EntryID, error) {
	var id EntryID
	ms := uint64(time.Now().UnixMilli())
	id[0] = byte(ms >> 40)
	id[1] = byte(ms >> 32)
	id[2] = byte(ms >> 24)
	id[3] = byte(ms >> 16)
	id[4] = byte(ms >> 8)
	id[5] = byte(ms)
	binary.BigEndian.PutUint64(id[6:14], mrand.Uint64())
	binary.BigEndian.PutUint16(id[14:16], uint16(mrand.Uint64()))
	return id, nil
}

// MustNewEntryID is NewEntryID for callers that cannot handle an error; it
// panics instead.
func MustNewEntryID() EntryID {
	id, err := NewEntryID()
	if err != nil {
		panic("amber: failed to generate EntryID: " + err.Error())
	}
	return id
}

// EntryIDFromString parses the canonical 26-character ULID encoding.
func EntryIDFromString(s string) (EntryID, error) {
	return ulid.ParseStrict(s)
}

// EntryIDToUint64 derives the 64-bit key used by the bitmap index from bytes
// [2:10]. The high bits carry the millisecond timestamp, so the keys preserve
// the ID's time ordering.
func EntryIDToUint64(id EntryID) uint64 {
	return uint64(id[2])<<56 |
		uint64(id[3])<<48 |
		uint64(id[4])<<40 |
		uint64(id[5])<<32 |
		uint64(id[6])<<24 |
		uint64(id[7])<<16 |
		uint64(id[8])<<8 |
		uint64(id[9])
}

// EntryIDTime recovers the creation time from a ULID's 6-byte millisecond prefix.
func EntryIDTime(id EntryID) time.Time {
	ms := uint64(id[0])<<40 |
		uint64(id[1])<<32 |
		uint64(id[2])<<24 |
		uint64(id[3])<<16 |
		uint64(id[4])<<8 |
		uint64(id[5])
	return time.UnixMilli(int64(ms))
}

func ZeroSpanID() SpanID {
	return SpanID{}
}

func IsZeroTraceID(id TraceID) bool {
	return id == TraceID{}
}

func IsZeroSpanID(id SpanID) bool {
	return id == SpanID{}
}

// MarshalJSON renders the ID as lowercase hex, or as "" when it is the zero
// value so absent trace/span IDs serialize as empty strings.
func (id TraceID) MarshalJSON() ([]byte, error) {
	if id == (TraceID{}) {
		return json.Marshal("")
	}
	return json.Marshal(hex.EncodeToString(id[:]))
}

func (id SpanID) MarshalJSON() ([]byte, error) {
	if id == (SpanID{}) {
		return json.Marshal("")
	}
	return json.Marshal(hex.EncodeToString(id[:]))
}
