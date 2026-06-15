package model

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	mrand "math/rand/v2"
	"time"

	"github.com/oklog/ulid/v2"
)

type EntryID = ulid.ULID
type TraceID [16]byte
type SpanID [8]byte

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

func MustNewEntryID() EntryID {
	id, err := NewEntryID()
	if err != nil {
		panic("amber: failed to generate EntryID: " + err.Error())
	}
	return id
}

func EntryIDFromString(s string) (EntryID, error) {
	return ulid.ParseStrict(s)
}

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
