package query

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/yaop-labs/amber/internal/model"
)

// Cursor encodes the position after which the next page starts.
// Clients should treat the encoded value as opaque.
//
// On-wire layout before base64 encoding:
//
//	[0:8]   int64 timestamp (UnixNano), big-endian
//	[8:24]  entry ID (16 bytes, ULID-shaped)
//
// Big-endian timestamp and ULID bytes preserve the executor ordering for
// debugging, but the executor does not rely on bytewise cursor comparison.
type Cursor struct {
	Timestamp int64
	EntryID   model.EntryID
}

const cursorRawSize = 8 + 16

var (
	errCursorDecode  = errors.New("cursor: decode")
	errCursorLength  = errors.New("cursor: bad length")
	errCursorPadding = errors.New("cursor: unexpected padding")
)

// EncodeCursor returns the opaque token for a cursor.
// The zero cursor encodes to the empty string.
func EncodeCursor(c Cursor) string {
	if c.Timestamp == 0 && c.EntryID == (model.EntryID{}) {
		return ""
	}
	var buf [cursorRawSize]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(c.Timestamp))
	copy(buf[8:24], c.EntryID[:])
	return base64.RawURLEncoding.EncodeToString(buf[:])
}

// DecodeCursor parses an opaque token. Empty input returns the zero
// Cursor with no error (callers treat the zero value as "no cursor").
func DecodeCursor(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return Cursor{}, fmt.Errorf("%w", errCursorPadding)
		}
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return Cursor{}, fmt.Errorf("%w: %v", errCursorDecode, err)
	}
	if len(raw) != cursorRawSize {
		return Cursor{}, fmt.Errorf("%w: got %d bytes, want %d", errCursorLength, len(raw), cursorRawSize)
	}
	var c Cursor
	c.Timestamp = int64(binary.BigEndian.Uint64(raw[0:8]))
	copy(c.EntryID[:], raw[8:24])
	return c, nil
}

// After reports whether (ts, id) belongs after c in newest-first pagination.
func (c Cursor) After(ts int64, id model.EntryID) bool {
	if ts != c.Timestamp {
		return ts < c.Timestamp
	}
	for i := range len(id) {
		if id[i] != c.EntryID[i] {
			return id[i] < c.EntryID[i]
		}
	}
	return false
}

// IsZero reports whether the cursor is unset.
func (c Cursor) IsZero() bool {
	return c.Timestamp == 0 && c.EntryID == (model.EntryID{})
}
