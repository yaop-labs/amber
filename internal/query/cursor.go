package query

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/yaop-labs/amber/internal/model"
)

// Cursor encodes the (timestamp, entry_id) pair after which the next page
// starts. The encoding is intentionally opaque: clients pass NextCursor
// from a previous response back as Query.Cursor; they should not parse it.
//
// On-wire layout (24 bytes raw → URL-safe base64 without padding):
//
//	[0:8]   int64 timestamp (UnixNano), big-endian
//	[8:24]  entry ID (16 bytes, ULID-shaped)
//
// Big-endian + the ULID layout (millisecond timestamp + monotonic suffix)
// means lexicographic byte comparison of the raw bytes matches the
// (timestamp, id) ordering the executor's min-heap uses — useful for
// debug but not relied upon by the executor.
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

// EncodeCursor returns the opaque token for a (timestamp, id) pair, or
// the empty string if the cursor is the zero value. Empty input → empty
// output simplifies the "no more pages" case.
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
	// RawURLEncoding rejects padding outright; a token with '=' is from a
	// wrong encoder and we want a clear error rather than silent partial
	// decode.
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

// afterCursor reports whether (ts, id) strictly orders after c. The pair
// (ts, id) > (c.Timestamp, c.EntryID) — same ordering the result min-heap
// uses (descending timestamp; within a timestamp, descending entry id).
// Because we paginate from newest to oldest, "after" means OLDER than the
// cursor.
func (c Cursor) After(ts int64, id model.EntryID) bool {
	if ts != c.Timestamp {
		return ts < c.Timestamp
	}
	// Same timestamp: compare IDs lexicographically. The cursor points to
	// the last record of the previous page; the next page must skip it.
	for i := 0; i < len(id); i++ {
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
