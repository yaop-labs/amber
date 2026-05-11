// Package model defines the on-wire and on-disk shapes for log and span entries.
package model

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

type Level uint8

const (
	LevelTrace Level = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func (l Level) String() string {
	switch l {
	case LevelTrace:
		return "TRACE"
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

func (l Level) MarshalJSON() ([]byte, error) {
	return []byte(`"` + l.String() + `"`), nil
}

func LevelFromString(s string) (Level, error) {
	switch s {
	case "TRACE", "trace":
		return LevelTrace, nil
	case "DEBUG", "debug":
		return LevelDebug, nil
	case "INFO", "info":
		return LevelInfo, nil
	case "WARN", "warn", "WARNING", "warning":
		return LevelWarn, nil
	case "ERROR", "error":
		return LevelError, nil
	case "FATAL", "fatal":
		return LevelFatal, nil
	default:
		return LevelInfo, fmt.Errorf("model: unknown level %q", s)
	}
}

type Attr struct {
	Key   string
	Value string
}

type LogEntry struct {
	ID        EntryID
	Timestamp time.Time
	Level     Level
	Service   string
	Host      string
	TraceID   TraceID
	SpanID    SpanID
	Body      string
	Attrs     []Attr
}

func NewLogEntry(level Level, service, host, body string, attrs ...Attr) (LogEntry, error) {
	id, err := NewEntryID()
	if err != nil {
		return LogEntry{}, fmt.Errorf("model: new log entry: %w", err)
	}

	return LogEntry{
		ID:        id,
		Timestamp: time.Now(),
		Level:     level,
		Service:   service,
		Host:      host,
		Body:      body,
		Attrs:     attrs,
	}, nil
}

func (e *LogEntry) WriteTo(w io.Writer) (int64, error) {
	var n int64

	nn, err := w.Write(e.ID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write entry id: %w", err)
	}

	var ts [8]byte
	binary.LittleEndian.PutUint64(ts[:], uint64(e.Timestamp.UnixNano()))
	nn, err = w.Write(ts[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write entry timestamp: %w", err)
	}

	nn, err = w.Write([]byte{byte(e.Level)})
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write entry level: %w", err)
	}

	n2, err := writeString(w, e.Service)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: write entry service: %w", err)
	}

	n2, err = writeString(w, e.Host)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: write entry host: %w", err)
	}

	nn, err = w.Write(e.TraceID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write entry trace_id: %w", err)
	}

	nn, err = w.Write(e.SpanID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write entry span_id: %w", err)
	}

	n2, err = writeLargeString(w, e.Body)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: write entry body: %w", err)
	}

	if len(e.Attrs) > 65535 {
		return n, fmt.Errorf("model: too many attrs: %d", len(e.Attrs))
	}
	var attrCount [2]byte
	binary.LittleEndian.PutUint16(attrCount[:], uint16(len(e.Attrs)))
	nn, err = w.Write(attrCount[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write entry attrs count: %w", err)
	}

	for i, attr := range e.Attrs {
		n2, err = writeString(w, attr.Key)
		n += n2
		if err != nil {
			return n, fmt.Errorf("model: write attr[%d] key: %w", i, err)
		}
		n2, err = writeString(w, attr.Value)
		n += n2
		if err != nil {
			return n, fmt.Errorf("model: write attr[%d] value: %w", i, err)
		}
	}

	return n, nil
}

func (e *LogEntry) ReadFrom(r io.Reader) (int64, error) {
	var n int64

	nn, err := io.ReadFull(r, e.ID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read entry id: %w", err)
	}

	var ts [8]byte
	nn, err = io.ReadFull(r, ts[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read entry timestamp: %w", err)
	}
	e.Timestamp = time.Unix(0, int64(binary.LittleEndian.Uint64(ts[:])))

	var level [1]byte
	nn, err = io.ReadFull(r, level[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read entry level: %w", err)
	}
	e.Level = Level(level[0])

	var n2 int64
	e.Service, n2, err = readString(r)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: read entry service: %w", err)
	}

	e.Host, n2, err = readString(r)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: read entry host: %w", err)
	}

	nn, err = io.ReadFull(r, e.TraceID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read entry trace_id: %w", err)
	}

	nn, err = io.ReadFull(r, e.SpanID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read entry span_id: %w", err)
	}

	e.Body, n2, err = readLargeString(r)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: read entry body: %w", err)
	}

	var attrCount [2]byte
	nn, err = io.ReadFull(r, attrCount[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read entry attrs count: %w", err)
	}
	count := int(binary.LittleEndian.Uint16(attrCount[:]))

	if count > 0 {
		e.Attrs = make([]Attr, count)
		for i := range e.Attrs {
			e.Attrs[i].Key, n2, err = readString(r)
			n += n2
			if err != nil {
				return n, fmt.Errorf("model: read attr[%d] key: %w", i, err)
			}
			e.Attrs[i].Value, n2, err = readString(r)
			n += n2
			if err != nil {
				return n, fmt.Errorf("model: read attr[%d] value: %w", i, err)
			}
		}
	}

	return n, nil
}

// writeString writes len-prefixed string. io.WriteString routes to
// w.WriteString if w implements io.StringWriter (the bufPool *bytes.Buffer
// does), avoiding the []byte(s) heap copy that `w.Write([]byte(s))` would
// force. Caller paths in processBatch always use *bytes.Buffer.
func writeString(w io.Writer, s string) (int64, error) {
	if len(s) > 65535 {
		return 0, fmt.Errorf("string too long: %d bytes", len(s))
	}
	var lenBuf [2]byte
	binary.LittleEndian.PutUint16(lenBuf[:], uint16(len(s)))
	nn, err := w.Write(lenBuf[:])
	n := int64(nn)
	if err != nil {
		return n, err
	}
	nn, err = io.WriteString(w, s)
	n += int64(nn)
	return n, err
}

func writeLargeString(w io.Writer, s string) (int64, error) {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(s)))
	nn, err := w.Write(lenBuf[:])
	n := int64(nn)
	if err != nil {
		return n, err
	}
	nn, err = io.WriteString(w, s)
	n += int64(nn)
	return n, err
}

var errShortRecord = fmt.Errorf("model: record too short")

func readStringBytes(data []byte, off int) (string, int, error) {
	if off+2 > len(data) {
		return "", off, errShortRecord
	}
	length := int(binary.LittleEndian.Uint16(data[off : off+2]))
	off += 2
	if length == 0 {
		return "", off, nil
	}
	if off+length > len(data) {
		return "", off, errShortRecord
	}
	s := string(data[off : off+length])
	return s, off + length, nil
}

func readLargeStringBytes(data []byte, off int) (string, int, error) {
	if off+4 > len(data) {
		return "", off, errShortRecord
	}
	length := int(binary.LittleEndian.Uint32(data[off : off+4]))
	off += 4
	if length == 0 {
		return "", off, nil
	}
	if off+length > len(data) {
		return "", off, errShortRecord
	}
	s := string(data[off : off+length])
	return s, off + length, nil
}

func (e *LogEntry) DecodeBytes(data []byte) error {
	off := 0

	if off+16 > len(data) {
		return errShortRecord
	}
	copy(e.ID[:], data[off:off+16])
	off += 16

	if off+8 > len(data) {
		return errShortRecord
	}
	e.Timestamp = time.Unix(0, int64(binary.LittleEndian.Uint64(data[off:off+8])))
	off += 8

	if off+1 > len(data) {
		return errShortRecord
	}
	e.Level = Level(data[off])
	off++

	var err error
	if e.Service, off, err = readStringBytes(data, off); err != nil {
		return err
	}
	if e.Host, off, err = readStringBytes(data, off); err != nil {
		return err
	}

	if off+16 > len(data) {
		return errShortRecord
	}
	copy(e.TraceID[:], data[off:off+16])
	off += 16

	if off+8 > len(data) {
		return errShortRecord
	}
	copy(e.SpanID[:], data[off:off+8])
	off += 8

	if e.Body, off, err = readLargeStringBytes(data, off); err != nil {
		return err
	}

	if off+2 > len(data) {
		return errShortRecord
	}
	count := int(binary.LittleEndian.Uint16(data[off : off+2]))
	off += 2

	if count > 0 {
		e.Attrs = make([]Attr, count)
		for i := range e.Attrs {
			if e.Attrs[i].Key, off, err = readStringBytes(data, off); err != nil {
				return err
			}
			if e.Attrs[i].Value, off, err = readStringBytes(data, off); err != nil {
				return err
			}
		}
	} else {
		e.Attrs = nil
	}

	return nil
}

func readString(r io.Reader) (string, int64, error) {
	var lenBuf [2]byte
	nn, err := io.ReadFull(r, lenBuf[:])
	n := int64(nn)
	if err != nil {
		return "", n, err
	}
	length := int(binary.LittleEndian.Uint16(lenBuf[:]))
	if length == 0 {
		return "", n, nil
	}
	buf := make([]byte, length)
	nn, err = io.ReadFull(r, buf)
	n += int64(nn)
	return string(buf), n, err
}

func readLargeString(r io.Reader) (string, int64, error) {
	var lenBuf [4]byte
	nn, err := io.ReadFull(r, lenBuf[:])
	n := int64(nn)
	if err != nil {
		return "", n, err
	}
	length := int(binary.LittleEndian.Uint32(lenBuf[:]))
	if length == 0 {
		return "", n, nil
	}
	buf := make([]byte, length)
	nn, err = io.ReadFull(r, buf)
	n += int64(nn)
	return string(buf), n, err
}
