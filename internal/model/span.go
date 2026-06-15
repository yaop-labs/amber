package model

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"
)

// SpanStatus is the OTLP span status code.
type SpanStatus uint8

const (
	SpanStatusUnset SpanStatus = iota
	SpanStatusOK
	SpanStatusError
)

func (s SpanStatus) String() string {
	switch s {
	case SpanStatusUnset:
		return "UNSET"
	case SpanStatusOK:
		return "OK"
	case SpanStatusError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// SpanEntry is a single trace span as stored in the WAL and span segments.
type SpanEntry struct {
	ID        EntryID
	TraceID   TraceID
	SpanID    SpanID
	ParentID  SpanID
	Service   string
	Operation string
	StartTime time.Time
	EndTime   time.Time
	Status    SpanStatus
	Attrs     []Attr
}

// NewSpanEntry builds a span with a freshly minted ID and StartTime set to now.
func NewSpanEntry(traceID TraceID, spanID SpanID, parentID SpanID, service, operation string) (SpanEntry, error) {
	id, err := NewEntryID()
	if err != nil {
		return SpanEntry{}, fmt.Errorf("model: new span entry: %w", err)
	}

	return SpanEntry{
		ID:        id,
		TraceID:   traceID,
		SpanID:    spanID,
		ParentID:  parentID,
		Service:   service,
		Operation: operation,
		StartTime: time.Now(),
		Status:    SpanStatusUnset,
	}, nil
}

func (s *SpanEntry) Duration() time.Duration {
	return s.EndTime.Sub(s.StartTime)
}

func (s *SpanEntry) IsRoot() bool {
	return IsZeroSpanID(s.ParentID)
}

// WriteTo encodes the span in amber's binary record format:
//
//	id[16] | trace_id[16] | span_id[8] | parent_id[8] |
//	service[u16-len+bytes] | operation[u16-len+bytes] |
//	start[8 LE unixnano] | end[8 LE unixnano] | status[1] |
//	attr_count[2] | (key[u16-len+bytes], value[u16-len+bytes])*
//
// ReadFrom and DecodeBytes are its inverses.
func (s *SpanEntry) WriteTo(w io.Writer) (int64, error) {
	var n int64

	nn, err := w.Write(s.ID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span id: %w", err)
	}

	nn, err = w.Write(s.TraceID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span trace_id: %w", err)
	}

	nn, err = w.Write(s.SpanID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span span_id: %w", err)
	}

	nn, err = w.Write(s.ParentID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span parent_id: %w", err)
	}

	n2, err := writeString(w, s.Service)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: write span service: %w", err)
	}

	n2, err = writeString(w, s.Operation)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: write span operation: %w", err)
	}

	var timeBuf [8]byte
	binary.LittleEndian.PutUint64(timeBuf[:], uint64(s.StartTime.UnixNano()))
	nn, err = w.Write(timeBuf[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span start_time: %w", err)
	}

	binary.LittleEndian.PutUint64(timeBuf[:], uint64(s.EndTime.UnixNano()))
	nn, err = w.Write(timeBuf[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span end_time: %w", err)
	}

	nn, err = w.Write([]byte{byte(s.Status)})
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span status: %w", err)
	}

	if len(s.Attrs) > 65535 {
		return n, fmt.Errorf("model: too many span attrs: %d", len(s.Attrs))
	}
	var attrCount [2]byte
	binary.LittleEndian.PutUint16(attrCount[:], uint16(len(s.Attrs)))
	nn, err = w.Write(attrCount[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: write span attrs count: %w", err)
	}

	for i, attr := range s.Attrs {
		n2, err = writeString(w, attr.Key)
		n += n2
		if err != nil {
			return n, fmt.Errorf("model: write span attr[%d] key: %w", i, err)
		}
		n2, err = writeString(w, attr.Value)
		n += n2
		if err != nil {
			return n, fmt.Errorf("model: write span attr[%d] value: %w", i, err)
		}
	}

	return n, nil
}

// DecodeBytes decodes a span from an in-memory slice, the read path used by
// segment scans.
func (s *SpanEntry) DecodeBytes(data []byte) error {
	off := 0

	if off+16 > len(data) {
		return errShortRecord
	}
	copy(s.ID[:], data[off:off+16])
	off += 16

	if off+16 > len(data) {
		return errShortRecord
	}
	copy(s.TraceID[:], data[off:off+16])
	off += 16

	if off+8 > len(data) {
		return errShortRecord
	}
	copy(s.SpanID[:], data[off:off+8])
	off += 8

	if off+8 > len(data) {
		return errShortRecord
	}
	copy(s.ParentID[:], data[off:off+8])
	off += 8

	var err error
	if s.Service, off, err = readStringBytes(data, off); err != nil {
		return err
	}
	if s.Operation, off, err = readStringBytes(data, off); err != nil {
		return err
	}

	if off+8 > len(data) {
		return errShortRecord
	}
	s.StartTime = time.Unix(0, int64(binary.LittleEndian.Uint64(data[off:off+8])))
	off += 8

	if off+8 > len(data) {
		return errShortRecord
	}
	s.EndTime = time.Unix(0, int64(binary.LittleEndian.Uint64(data[off:off+8])))
	off += 8

	if off+1 > len(data) {
		return errShortRecord
	}
	s.Status = SpanStatus(data[off])
	off++

	if off+2 > len(data) {
		return errShortRecord
	}
	count := int(binary.LittleEndian.Uint16(data[off : off+2]))
	off += 2

	if count > 0 {
		s.Attrs = make([]Attr, count)
		for i := range s.Attrs {
			if s.Attrs[i].Key, off, err = readStringBytes(data, off); err != nil {
				return err
			}
			if s.Attrs[i].Value, off, err = readStringBytes(data, off); err != nil {
				return err
			}
		}
	} else {
		s.Attrs = nil
	}

	return nil
}

// ReadFrom decodes a span written by WriteTo from a stream.
func (s *SpanEntry) ReadFrom(r io.Reader) (int64, error) {
	var n int64

	nn, err := io.ReadFull(r, s.ID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span id: %w", err)
	}

	nn, err = io.ReadFull(r, s.TraceID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span trace_id: %w", err)
	}

	nn, err = io.ReadFull(r, s.SpanID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span span_id: %w", err)
	}

	nn, err = io.ReadFull(r, s.ParentID[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span parent_id: %w", err)
	}

	var n2 int64
	s.Service, n2, err = readString(r)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: read span service: %w", err)
	}

	s.Operation, n2, err = readString(r)
	n += n2
	if err != nil {
		return n, fmt.Errorf("model: read span operation: %w", err)
	}

	var timeBuf [8]byte
	nn, err = io.ReadFull(r, timeBuf[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span start_time: %w", err)
	}
	s.StartTime = time.Unix(0, int64(binary.LittleEndian.Uint64(timeBuf[:])))

	nn, err = io.ReadFull(r, timeBuf[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span end_time: %w", err)
	}
	s.EndTime = time.Unix(0, int64(binary.LittleEndian.Uint64(timeBuf[:])))

	var status [1]byte
	nn, err = io.ReadFull(r, status[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span status: %w", err)
	}
	s.Status = SpanStatus(status[0])

	var attrCount [2]byte
	nn, err = io.ReadFull(r, attrCount[:])
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("model: read span attrs count: %w", err)
	}
	count := int(binary.LittleEndian.Uint16(attrCount[:]))

	if count > 0 {
		s.Attrs = make([]Attr, count)
		for i := range s.Attrs {
			s.Attrs[i].Key, n2, err = readString(r)
			n += n2
			if err != nil {
				return n, fmt.Errorf("model: read span attr[%d] key: %w", i, err)
			}
			s.Attrs[i].Value, n2, err = readString(r)
			n += n2
			if err != nil {
				return n, fmt.Errorf("model: read span attr[%d] value: %w", i, err)
			}
		}
	}

	return n, nil
}
