package model

import (
	"encoding/binary"
	"fmt"
	"slices"
)

// AppendTo appends e in Amber's binary record format to dst.
//
// Unlike WriteTo, AppendTo doesn't route small fixed-width fields through an
// io.Writer interface, so their temporary buffers stay out of the heap. The
// returned slice may share dst's backing array.
func (e *LogEntry) AppendTo(dst []byte) ([]byte, error) {
	size, err := e.EncodedSize()
	if err != nil {
		return dst, err
	}
	if size > uint64(maxInt-len(dst)) {
		return dst, fmt.Errorf("model: encoded log size %d overflows destination", size)
	}

	dst = slices.Grow(dst, int(size))
	dst = append(dst, e.ID[:]...)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(e.Timestamp.UnixNano()))
	dst = append(dst, byte(e.Level))
	dst = appendSmallString(dst, e.Service)
	dst = appendSmallString(dst, e.Host)
	dst = append(dst, e.TraceID[:]...)
	dst = append(dst, e.SpanID[:]...)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Body)))
	dst = append(dst, e.Body...)
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(e.Attrs)))
	for _, attr := range e.Attrs {
		dst = appendSmallString(dst, attr.Key)
		dst = appendSmallString(dst, attr.Value)
	}
	return dst, nil
}

// AppendTo appends s in Amber's binary span record format to dst.
func (s *SpanEntry) AppendTo(dst []byte) ([]byte, error) {
	size, err := s.EncodedSize()
	if err != nil {
		return dst, err
	}
	if size > uint64(maxInt-len(dst)) {
		return dst, fmt.Errorf("model: encoded span size %d overflows destination", size)
	}

	dst = slices.Grow(dst, int(size))
	dst = append(dst, s.ID[:]...)
	dst = append(dst, s.TraceID[:]...)
	dst = append(dst, s.SpanID[:]...)
	dst = append(dst, s.ParentID[:]...)
	dst = appendSmallString(dst, s.Service)
	dst = appendSmallString(dst, s.Operation)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(s.StartTime.UnixNano()))
	dst = binary.LittleEndian.AppendUint64(dst, uint64(s.EndTime.UnixNano()))
	dst = append(dst, byte(s.Status))
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(s.Attrs)))
	for _, attr := range s.Attrs {
		dst = appendSmallString(dst, attr.Key)
		dst = appendSmallString(dst, attr.Value)
	}
	return dst, nil
}

func appendSmallString(dst []byte, value string) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(value)))
	return append(dst, value...)
}

const maxInt = int(^uint(0) >> 1)
