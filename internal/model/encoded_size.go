package model

import "fmt"

const (
	maxSmallStringBytes = int(^uint16(0))
	maxLargeStringBytes = uint64(^uint32(0))
)

// EncodedSize returns the exact number of bytes WriteTo would emit, while
// validating every length-prefixed field without allocating the encoded form.
func (e LogEntry) EncodedSize() (uint64, error) {
	if err := validateSmallString("service", e.Service); err != nil {
		return 0, err
	}
	if err := validateSmallString("host", e.Host); err != nil {
		return 0, err
	}
	if uint64(len(e.Body)) > maxLargeStringBytes {
		return 0, fmt.Errorf("model: body too long: %d bytes", len(e.Body))
	}
	if len(e.Attrs) > int(^uint16(0)) {
		return 0, fmt.Errorf("model: too many attrs: %d", len(e.Attrs))
	}

	// Fixed fields and prefixes: id(16), timestamp(8), level(1), two u16
	// strings, trace(16), span(8), body u32 prefix and attr-count u16.
	size := uint64(59 + len(e.Service) + len(e.Host) + len(e.Body))
	for i, attr := range e.Attrs {
		if len(attr.Key) > maxSmallStringBytes {
			return 0, fmt.Errorf("model: attr[%d] key too long: %d bytes", i, len(attr.Key))
		}
		if len(attr.Value) > maxSmallStringBytes {
			return 0, fmt.Errorf("model: attr[%d] value too long: %d bytes", i, len(attr.Value))
		}
		size += uint64(4 + len(attr.Key) + len(attr.Value))
	}
	return size, nil
}

// EncodedSize returns the exact number of bytes WriteTo would emit, while
// validating every length-prefixed field without allocating the encoded form.
func (s SpanEntry) EncodedSize() (uint64, error) {
	if err := validateSmallString("service", s.Service); err != nil {
		return 0, err
	}
	if err := validateSmallString("operation", s.Operation); err != nil {
		return 0, err
	}
	if len(s.Attrs) > int(^uint16(0)) {
		return 0, fmt.Errorf("model: too many span attrs: %d", len(s.Attrs))
	}

	// Fixed fields and prefixes: IDs(48), two u16 strings, two timestamps,
	// status and attr-count.
	size := uint64(71 + len(s.Service) + len(s.Operation))
	for i, attr := range s.Attrs {
		if len(attr.Key) > maxSmallStringBytes {
			return 0, fmt.Errorf("model: span attr[%d] key too long: %d bytes", i, len(attr.Key))
		}
		if len(attr.Value) > maxSmallStringBytes {
			return 0, fmt.Errorf("model: span attr[%d] value too long: %d bytes", i, len(attr.Value))
		}
		size += uint64(4 + len(attr.Key) + len(attr.Value))
	}
	return size, nil
}

func validateSmallString(field, value string) error {
	if len(value) > maxSmallStringBytes {
		return fmt.Errorf("model: %s too long: %d bytes", field, len(value))
	}
	return nil
}
