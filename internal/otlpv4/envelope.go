// Package otlpv4 defines Amber's lossless OTLP replay envelope.
package otlpv4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"reflect"

	collectorlogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	collectormetrics "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"google.golang.org/protobuf/proto"
)

const (
	magic         = "AOT4"
	headerSize    = 16
	FormatVersion = uint16(4)
	// MaxPayloadBytes leaves room for the envelope header and the storage
	// manager's event-time prefix within its 64 MiB WAL record limit.
	MaxPayloadBytes = (64 << 20) - headerSize - 8
)

// Signal identifies the OTLP collector request encoded in an Envelope.
type Signal uint8

const (
	SignalLogs Signal = iota + 1
	SignalTraces
	SignalMetrics
)

func (s Signal) String() string {
	switch s {
	case SignalLogs:
		return "logs"
	case SignalTraces:
		return "traces"
	case SignalMetrics:
		return "metrics"
	default:
		return "unknown"
	}
}

// Fidelity states whether Payload is accepted original OTLP or a deterministic
// normalized native-ingest projection.
type Fidelity uint8

const (
	FidelityOTLP Fidelity = 1
	// FidelityNormalizedV3 marks a deterministic, lossy OTLP projection of
	// records retained by Amber v0.3.0. It is not the original accepted OTLP.
	FidelityNormalizedV3     Fidelity = 2
	FidelityNormalizedNative Fidelity = 3
)

// Envelope contains one deterministic protobuf OTLP Export request. Payload is
// intentionally private so callers cannot invalidate a verified envelope.
type Envelope struct {
	signal   Signal
	fidelity Fidelity
	payload  []byte
}

// New creates an envelope from the collector request type implied by signal.
// Deterministic protobuf encoding preserves all known and unknown fields.
func New(signal Signal, fidelity Fidelity, request proto.Message) (Envelope, error) {
	if err := validateSignal(signal); err != nil {
		return Envelope{}, err
	}
	if err := validateFidelity(fidelity); err != nil {
		return Envelope{}, err
	}
	if isNilMessage(request) {
		return Envelope{}, errors.New("otlpv4: nil request")
	}
	if !requestMatchesSignal(signal, request) {
		return Envelope{}, fmt.Errorf("otlpv4: signal %s does not match request %T", signal, request)
	}
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return Envelope{}, fmt.Errorf("otlpv4: marshal %s request: %w", signal, err)
	}
	if len(payload) > MaxPayloadBytes {
		return Envelope{}, fmt.Errorf("otlpv4: payload is too large: %d bytes", len(payload))
	}
	return Envelope{signal: signal, fidelity: fidelity, payload: payload}, nil
}

// marshalRequest writes a request directly into its final envelope buffer.
// AppendRequest uses this path to avoid allocating a standalone protobuf
// payload and then copying it into a second allocation in MarshalBinary.
func marshalRequest(signal Signal, fidelity Fidelity, request proto.Message) ([]byte, error) {
	if err := validateSignal(signal); err != nil {
		return nil, err
	}
	if err := validateFidelity(fidelity); err != nil {
		return nil, err
	}
	if isNilMessage(request) {
		return nil, errors.New("otlpv4: nil request")
	}
	if !requestMatchesSignal(signal, request) {
		return nil, fmt.Errorf("otlpv4: signal %s does not match request %T", signal, request)
	}

	marshalOptions := proto.MarshalOptions{Deterministic: true}
	payloadSize := marshalOptions.Size(request)
	if payloadSize > MaxPayloadBytes {
		return nil, fmt.Errorf("otlpv4: payload is too large: %d bytes", payloadSize)
	}
	out := make([]byte, headerSize, headerSize+payloadSize)
	var err error
	marshalOptions.UseCachedSize = true
	out, err = marshalOptions.MarshalAppend(out, request)
	if err != nil {
		return nil, fmt.Errorf("otlpv4: marshal %s request: %w", signal, err)
	}
	payloadSize = len(out) - headerSize
	if payloadSize > MaxPayloadBytes {
		return nil, fmt.Errorf("otlpv4: payload is too large: %d bytes", payloadSize)
	}

	copy(out[:4], magic)
	binary.LittleEndian.PutUint16(out[4:6], FormatVersion)
	out[6] = byte(signal)
	out[7] = byte(fidelity)
	binary.LittleEndian.PutUint32(out[8:12], uint32(payloadSize))
	checksum := crc32.Update(0, crc32.IEEETable, out[:12])
	checksum = crc32.Update(checksum, crc32.IEEETable, out[headerSize:])
	binary.LittleEndian.PutUint32(out[12:16], checksum)
	return out, nil
}

func (e Envelope) Signal() Signal { return e.signal }

func (e Envelope) Fidelity() Fidelity { return e.fidelity }

func (e Envelope) PayloadSize() int { return len(e.payload) }

// MarshalBinary writes the self-describing AOT4 envelope:
//
//	magic[4] | version[2] | signal[1] | fidelity[1] | payload_len[4] |
//	crc32(header_without_crc + payload)[4] | deterministic protobuf payload
func (e Envelope) MarshalBinary() ([]byte, error) {
	if err := validateSignal(e.signal); err != nil {
		return nil, err
	}
	if err := validateFidelity(e.fidelity); err != nil {
		return nil, err
	}
	if len(e.payload) > MaxPayloadBytes {
		return nil, fmt.Errorf("otlpv4: payload is too large: %d bytes", len(e.payload))
	}
	out := make([]byte, headerSize+len(e.payload))
	copy(out[:4], magic)
	binary.LittleEndian.PutUint16(out[4:6], FormatVersion)
	out[6] = byte(e.signal)
	out[7] = byte(e.fidelity)
	binary.LittleEndian.PutUint32(out[8:12], uint32(len(e.payload)))
	copy(out[headerSize:], e.payload)
	checksum := crc32.Update(0, crc32.IEEETable, out[:12])
	checksum = crc32.Update(checksum, crc32.IEEETable, e.payload)
	binary.LittleEndian.PutUint32(out[12:16], checksum)
	return out, nil
}

// Parse validates and copies one AOT4 envelope.
func Parse(data []byte) (Envelope, error) {
	if len(data) < headerSize {
		return Envelope{}, errors.New("otlpv4: truncated envelope")
	}
	if string(data[:4]) != magic {
		return Envelope{}, errors.New("otlpv4: bad magic")
	}
	if version := binary.LittleEndian.Uint16(data[4:6]); version != FormatVersion {
		return Envelope{}, fmt.Errorf("otlpv4: unsupported format version %d", version)
	}
	signal := Signal(data[6])
	if err := validateSignal(signal); err != nil {
		return Envelope{}, err
	}
	fidelity := Fidelity(data[7])
	if err := validateFidelity(fidelity); err != nil {
		return Envelope{}, err
	}
	payloadLen := uint64(binary.LittleEndian.Uint32(data[8:12]))
	if payloadLen > MaxPayloadBytes || payloadLen != uint64(len(data)-headerSize) {
		return Envelope{}, errors.New("otlpv4: invalid payload length")
	}
	wantChecksum := binary.LittleEndian.Uint32(data[12:16])
	checksum := crc32.Update(0, crc32.IEEETable, data[:12])
	checksum = crc32.Update(checksum, crc32.IEEETable, data[headerSize:])
	if checksum != wantChecksum {
		return Envelope{}, errors.New("otlpv4: checksum mismatch")
	}
	payload := append([]byte(nil), data[headerSize:]...)
	envelope := Envelope{signal: signal, fidelity: fidelity, payload: payload}
	if _, err := envelope.Request(); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

// Request decodes a fresh collector request of the envelope's signal type.
func (e Envelope) Request() (proto.Message, error) {
	var request proto.Message
	switch e.signal {
	case SignalLogs:
		request = &collectorlogs.ExportLogsServiceRequest{}
	case SignalTraces:
		request = &collectortrace.ExportTraceServiceRequest{}
	case SignalMetrics:
		request = &collectormetrics.ExportMetricsServiceRequest{}
	default:
		return nil, fmt.Errorf("otlpv4: unsupported signal %d", e.signal)
	}
	if err := proto.Unmarshal(e.payload, request); err != nil {
		return nil, fmt.Errorf("otlpv4: decode %s request: %w", e.signal, err)
	}
	return request, nil
}

func requestMatchesSignal(signal Signal, request proto.Message) bool {
	switch signal {
	case SignalLogs:
		_, ok := request.(*collectorlogs.ExportLogsServiceRequest)
		return ok
	case SignalTraces:
		_, ok := request.(*collectortrace.ExportTraceServiceRequest)
		return ok
	case SignalMetrics:
		_, ok := request.(*collectormetrics.ExportMetricsServiceRequest)
		return ok
	default:
		return false
	}
}

func isNilMessage(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func validateSignal(signal Signal) error {
	if signal < SignalLogs || signal > SignalMetrics {
		return fmt.Errorf("otlpv4: unsupported signal %d", signal)
	}
	return nil
}

func validateFidelity(fidelity Fidelity) error {
	if fidelity != FidelityOTLP && fidelity != FidelityNormalizedV3 && fidelity != FidelityNormalizedNative {
		return fmt.Errorf("otlpv4: unsupported fidelity %d", fidelity)
	}
	return nil
}
