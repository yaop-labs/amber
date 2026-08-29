package engine

import (
	"encoding/binary"
	"errors"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

const maxRecordSize = 16 << 20

type recordKind uint8

const (
	kindSeries         recordKind = 1
	kindSample         recordKind = 2
	kindLegacySample   recordKind = 3
	kindSketchExp      recordKind = 4
	kindSketchExplicit recordKind = 5
)

type record struct {
	kind      recordKind
	id        uint64
	labels    model.LabelSet
	typ       model.MetricType
	timestamp int64
	value     int64
	payload   []byte
}

// encodeRecord returns only the metrics payload. The shared WAL owns the
// framing, CRC, sequence numbers, sync and recovery around this payload.
func encodeRecord(r record) ([]byte, error) {
	var payload []byte
	switch r.kind {
	case kindSeries:
		payload = appendUvarint([]byte{byte(kindSeries)}, r.id)
		payload = appendUvarint(payload, uint64(len(r.labels)))
		for _, label := range r.labels {
			payload = appendString(payload, label.Name)
			payload = appendString(payload, label.Value)
		}
	case kindSample:
		payload = appendUvarint([]byte{byte(kindSample)}, r.id)
		payload = append(payload, byte(r.typ))
		payload = appendZigZag(payload, r.timestamp)
		payload = appendZigZag(payload, r.value)
	case kindSketchExp, kindSketchExplicit:
		payload = appendUvarint([]byte{byte(r.kind)}, r.id)
		payload = appendZigZag(payload, r.timestamp)
		payload = append(payload, r.payload...)
	default:
		return nil, errors.New("engine: unknown WAL record kind")
	}
	if len(payload) > maxRecordSize {
		return nil, errors.New("engine: WAL record too large")
	}
	return payload, nil
}

func decodeRecord(payload []byte) (record, error) {
	if len(payload) == 0 {
		return record{}, errors.New("engine: empty WAL record payload")
	}
	r := byteReader{buf: payload[1:]}
	switch recordKind(payload[0]) {
	case kindSeries:
		rec := record{kind: kindSeries, id: r.uvarint()}
		n := r.uvarint()
		if n > uint64(maxRecordSize) {
			return record{}, errors.New("engine: series record label count out of range")
		}
		labels := make(model.LabelSet, 0, n)
		for i := uint64(0); i < n; i++ {
			labels = append(labels, model.Label{Name: r.str(), Value: r.str()})
		}
		rec.labels = labels
		if r.err != nil || r.remaining() != 0 {
			return record{}, errors.New("engine: malformed series record")
		}
		return rec, nil
	case kindSample:
		rec := record{kind: kindSample, id: r.uvarint(), typ: model.MetricType(r.byte()), timestamp: r.zigzag(), value: r.zigzag()}
		if r.err != nil || r.remaining() != 0 {
			return record{}, errors.New("engine: malformed sample record")
		}
		return rec, nil
	case kindSketchExp, kindSketchExplicit:
		rec := record{kind: recordKind(payload[0]), id: r.uvarint(), timestamp: r.zigzag()}
		if r.err != nil {
			return record{}, errors.New("engine: malformed sketch record")
		}
		rec.payload = append([]byte(nil), r.buf...)
		return rec, nil
	default:
		return record{}, errors.New("engine: unknown WAL record kind")
	}
}

func appendUvarint(buf []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(buf, tmp[:n]...)
}

func appendZigZag(buf []byte, v int64) []byte {
	return appendUvarint(buf, uint64(v<<1)^uint64(v>>63))
}

func appendString(buf []byte, s string) []byte {
	buf = appendUvarint(buf, uint64(len(s)))
	return append(buf, s...)
}

type byteReader struct {
	buf []byte
	err error
}

func (r *byteReader) remaining() int { return len(r.buf) }

func (r *byteReader) uvarint() uint64 {
	if r.err != nil {
		return 0
	}
	v, n := binary.Uvarint(r.buf)
	if n <= 0 {
		r.err = errors.New("engine: bad uvarint")
		return 0
	}
	r.buf = r.buf[n:]
	return v
}

func (r *byteReader) zigzag() int64 {
	v := r.uvarint()
	return int64(v>>1) ^ -int64(v&1)
}

func (r *byteReader) byte() byte {
	if r.err != nil {
		return 0
	}
	if len(r.buf) == 0 {
		r.err = errors.New("engine: short record")
		return 0
	}
	b := r.buf[0]
	r.buf = r.buf[1:]
	return b
}

func (r *byteReader) str() string {
	n := r.uvarint()
	if r.err != nil {
		return ""
	}
	if n > uint64(len(r.buf)) {
		r.err = errors.New("engine: string length out of range")
		return ""
	}
	s := string(r.buf[:n])
	r.buf = r.buf[n:]
	return s
}
