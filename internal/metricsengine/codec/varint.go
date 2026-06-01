package codec

import (
	"encoding/binary"
	"errors"
	"io"
)

func ZigZagEncode(v int64) uint64 {
	return uint64(v<<1) ^ uint64(v>>63)
}

func ZigZagDecode(v uint64) int64 {
	return int64(v>>1) ^ -int64(v&1)
}

func EncodeSignedVarints(values []int64) []byte {
	buf := make([]byte, 0, len(values))
	var tmp [binary.MaxVarintLen64]byte
	for _, value := range values {
		n := binary.PutUvarint(tmp[:], ZigZagEncode(value))
		buf = append(buf, tmp[:n]...)
	}
	return buf
}

func DecodeSignedVarints(payload []byte, count int) ([]int64, error) {
	return DecodeSignedVarintsInto(payload, count, nil)
}

func DecodeSignedVarintsInto(payload []byte, count int, out []int64) ([]int64, error) {
	if cap(out) < count {
		out = make([]int64, 0, count)
	}
	out = out[:0]
	for len(payload) > 0 && len(out) < count {
		value, n := binary.Uvarint(payload)
		if n <= 0 {
			return nil, errors.New("codec: invalid signed varint payload")
		}
		out = append(out, ZigZagDecode(value))
		payload = payload[n:]
	}
	if len(out) != count {
		return nil, io.ErrUnexpectedEOF
	}
	if len(payload) != 0 {
		return nil, errors.New("codec: trailing bytes after signed varint stream")
	}
	return out, nil
}
