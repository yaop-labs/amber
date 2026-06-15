package codec

import "errors"

// TimestampStrategy identifies how a series' timestamps were encoded.
// EncodeTimestamps picks the one that yields the smallest payload.
type TimestampStrategy uint8

const (
	TimestampStrategyDeltaOfDelta TimestampStrategy = iota + 1
	TimestampStrategyRegular
	// TimestampStrategyDeltaOfDeltaPacked carries the same delta-of-delta
	// stream with a fixed-width bit-packed payload (bitpack.go) — wins on
	// jittered-but-bounded scrape grids where every residual is small.
	TimestampStrategyDeltaOfDeltaPacked
)

func (s TimestampStrategy) String() string {
	switch s {
	case TimestampStrategyDeltaOfDelta:
		return "delta_of_delta"
	case TimestampStrategyRegular:
		return "regular"
	case TimestampStrategyDeltaOfDeltaPacked:
		return "delta_of_delta_packed"
	default:
		return "unknown"
	}
}

// TimestampEncoding is the encoded form of one series' timestamps. For the
// regular strategy Base is the first timestamp and Step the fixed interval; for
// the packed delta-of-delta strategy Base is the first timestamp and Step the
// first delta, with Payload holding the remaining residuals.
type TimestampEncoding struct {
	Strategy TimestampStrategy
	Count    int
	Base     int64
	Step     int64
	Payload  []byte
}

// EncodeTimestamps takes a fixed-interval fast path (Base+Step, no payload) for
// regularly spaced timestamps, and otherwise delta-of-delta encodes them as
// varints or, when smaller, a bit-packed residual stream.
func EncodeTimestamps(timestamps []int64) TimestampEncoding {
	if isRegular(timestamps) {
		var step int64
		if len(timestamps) > 1 {
			step = timestamps[1] - timestamps[0]
		}
		var base int64
		if len(timestamps) > 0 {
			base = timestamps[0]
		}
		return TimestampEncoding{
			Strategy: TimestampStrategyRegular,
			Count:    len(timestamps),
			Base:     base,
			Step:     step,
		}
	}

	transformed := make([]int64, len(timestamps))
	switch len(timestamps) {
	case 0:
	case 1:
		transformed[0] = timestamps[0]
	default:
		transformed[0] = timestamps[0]
		prevDelta := timestamps[1] - timestamps[0]
		transformed[1] = prevDelta
		for i := 2; i < len(timestamps); i++ {
			delta := timestamps[i] - timestamps[i-1]
			transformed[i] = delta - prevDelta
			prevDelta = delta
		}
	}
	varint := EncodeSignedVarints(transformed)
	// Packed candidate: transformed[0] (absolute first timestamp) and
	// transformed[1] (first delta) would poison the fixed width, so they ride
	// in the Base/Step fields and only the dod residuals are packed.
	// isRegular returns true for len ≤ 2, so this branch always has ≥ 3.
	packed := EncodeBitPacked(transformed[2:])
	if len(packed) < len(varint) {
		return TimestampEncoding{
			Strategy: TimestampStrategyDeltaOfDeltaPacked,
			Count:    len(timestamps),
			Base:     transformed[0],
			Step:     transformed[1],
			Payload:  packed,
		}
	}
	return TimestampEncoding{
		Strategy: TimestampStrategyDeltaOfDelta,
		Count:    len(timestamps),
		Payload:  varint,
	}
}

// DecodeTimestamps reverses EncodeTimestamps.
func DecodeTimestamps(enc TimestampEncoding) ([]int64, error) {
	if enc.Strategy == TimestampStrategyRegular {
		out := make([]int64, enc.Count)
		for i := range out {
			out[i] = enc.Base + int64(i)*enc.Step
		}
		return out, nil
	}
	var values []int64
	var err error
	if enc.Strategy == TimestampStrategyDeltaOfDeltaPacked {
		if enc.Count < 3 {
			return nil, errors.New("codec: packed timestamp encoding requires at least 3 samples")
		}
		residuals, derr := DecodeBitPackedInto(enc.Payload, enc.Count-2, nil)
		if derr != nil {
			return nil, derr
		}
		values = make([]int64, enc.Count)
		values[0] = enc.Base
		values[1] = enc.Step
		copy(values[2:], residuals)
	} else {
		values, err = DecodeSignedVarints(enc.Payload, enc.Count)
	}
	if err != nil {
		return nil, err
	}
	switch len(values) {
	case 0:
		return values, nil
	case 1:
		return values, nil
	default:
		out := make([]int64, len(values))
		out[0] = values[0]
		prevDelta := values[1]
		out[1] = out[0] + prevDelta
		for i := 2; i < len(values); i++ {
			delta := prevDelta + values[i]
			out[i] = out[i-1] + delta
			prevDelta = delta
		}
		return out, nil
	}
}

func isRegular(timestamps []int64) bool {
	if len(timestamps) <= 2 {
		return true
	}
	step := timestamps[1] - timestamps[0]
	for i := 2; i < len(timestamps); i++ {
		if timestamps[i]-timestamps[i-1] != step {
			return false
		}
	}
	return true
}
