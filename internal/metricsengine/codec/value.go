package codec

import "errors"

type ValueStrategy uint8

const (
	ValueStrategyDirectResidual ValueStrategy = iota + 1
	ValueStrategyDelta
	ValueStrategyDeltaOfDelta
	ValueStrategyConstant
)

func (s ValueStrategy) String() string {
	switch s {
	case ValueStrategyDirectResidual:
		return "direct_residual"
	case ValueStrategyDelta:
		return "delta"
	case ValueStrategyDeltaOfDelta:
		return "delta_of_delta"
	case ValueStrategyConstant:
		return "constant"
	default:
		return "unknown"
	}
}

type ValueEncoding struct {
	Strategy ValueStrategy
	Count    int
	Base     int64
	Payload  []byte
}

func EncodeIntegerValues(values []int64) ValueEncoding {
	if isConstant(values) {
		var base int64
		if len(values) > 0 {
			base = values[0]
		}
		return ValueEncoding{
			Strategy: ValueStrategyConstant,
			Count:    len(values),
			Base:     base,
		}
	}
	candidates := []ValueEncoding{
		encodeDirectResidual(values),
		encodeDelta(values),
		encodeDeltaOfDelta(values),
	}
	best := candidates[0]
	for _, candidate := range candidates[1:] {
		if len(candidate.Payload) < len(best.Payload) {
			best = candidate
		}
	}
	return best
}

func DecodeIntegerValues(enc ValueEncoding) ([]int64, error) {
	values, _, err := DecodeIntegerValuesInto(enc, nil)
	return values, err
}

func DecodeIntegerValuesInto(enc ValueEncoding, out []int64) ([]int64, []int64, error) {
	if cap(out) < enc.Count {
		out = make([]int64, enc.Count)
	}
	out = out[:enc.Count]
	if enc.Strategy == ValueStrategyConstant {
		for i := range out {
			out[i] = enc.Base
		}
		return out, out, nil
	}
	transformed, err := DecodeSignedVarintsInto(enc.Payload, enc.Count, out)
	if err != nil {
		return nil, out, err
	}
	switch enc.Strategy {
	case ValueStrategyDirectResidual:
		for i, value := range transformed {
			out[i] = enc.Base + value
		}
		return out, out, nil
	case ValueStrategyDelta:
		restoreDeltaInPlace(out)
		return out, out, nil
	case ValueStrategyDeltaOfDelta:
		restoreDeltaOfDeltaInPlace(out)
		return out, out, nil
	default:
		return nil, out, errors.New("codec: unknown value strategy")
	}
}

func isConstant(values []int64) bool {
	if len(values) <= 1 {
		return true
	}
	first := values[0]
	for _, value := range values[1:] {
		if value != first {
			return false
		}
	}
	return true
}

func encodeDirectResidual(values []int64) ValueEncoding {
	var sum int64
	for _, value := range values {
		sum += value
	}
	var base int64
	if len(values) > 0 {
		base = sum / int64(len(values))
	}
	transformed := make([]int64, len(values))
	for i, value := range values {
		transformed[i] = value - base
	}
	return ValueEncoding{
		Strategy: ValueStrategyDirectResidual,
		Count:    len(values),
		Base:     base,
		Payload:  EncodeSignedVarints(transformed),
	}
}

func encodeDelta(values []int64) ValueEncoding {
	transformed := make([]int64, len(values))
	if len(values) > 0 {
		transformed[0] = values[0]
		for i := 1; i < len(values); i++ {
			transformed[i] = values[i] - values[i-1]
		}
	}
	return ValueEncoding{
		Strategy: ValueStrategyDelta,
		Count:    len(values),
		Payload:  EncodeSignedVarints(transformed),
	}
}

func encodeDeltaOfDelta(values []int64) ValueEncoding {
	transformed := make([]int64, len(values))
	switch len(values) {
	case 0:
	case 1:
		transformed[0] = values[0]
	default:
		transformed[0] = values[0]
		prevDelta := values[1] - values[0]
		transformed[1] = prevDelta
		for i := 2; i < len(values); i++ {
			delta := values[i] - values[i-1]
			transformed[i] = delta - prevDelta
			prevDelta = delta
		}
	}
	return ValueEncoding{
		Strategy: ValueStrategyDeltaOfDelta,
		Count:    len(values),
		Payload:  EncodeSignedVarints(transformed),
	}
}

func restoreDeltaInPlace(out []int64) {
	var current int64
	for i, value := range out {
		if i == 0 {
			current = value
		} else {
			current += value
		}
		out[i] = current
	}
}

func restoreDeltaOfDeltaInPlace(out []int64) {
	switch len(out) {
	case 0:
	case 1:
	default:
		prevDelta := out[1]
		out[1] = out[0] + prevDelta
		for i := 2; i < len(out); i++ {
			delta := prevDelta + out[i]
			out[i] = out[i-1] + delta
			prevDelta = delta
		}
	}
}
