package codec

import "errors"

type ValueStrategy uint8

const (
	ValueStrategyDirectResidual ValueStrategy = iota + 1
	ValueStrategyDelta
	ValueStrategyDeltaOfDelta
	ValueStrategyConstant
	// Packed variants carry the same transformed stream as their varint
	// counterparts but with a fixed-width bit-packed payload (bitpack.go).
	ValueStrategyDirectResidualPacked
	ValueStrategyDeltaPacked
	ValueStrategyDeltaOfDeltaPacked
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
	case ValueStrategyDirectResidualPacked:
		return "direct_residual_packed"
	case ValueStrategyDeltaPacked:
		return "delta_packed"
	case ValueStrategyDeltaOfDeltaPacked:
		return "delta_of_delta_packed"
	default:
		return "unknown"
	}
}

// valueBaseStrategy maps a strategy to its transform family and whether the
// payload is bit-packed.
func valueBaseStrategy(s ValueStrategy) (ValueStrategy, bool) {
	switch s {
	case ValueStrategyDirectResidualPacked:
		return ValueStrategyDirectResidual, true
	case ValueStrategyDeltaPacked:
		return ValueStrategyDelta, true
	case ValueStrategyDeltaOfDeltaPacked:
		return ValueStrategyDeltaOfDelta, true
	default:
		return s, false
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
	// Race every transform with both payload codecs: varint wins on streams
	// with rare large outliers, bit-packing wins on small uniform residuals
	// (it goes below the 1-byte/value varint floor).
	transforms := []valueTransform{
		residualTransform(values),
		deltaTransform(values),
		deltaOfDeltaTransform(values),
	}
	var best ValueEncoding
	for i, t := range transforms {
		varint := ValueEncoding{
			Strategy: t.strategy,
			Count:    len(values),
			Base:     t.base,
			Payload:  EncodeSignedVarints(t.data),
		}
		packed := encodePackedTransform(t, len(values))
		candidate := varint
		if len(packed.Payload) < len(varint.Payload) {
			candidate = packed
		}
		if i == 0 || len(candidate.Payload) < len(best.Payload) {
			best = candidate
		}
	}
	return best
}

// encodePackedTransform builds the bit-packed candidate for a transform.
// Delta and delta-of-delta keep the absolute first element in transformed[0];
// packing it inline would set the fixed width to the magnitude of a raw
// value/timestamp and poison the whole stream, so it moves to the (otherwise
// unused) Base field and only transformed[1:] is packed. The residual
// transform already centers every element, so Base keeps the mean and the
// full stream is packed.
func encodePackedTransform(t valueTransform, count int) ValueEncoding {
	enc := ValueEncoding{Strategy: t.packed, Count: count, Base: t.base}
	switch t.strategy {
	case ValueStrategyDirectResidual:
		enc.Payload = EncodeBitPacked(t.data)
	default: // delta, delta-of-delta
		if len(t.data) == 0 {
			enc.Payload = EncodeBitPacked(nil)
			return enc
		}
		enc.Base = t.data[0]
		enc.Payload = EncodeBitPacked(t.data[1:])
	}
	return enc
}

type valueTransform struct {
	strategy ValueStrategy
	packed   ValueStrategy
	base     int64
	data     []int64
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
	base, packed := valueBaseStrategy(enc.Strategy)
	var transformed []int64
	var err error
	switch {
	case packed && base == ValueStrategyDirectResidual:
		transformed, err = DecodeBitPackedInto(enc.Payload, enc.Count, out)
	case packed:
		// Delta/dod packed: transformed[0] lives in Base, payload holds the
		// remaining count-1 elements.
		if enc.Count == 0 {
			transformed = out[:0]
			break
		}
		rest, derr := DecodeBitPackedInto(enc.Payload, enc.Count-1, out)
		if derr != nil {
			return nil, out, derr
		}
		if cap(out) < enc.Count {
			out = make([]int64, enc.Count)
		}
		out = out[:enc.Count]
		copy(out[1:], rest) // memmove-safe when rest aliases out
		out[0] = enc.Base
		transformed = out
	default:
		transformed, err = DecodeSignedVarintsInto(enc.Payload, enc.Count, out)
	}
	if err != nil {
		return nil, out, err
	}
	out = transformed
	switch base {
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

func residualTransform(values []int64) valueTransform {
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
	return valueTransform{
		strategy: ValueStrategyDirectResidual,
		packed:   ValueStrategyDirectResidualPacked,
		base:     base,
		data:     transformed,
	}
}

func deltaTransform(values []int64) valueTransform {
	transformed := make([]int64, len(values))
	if len(values) > 0 {
		transformed[0] = values[0]
		for i := 1; i < len(values); i++ {
			transformed[i] = values[i] - values[i-1]
		}
	}
	return valueTransform{
		strategy: ValueStrategyDelta,
		packed:   ValueStrategyDeltaPacked,
		data:     transformed,
	}
}

func deltaOfDeltaTransform(values []int64) valueTransform {
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
	return valueTransform{
		strategy: ValueStrategyDeltaOfDelta,
		packed:   ValueStrategyDeltaOfDeltaPacked,
		data:     transformed,
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
