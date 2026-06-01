package codec

type TimestampStrategy uint8

const (
	TimestampStrategyDeltaOfDelta TimestampStrategy = iota + 1
	TimestampStrategyRegular
)

func (s TimestampStrategy) String() string {
	switch s {
	case TimestampStrategyDeltaOfDelta:
		return "delta_of_delta"
	case TimestampStrategyRegular:
		return "regular"
	default:
		return "unknown"
	}
}

type TimestampEncoding struct {
	Strategy TimestampStrategy
	Count    int
	Base     int64
	Step     int64
	Payload  []byte
}

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
	return TimestampEncoding{
		Strategy: TimestampStrategyDeltaOfDelta,
		Count:    len(timestamps),
		Payload:  EncodeSignedVarints(transformed),
	}
}

func DecodeTimestamps(enc TimestampEncoding) ([]int64, error) {
	if enc.Strategy == TimestampStrategyRegular {
		out := make([]int64, enc.Count)
		for i := range out {
			out[i] = enc.Base + int64(i)*enc.Step
		}
		return out, nil
	}
	values, err := DecodeSignedVarints(enc.Payload, enc.Count)
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
