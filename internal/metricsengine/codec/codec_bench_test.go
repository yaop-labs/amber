package codec

import "testing"

func BenchmarkEncodeIntegerValuesCounter(b *testing.B) {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i * 10)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = EncodeIntegerValues(values)
	}
}

func BenchmarkDecodeIntegerValuesCounter(b *testing.B) {
	values := make([]int64, 4096)
	for i := range values {
		values[i] = int64(i * 10)
	}
	enc := EncodeIntegerValues(values)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeIntegerValues(enc); err != nil {
			b.Fatal(err)
		}
	}
}
