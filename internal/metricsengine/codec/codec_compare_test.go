package codec

// bytes/sample comparison: amber's strategy-race codec vs the reference
// Gorilla encoder (gorilla_ref_test.go). This is the falsifiable check from
// ARCHITECTURE.md §6 criterion #1 ("байт/сэмпл — обязан бить Gorilla").
// Run with `go test -run TestBytesPerSampleVsGorilla -v` to see the table.
//
// Methodology notes:
//   - amber numbers are payload bytes only; per-series directory overhead
//     (~fixed JSON entry) is excluded on both sides (Gorilla's chunk header
//     is likewise excluded beyond the first-sample cost).
//   - int shapes go to Gorilla as float64 — exactly what a Prometheus chunk
//     stores for the same data.
//   - decimal float shapes use the ALP path (EncodeFloatValues) for amber.

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
)

func amberIntBytes(values []int64) (int, ValueStrategy) {
	enc := EncodeIntegerValues(values)
	return len(enc.Payload), enc.Strategy
}

func TestBytesPerSampleVsGorilla(t *testing.T) {
	const n = 1000
	rng := rand.New(rand.NewSource(7))

	// --- int shapes ---
	counter := make([]int64, n) // Poisson-ish increments, λ≈5
	cum := int64(0)
	for i := range counter {
		inc := int64(0)
		for k := 0; k < 10; k++ {
			if rng.Float64() < 0.5 {
				inc++
			}
		}
		cum += inc
		counter[i] = cum
	}

	noisyGauge := make([]int64, n) // level 5000 ± 3
	for i := range noisyGauge {
		noisyGauge[i] = 5000 + rng.Int63n(7) - 3
	}

	randomWalk := make([]int64, n)
	w := int64(100000)
	for i := range randomWalk {
		w += int64(rng.NormFloat64() * 10)
		randomWalk[i] = w
	}

	latencyMS := make([]int64, n) // lognormal-ish request latencies
	for i := range latencyMS {
		latencyMS[i] = int64(math.Exp(rng.NormFloat64()*0.8+3.2)) + 1
	}

	constant := make([]int64, n)
	for i := range constant {
		constant[i] = 42
	}

	type intRow struct {
		name   string
		values []int64
	}
	intRows := []intRow{
		{"counter_poisson", counter},
		{"gauge_noisy_level", noisyGauge},
		{"random_walk", randomWalk},
		{"latency_ms", latencyMS},
		{"constant", constant},
	}

	t.Logf("%-22s %14s %14s %8s  %s", "shape (int64)", "amber B/sample", "gorilla B/sample", "ratio", "amber strategy")
	for _, row := range intRows {
		aBytes, strategy := amberIntBytes(row.values)
		gBytes := (gorillaIntValueBits(row.values) + 7) / 8
		ratio := float64(aBytes) / float64(gBytes)
		t.Logf("%-22s %14.3f %14.3f %8.2f  %s",
			row.name, float64(aBytes)/n, float64(gBytes)/n, ratio, strategy)
		if row.name == "constant" || row.name == "counter_poisson" || row.name == "gauge_noisy_level" {
			if aBytes > gBytes {
				t.Errorf("%s: amber %d bytes > gorilla %d bytes", row.name, aBytes, gBytes)
			}
		}
	}

	// --- float shapes (ALP path vs XOR) ---
	decimal2 := make([]float64, n) // sine, 2 decimal digits — SDK-rounded gauge
	for i := range decimal2 {
		decimal2[i] = math.Round((50+40*math.Sin(float64(i)/30)+rng.Float64()*2)*100) / 100
	}
	decimal0 := make([]float64, n) // integer-valued float gauge
	for i := range decimal0 {
		decimal0[i] = float64(20 + rng.Intn(10))
	}
	binaryFloat := make([]float64, n) // true binary floats — ALP worst case
	for i := range binaryFloat {
		binaryFloat[i] = rng.NormFloat64() * 1e3
	}

	type floatRow struct {
		name   string
		values []float64
	}
	floatRows := []floatRow{
		{"gauge_decimal_2dig", decimal2},
		{"gauge_decimal_0dig", decimal0},
		{"gauge_binary_float", binaryFloat},
	}

	t.Logf("%-22s %14s %14s %8s  %s", "shape (float64)", "amber B/sample", "gorilla B/sample", "ratio", "amber form")
	for _, row := range floatRows {
		enc := EncodeFloatValues(row.values)
		aBytes := enc.EncodedSize()
		gBytes := (gorillaValueBits(row.values) + 7) / 8
		ratio := float64(aBytes) / float64(gBytes)
		form := fmt.Sprintf("exp=%d exc=%d %s", enc.DecimalExp, len(enc.ExceptionPositions), enc.Mantissas.Strategy)
		t.Logf("%-22s %14.3f %14.3f %8.2f  %s",
			row.name, float64(aBytes)/n, float64(gBytes)/n, ratio, form)
		if row.name == "gauge_decimal_2dig" || row.name == "gauge_decimal_0dig" {
			if aBytes > gBytes {
				t.Errorf("%s: amber %d bytes > gorilla %d bytes", row.name, aBytes, gBytes)
			}
		}
	}

	// --- timestamps ---
	regular := make([]int64, n)
	for i := range regular {
		regular[i] = 1_700_000_000_000 + int64(i)*10_000
	}
	jittered := make([]int64, n)
	cur := int64(1_700_000_000_000)
	for i := range jittered {
		cur += 10_000 + rng.Int63n(7) - 3
		jittered[i] = cur
	}

	t.Logf("%-22s %14s %14s %8s  %s", "timestamps", "amber B/sample", "gorilla B/sample", "ratio", "amber strategy")
	for _, row := range []struct {
		name string
		ts   []int64
	}{{"regular_10s", regular}, {"jitter_pm3ms", jittered}} {
		enc := EncodeTimestamps(row.ts)
		aBytes := len(enc.Payload) // Regular: 0 bytes — base/step live in the directory
		gBytes := (gorillaTimestampBits(row.ts) + 7) / 8
		t.Logf("%-22s %14.3f %14.3f %8.2f  %s",
			row.name, float64(aBytes)/n, float64(gBytes)/n, float64(aBytes)/float64(gBytes), enc.Strategy)
		if aBytes > gBytes {
			t.Errorf("%s: amber %d bytes > gorilla %d bytes", row.name, aBytes, gBytes)
		}
	}
}
