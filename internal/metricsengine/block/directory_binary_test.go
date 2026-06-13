package block

import (
	"encoding/json"
	"hash/crc32"
	"reflect"
	"testing"
	"unsafe"

	"github.com/yaop-labs/amber/internal/metricsengine/codec"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func unsafeStringData(s string) *byte { return unsafe.StringData(s) }

func sampleDirectory() Directory {
	return Directory{
		Version: version,
		Series: []DirectoryEntry{
			{
				SeriesID:      1,
				Type:          model.MetricTypeCounter,
				Labels:        model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "service", Value: "api"}},
				TimeMin:       1000,
				TimeMax:       6000,
				TimestampOff:  12,
				TimestampLen:  34,
				TimestampN:    6,
				TimestampBase: 1000,
				TimestampStep: 1000,
				TimestampKind: codec.TimestampStrategyRegular,
				ValueOff:      48,
				ValueLen:      9,
				ValueN:        6,
				ValueBase:     -5,
				ValueStrategy: codec.ValueStrategyDelta,
				ZoneMap:       ZoneMap{Min: -5, Max: 50, Sum: 120, Count: 6, First: 0, Last: 50, Monotonic: true, HasReset: false},
				AggregateBuckets: []AggregateBucket{
					{TimeMin: 1000, TimeMax: 3000, Min: 0, Max: 20, Sum: 30, Count: 3},
					{TimeMin: 4000, TimeMax: 6000, Min: 30, Max: 50, Sum: 90, Count: 3},
				},
			},
			{
				SeriesID:      2,
				Type:          model.MetricTypeGauge,
				Labels:        model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "service", Value: "web"}},
				TimeMin:       1000,
				TimeMax:       2000,
				TimestampOff:  12,
				TimestampLen:  34,
				TimestampN:    2,
				TimestampBase: 1000,
				TimestampStep: 1000,
				TimestampKind: codec.TimestampStrategyDeltaOfDelta,
				ValueOff:      60,
				ValueLen:      4,
				ValueN:        2,
				ValueBase:     7,
				ValueStrategy: codec.ValueStrategyConstant,
				ZoneMap:       ZoneMap{Min: 7, Max: 7, Sum: 14, Count: 2, First: 7, Last: 7, Monotonic: false, HasReset: true},
				// No aggregate buckets: exercises the empty-bucket path.
			},
		},
	}
}

func TestDirectoryBinaryRoundTrip(t *testing.T) {
	in := sampleDirectory()
	payload := encodeDirectoryBinary(in)
	if !hasBinaryDirectoryMagic(payload) {
		t.Fatal("encoded payload missing binary magic")
	}
	out, err := decodeDirectoryBinary(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

// TestDirectoryBinaryInternsLabelStrings checks that repeated label strings
// across entries decode to the same backing string (the interning that makes
// large directories cheap to hold).
func TestDirectoryBinaryInternsLabelStrings(t *testing.T) {
	in := sampleDirectory()
	out, err := decodeDirectoryBinary(encodeDirectoryBinary(in))
	if err != nil {
		t.Fatal(err)
	}
	a := out.Series[0].Labels[0].Value // "http_requests_total"
	b := out.Series[1].Labels[0].Value // same metric name, different entry
	if a != b {
		t.Fatalf("metric names differ: %q vs %q", a, b)
	}
	// Same underlying data pointer => one allocation shared across entries.
	pa := unsafeStringData(a)
	pb := unsafeStringData(b)
	if pa != pb {
		t.Fatalf("repeated label value not interned: ptr %v vs %v", pa, pb)
	}
}

// TestDecodeDirectoryPayloadReadsLegacyJSON proves a block written in the old
// JSON directory format still loads through the dispatch in
// decodeDirectoryPayload (graceful degradation for pre-existing data dirs).
func TestDecodeDirectoryPayloadReadsLegacyJSON(t *testing.T) {
	in := sampleDirectory()
	jsonPayload, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if jsonPayload[0] != '{' {
		t.Fatalf("legacy JSON should start with '{', got %q", jsonPayload[0])
	}
	crc := crc32.ChecksumIEEE(jsonPayload)
	out, err := decodeDirectoryPayload(jsonPayload, crc)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("legacy JSON decode mismatch:\n in=%+v\nout=%+v", in, out)
	}
}

func TestDecodeDirectoryPayloadRejectsBadCRC(t *testing.T) {
	payload := encodeDirectoryBinary(sampleDirectory())
	if _, err := decodeDirectoryPayload(payload, 0xdeadbeef); err == nil {
		t.Fatal("expected checksum mismatch error")
	}
}
