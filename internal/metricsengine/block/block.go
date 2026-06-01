package block

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/yaop-labs/amber/internal/metricsengine/codec"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

const (
	fileMagic                  = "MEB1"
	footerMagic                = "MEF1"
	version                    = uint16(1)
	footerSize                 = 24
	defaultAggregateBucketSize = 64
)

type Series struct {
	ID         uint64
	Type       model.MetricType
	Labels     model.LabelSet
	Timestamps []int64
	Values     []int64
}

type ZoneMap struct {
	Min       int64 `json:"min"`
	Max       int64 `json:"max"`
	Sum       int64 `json:"sum"`
	Count     int   `json:"count"`
	First     int64 `json:"first"`
	Last      int64 `json:"last"`
	Monotonic bool  `json:"monotonic"`
	HasReset  bool  `json:"has_reset"`
}

type AggregateBucket struct {
	TimeMin int64 `json:"time_min"`
	TimeMax int64 `json:"time_max"`
	Min     int64 `json:"min"`
	Max     int64 `json:"max"`
	Sum     int64 `json:"sum"`
	Count   int   `json:"count"`
}

type DirectoryEntry struct {
	SeriesID         uint64                  `json:"series_id"`
	Type             model.MetricType        `json:"type"`
	Labels           model.LabelSet          `json:"labels"`
	TimeMin          int64                   `json:"time_min"`
	TimeMax          int64                   `json:"time_max"`
	TimestampOff     int64                   `json:"timestamp_offset"`
	TimestampLen     int64                   `json:"timestamp_len"`
	TimestampN       int                     `json:"timestamp_count"`
	TimestampBase    int64                   `json:"timestamp_base"`
	TimestampStep    int64                   `json:"timestamp_step"`
	TimestampKind    codec.TimestampStrategy `json:"timestamp_strategy"`
	ValueOff         int64                   `json:"value_offset"`
	ValueLen         int64                   `json:"value_len"`
	ValueN           int                     `json:"value_count"`
	ValueBase        int64                   `json:"value_base"`
	ValueStrategy    codec.ValueStrategy     `json:"value_strategy"`
	ZoneMap          ZoneMap                 `json:"zone_map"`
	AggregateBuckets []AggregateBucket       `json:"aggregate_buckets,omitempty"`
}

type Directory struct {
	Version uint16           `json:"version"`
	Series  []DirectoryEntry `json:"series"`
}

func (d Directory) TimeRange() (int64, int64, bool) {
	if len(d.Series) == 0 {
		return 0, 0, false
	}
	min := d.Series[0].TimeMin
	max := d.Series[0].TimeMax
	for _, entry := range d.Series[1:] {
		if entry.TimeMin < min {
			min = entry.TimeMin
		}
		if entry.TimeMax > max {
			max = entry.TimeMax
		}
	}
	return min, max, true
}

type DecodedSeries struct {
	Entry      DirectoryEntry
	Timestamps []int64
	Values     []int64
}

type EntryFilter func(DirectoryEntry) bool
type SeriesFunc func(DecodedSeries) error

func WriteFile(path string, series []Series) error {
	var buf bytes.Buffer
	if _, err := buf.WriteString(fileMagic); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, version); err != nil {
		return err
	}

	dir := Directory{Version: version, Series: make([]DirectoryEntry, 0, len(series))}
	tsRefs := make(map[string]timestampRef)
	for _, s := range series {
		s = normalizeSeries(s)
		if len(s.Timestamps) != len(s.Values) {
			return errors.New("block: timestamp/value length mismatch")
		}
		ts := codec.EncodeTimestamps(s.Timestamps)
		tsKey := timestampKey(s.Timestamps)
		tsRef, ok := tsRefs[tsKey]
		if !ok {
			tsRef = timestampRef{
				off:      int64(buf.Len()),
				n:        int64(len(ts.Payload)),
				count:    ts.Count,
				base:     ts.Base,
				step:     ts.Step,
				strategy: ts.Strategy,
			}
			if _, err := buf.Write(ts.Payload); err != nil {
				return err
			}
			tsRefs[tsKey] = tsRef
		}

		values := codec.EncodeIntegerValues(s.Values)
		valueOff := int64(buf.Len())
		if _, err := buf.Write(values.Payload); err != nil {
			return err
		}
		zoneMap := BuildZoneMap(s.Values)
		if s.Type != model.MetricTypeCounter {
			zoneMap.HasReset = false
		}

		dir.Series = append(dir.Series, DirectoryEntry{
			SeriesID:         s.ID,
			Type:             s.Type,
			Labels:           s.Labels.Canonical(),
			TimeMin:          minTimestamp(s.Timestamps),
			TimeMax:          maxTimestamp(s.Timestamps),
			TimestampOff:     tsRef.off,
			TimestampLen:     tsRef.n,
			TimestampN:       tsRef.count,
			TimestampBase:    tsRef.base,
			TimestampStep:    tsRef.step,
			TimestampKind:    tsRef.strategy,
			ValueOff:         valueOff,
			ValueLen:         int64(len(values.Payload)),
			ValueN:           values.Count,
			ValueBase:        values.Base,
			ValueStrategy:    values.Strategy,
			ZoneMap:          zoneMap,
			AggregateBuckets: BuildAggregateBuckets(s.Timestamps, s.Values, defaultAggregateBucketSize),
		})
	}

	dirOff := int64(buf.Len())
	dirPayload, err := json.Marshal(dir)
	if err != nil {
		return err
	}
	if _, err := buf.Write(dirPayload); err != nil {
		return err
	}
	dirLen := int64(len(dirPayload))
	dirCRC := crc32.ChecksumIEEE(dirPayload)
	if err := binary.Write(&buf, binary.LittleEndian, dirOff); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, dirLen); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, dirCRC); err != nil {
		return err
	}
	if _, err := buf.WriteString(footerMagic); err != nil {
		return err
	}

	tmp := path + ".tmp"
	file, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(buf.Bytes()); err != nil {
		file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

func normalizeSeries(s Series) Series {
	if len(s.Timestamps) != len(s.Values) || len(s.Timestamps) <= 1 {
		return s
	}
	type sample struct {
		timestamp int64
		value     int64
	}
	samples := make([]sample, len(s.Timestamps))
	for i := range s.Timestamps {
		samples[i] = sample{timestamp: s.Timestamps[i], value: s.Values[i]}
	}
	sort.SliceStable(samples, func(i, j int) bool {
		return samples[i].timestamp < samples[j].timestamp
	})
	out := s
	out.Timestamps = make([]int64, len(samples))
	out.Values = make([]int64, len(samples))
	for i, sample := range samples {
		out.Timestamps[i] = sample.timestamp
		out.Values[i] = sample.value
	}
	return out
}

type timestampRef struct {
	off      int64
	n        int64
	count    int
	base     int64
	step     int64
	strategy codec.TimestampStrategy
}

func minTimestamp(timestamps []int64) int64 {
	if len(timestamps) == 0 {
		return 0
	}
	min := timestamps[0]
	for _, ts := range timestamps[1:] {
		if ts < min {
			min = ts
		}
	}
	return min
}

func maxTimestamp(timestamps []int64) int64 {
	if len(timestamps) == 0 {
		return 0
	}
	max := timestamps[0]
	for _, ts := range timestamps[1:] {
		if ts > max {
			max = ts
		}
	}
	return max
}

func timestampKey(timestamps []int64) string {
	var b strings.Builder
	b.Grow(len(timestamps) * 12)
	for _, ts := range timestamps {
		b.WriteString(strconv.FormatInt(ts, 10))
		b.WriteByte(',')
	}
	return b.String()
}

func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func ReadDirectory(path string) (Directory, error) {
	file, err := os.Open(path)
	if err != nil {
		return Directory{}, err
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return Directory{}, err
	}
	size := info.Size()
	if size < int64(len(fileMagic)+2+footerSize) {
		return Directory{}, io.ErrUnexpectedEOF
	}
	magic := make([]byte, len(fileMagic))
	if _, err := file.ReadAt(magic, 0); err != nil {
		return Directory{}, err
	}
	if string(magic) != fileMagic {
		return Directory{}, errors.New("block: invalid file magic")
	}

	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, size-footerSize); err != nil {
		return Directory{}, err
	}
	if string(footer[20:24]) != footerMagic {
		return Directory{}, errors.New("block: invalid footer magic")
	}
	dirOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	dirLen := int64(binary.LittleEndian.Uint64(footer[8:16]))
	dirCRC := binary.LittleEndian.Uint32(footer[16:20])
	if dirOff < 0 || dirLen < 0 || dirOff+dirLen > size-footerSize {
		return Directory{}, errors.New("block: invalid directory offset")
	}

	dirPayload := make([]byte, dirLen)
	if _, err := file.ReadAt(dirPayload, dirOff); err != nil {
		return Directory{}, err
	}
	return decodeDirectoryPayload(dirPayload, dirCRC)
}

func ReadFile(path string) ([]DecodedSeries, error) {
	return ReadFileFiltered(path, nil)
}

func ReadFileFiltered(path string, filter EntryFilter) ([]DecodedSeries, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	dir, err := readDirectoryFromFile(file)
	if err != nil {
		return nil, err
	}
	return readFileFilteredWithDirectory(file, dir, filter)
}

func ReadFileFilteredWithDirectory(path string, dir Directory, filter EntryFilter) ([]DecodedSeries, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return readFileFilteredWithDirectory(file, dir, filter)
}

func readFileFilteredWithDirectory(file *os.File, dir Directory, filter EntryFilter) ([]DecodedSeries, error) {
	out := make([]DecodedSeries, 0)
	err := scanFileFilteredWithDirectory(file, dir, filter, true, true, func(series DecodedSeries) error {
		out = append(out, series)
		return nil
	})
	return out, err
}

func ScanFileFilteredWithDirectory(path string, dir Directory, filter EntryFilter, fn SeriesFunc) error {
	return scanFileFilteredWithDirectoryPath(path, dir, filter, true, true, fn)
}

func ScanFileFilteredWithDirectoryShared(path string, dir Directory, filter EntryFilter, fn SeriesFunc) error {
	return scanFileFilteredWithDirectoryPath(path, dir, filter, false, false, fn)
}

func scanFileFilteredWithDirectoryPath(path string, dir Directory, filter EntryFilter, copyTimestamps bool, copyValues bool, fn SeriesFunc) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	return scanFileFilteredWithDirectory(file, dir, filter, copyTimestamps, copyValues, fn)
}

func scanFileFilteredWithDirectory(file *os.File, dir Directory, filter EntryFilter, copyTimestamps bool, copyValues bool, fn SeriesFunc) error {
	timestampCache := make(map[timestampCacheKey][]int64)
	valuePayload := make([]byte, 0)
	valueBuffer := make([]int64, 0)
	for _, entry := range dir.Series {
		if filter != nil && !filter(entry) {
			continue
		}
		var err error
		valuePayload, err = readAtReuse(file, valuePayload, entry.ValueOff, entry.ValueLen)
		if err != nil {
			return err
		}
		timestamps, err := decodeEntryTimestampsFromFileCopy(file, entry, timestampCache, copyTimestamps)
		if err != nil {
			return err
		}
		enc := codec.ValueEncoding{
			Strategy: entry.ValueStrategy,
			Count:    entry.ValueN,
			Base:     entry.ValueBase,
			Payload:  valuePayload,
		}
		var values []int64
		if copyValues {
			values, err = codec.DecodeIntegerValues(enc)
			if err != nil {
				return err
			}
		} else {
			values, valueBuffer, err = codec.DecodeIntegerValuesInto(enc, valueBuffer)
			if err != nil {
				return err
			}
		}
		if err := fn(DecodedSeries{
			Entry:      entry,
			Timestamps: timestamps,
			Values:     values,
		}); err != nil {
			return err
		}
	}
	return nil
}

func readDirectoryFromFile(file *os.File) (Directory, error) {
	info, err := file.Stat()
	if err != nil {
		return Directory{}, err
	}
	size := info.Size()
	if size < int64(len(fileMagic)+2+footerSize) {
		return Directory{}, io.ErrUnexpectedEOF
	}
	magic := make([]byte, len(fileMagic))
	if _, err := file.ReadAt(magic, 0); err != nil {
		return Directory{}, err
	}
	if string(magic) != fileMagic {
		return Directory{}, errors.New("block: invalid file magic")
	}

	footer := make([]byte, footerSize)
	if _, err := file.ReadAt(footer, size-footerSize); err != nil {
		return Directory{}, err
	}
	if string(footer[20:24]) != footerMagic {
		return Directory{}, errors.New("block: invalid footer magic")
	}
	dirOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	dirLen := int64(binary.LittleEndian.Uint64(footer[8:16]))
	dirCRC := binary.LittleEndian.Uint32(footer[16:20])
	if dirOff < 0 || dirLen < 0 || dirOff+dirLen > size-footerSize {
		return Directory{}, errors.New("block: invalid directory offset")
	}

	dirPayload, err := readAt(file, dirOff, dirLen)
	if err != nil {
		return Directory{}, err
	}
	return decodeDirectoryPayload(dirPayload, dirCRC)
}

func readAt(file *os.File, off int64, n int64) ([]byte, error) {
	return readAtReuse(file, nil, off, n)
}

func readAtReuse(file *os.File, buf []byte, off int64, n int64) ([]byte, error) {
	if off < 0 || n < 0 {
		return nil, errors.New("block: invalid offset")
	}
	if n > int64(int(n)) {
		return nil, errors.New("block: payload too large")
	}
	if cap(buf) < int(n) {
		buf = make([]byte, int(n))
	}
	payload := buf[:int(n)]
	if _, err := file.ReadAt(payload, off); err != nil {
		return nil, err
	}
	return payload, nil
}

type timestampCacheKey struct {
	off      int64
	n        int64
	count    int
	base     int64
	step     int64
	strategy codec.TimestampStrategy
}

func decodeEntryTimestamps(payload []byte, entry DirectoryEntry, cache map[timestampCacheKey][]int64) ([]int64, error) {
	key := timestampCacheKey{
		off:      entry.TimestampOff,
		n:        entry.TimestampLen,
		count:    entry.TimestampN,
		base:     entry.TimestampBase,
		step:     entry.TimestampStep,
		strategy: entry.TimestampKind,
	}
	if timestamps, ok := cache[key]; ok {
		return append([]int64(nil), timestamps...), nil
	}
	tsPayload, err := checkedSlice(payload, entry.TimestampOff, entry.TimestampLen)
	if err != nil {
		return nil, err
	}
	timestamps, err := codec.DecodeTimestamps(codec.TimestampEncoding{
		Strategy: entry.TimestampKind,
		Count:    entry.TimestampN,
		Base:     entry.TimestampBase,
		Step:     entry.TimestampStep,
		Payload:  tsPayload,
	})
	if err != nil {
		return nil, err
	}
	cache[key] = append([]int64(nil), timestamps...)
	return timestamps, nil
}

func decodeEntryTimestampsFromFile(file *os.File, entry DirectoryEntry, cache map[timestampCacheKey][]int64) ([]int64, error) {
	return decodeEntryTimestampsFromFileCopy(file, entry, cache, true)
}

func decodeEntryTimestampsFromFileCopy(file *os.File, entry DirectoryEntry, cache map[timestampCacheKey][]int64, copyResult bool) ([]int64, error) {
	key := timestampCacheKey{
		off:      entry.TimestampOff,
		n:        entry.TimestampLen,
		count:    entry.TimestampN,
		base:     entry.TimestampBase,
		step:     entry.TimestampStep,
		strategy: entry.TimestampKind,
	}
	if timestamps, ok := cache[key]; ok {
		if !copyResult {
			return timestamps, nil
		}
		return append([]int64(nil), timestamps...), nil
	}
	tsPayload, err := readAt(file, entry.TimestampOff, entry.TimestampLen)
	if err != nil {
		return nil, err
	}
	timestamps, err := codec.DecodeTimestamps(codec.TimestampEncoding{
		Strategy: entry.TimestampKind,
		Count:    entry.TimestampN,
		Base:     entry.TimestampBase,
		Step:     entry.TimestampStep,
		Payload:  tsPayload,
	})
	if err != nil {
		return nil, err
	}
	cache[key] = append([]int64(nil), timestamps...)
	if !copyResult {
		return timestamps, nil
	}
	return timestamps, nil
}

func BuildZoneMap(values []int64) ZoneMap {
	if len(values) == 0 {
		return ZoneMap{}
	}
	z := ZoneMap{
		Min:       values[0],
		Max:       values[0],
		Count:     len(values),
		First:     values[0],
		Last:      values[len(values)-1],
		Monotonic: true,
	}
	for i, value := range values {
		if value < z.Min {
			z.Min = value
		}
		if value > z.Max {
			z.Max = value
		}
		z.Sum += value
		if i > 0 && value < values[i-1] {
			z.Monotonic = false
			z.HasReset = true
		}
	}
	return z
}

func BuildAggregateBuckets(timestamps []int64, values []int64, bucketSize int) []AggregateBucket {
	if bucketSize <= 0 || len(timestamps) == 0 || len(timestamps) != len(values) {
		return nil
	}
	out := make([]AggregateBucket, 0, (len(values)+bucketSize-1)/bucketSize)
	for start := 0; start < len(values); start += bucketSize {
		end := start + bucketSize
		if end > len(values) {
			end = len(values)
		}
		bucket := AggregateBucket{
			TimeMin: timestamps[start],
			TimeMax: timestamps[end-1],
			Min:     values[start],
			Max:     values[start],
			Count:   end - start,
		}
		for _, value := range values[start:end] {
			if value < bucket.Min {
				bucket.Min = value
			}
			if value > bucket.Max {
				bucket.Max = value
			}
			bucket.Sum += value
		}
		out = append(out, bucket)
	}
	return out
}

func readDirectory(payload []byte) (Directory, error) {
	if len(payload) < len(fileMagic)+2+footerSize {
		return Directory{}, io.ErrUnexpectedEOF
	}
	if string(payload[:len(fileMagic)]) != fileMagic {
		return Directory{}, errors.New("block: invalid file magic")
	}
	footer := payload[len(payload)-footerSize:]
	if string(footer[20:24]) != footerMagic {
		return Directory{}, errors.New("block: invalid footer magic")
	}
	dirOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	dirLen := int64(binary.LittleEndian.Uint64(footer[8:16]))
	dirCRC := binary.LittleEndian.Uint32(footer[16:20])
	if dirOff < 0 || dirLen < 0 || dirOff+dirLen > int64(len(payload)-footerSize) {
		return Directory{}, errors.New("block: invalid directory offset")
	}
	dirPayload, err := checkedSlice(payload, dirOff, dirLen)
	if err != nil {
		return Directory{}, err
	}
	return decodeDirectoryPayload(dirPayload, dirCRC)
}

func decodeDirectoryPayload(dirPayload []byte, expectedCRC uint32) (Directory, error) {
	if crc32.ChecksumIEEE(dirPayload) != expectedCRC {
		return Directory{}, errors.New("block: directory checksum mismatch")
	}
	var dir Directory
	if err := json.Unmarshal(dirPayload, &dir); err != nil {
		return Directory{}, err
	}
	if dir.Version != version {
		return Directory{}, errors.New("block: unsupported version")
	}
	return dir, nil
}

func checkedSlice(payload []byte, off, n int64) ([]byte, error) {
	if off < 0 || n < 0 || off+n < off || off+n > int64(len(payload)) {
		return nil, errors.New("block: invalid section offset")
	}
	return payload[off : off+n], nil
}
