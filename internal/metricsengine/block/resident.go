package block

import (
	"errors"
	"os"

	"github.com/yaop-labs/amber/internal/metricsengine/codec"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// resident.go is the compact, resident, query-shaped form of a block directory.
//
// The decoded block.Directory is ~40 MiB for a 100k-series campaign block
// (100k×176 B DirectoryEntry + 500k×48 B model.Label), so the byte-budgeted
// dirCache holds only ~8 of the ~15 in-window blocks a range-step query touches
// and re-decodes the rest every query — ~43% of the qm2 range-step query is
// decodeDirectoryBinary (see metrics-query-redesign.md, phase 2). A ResidentBlock
// stores the same query-relevant fields columnar with dict-encoded labels at
// ~12 MiB/block, so all blocks fit resident → decode happens once, never recurs,
// and selector matching / group resolution work on integer dict codes with no
// per-series model.Label allocation.
//
// It carries only what the range-step exact scan needs (chunk-decode offsets,
// time/zone bounds, labels). Consumers that need full LabelSets or aggregate
// buckets keep using Directory / ScanFileFilteredWithDirectory.

// noLabelCode marks a series that lacks a given label name in the dict columns.
const noLabelCode = ^uint32(0)

// ResidentBlock is the columnar, dict-encoded resident form of one block.
type ResidentBlock struct {
	n int

	seriesID         []uint64
	typ              []model.MetricType
	timeMin, timeMax []int64
	zoneMin, zoneMax []int64

	tsOff, tsLen   []int64
	tsN            []int32
	tsBase, tsStep []int64
	tsKind         []codec.TimestampStrategy

	valOff, valLen []int64
	valN           []int32
	valBase        []int64
	valStrategy    []codec.ValueStrategy

	// dataEnd is the end of the contiguous chunk section [0,dataEnd) — equal to
	// the directory offset. Used to bulk-read every chunk in one syscall.
	dataEnd int64

	// Per-block label dictionary. valueDict holds the unique label values (and
	// names, since codes index a single string table); labelCode[nameIdx][i] is
	// the value code for series i under labelNames[nameIdx], or noLabelCode.
	labelNames []string
	nameIndex  map[string]int
	valueDict  []string
	valueIndex map[string]uint32
	labelCode  [][]uint32
}

// BuildResidentFromDirectory converts a decoded Directory into the compact
// resident form. It is built once per block (on first query access) and cached;
// the transient Directory is released afterwards.
func BuildResidentFromDirectory(dir Directory) *ResidentBlock {
	n := len(dir.Series)
	rb := &ResidentBlock{
		n:           n,
		seriesID:    make([]uint64, n),
		typ:         make([]model.MetricType, n),
		timeMin:     make([]int64, n),
		timeMax:     make([]int64, n),
		zoneMin:     make([]int64, n),
		zoneMax:     make([]int64, n),
		tsOff:       make([]int64, n),
		tsLen:       make([]int64, n),
		tsN:         make([]int32, n),
		tsBase:      make([]int64, n),
		tsStep:      make([]int64, n),
		tsKind:      make([]codec.TimestampStrategy, n),
		valOff:      make([]int64, n),
		valLen:      make([]int64, n),
		valN:        make([]int32, n),
		valBase:     make([]int64, n),
		valStrategy: make([]codec.ValueStrategy, n),
		nameIndex:   make(map[string]int),
		valueIndex:  make(map[string]uint32),
	}

	intern := func(s string) uint32 {
		if code, ok := rb.valueIndex[s]; ok {
			return code
		}
		code := uint32(len(rb.valueDict))
		rb.valueIndex[s] = code
		rb.valueDict = append(rb.valueDict, s)
		return code
	}
	nameIdxOf := func(name string) int {
		if idx, ok := rb.nameIndex[name]; ok {
			return idx
		}
		idx := len(rb.labelNames)
		rb.nameIndex[name] = idx
		rb.labelNames = append(rb.labelNames, name)
		rb.labelCode = append(rb.labelCode, nil)
		return idx
	}

	for i := range dir.Series {
		e := &dir.Series[i]
		rb.seriesID[i] = e.SeriesID
		rb.typ[i] = e.Type
		rb.timeMin[i] = e.TimeMin
		rb.timeMax[i] = e.TimeMax
		rb.zoneMin[i] = e.ZoneMap.Min
		rb.zoneMax[i] = e.ZoneMap.Max
		rb.tsOff[i] = e.TimestampOff
		rb.tsLen[i] = e.TimestampLen
		rb.tsN[i] = int32(e.TimestampN)
		rb.tsBase[i] = e.TimestampBase
		rb.tsStep[i] = e.TimestampStep
		rb.tsKind[i] = e.TimestampKind
		rb.valOff[i] = e.ValueOff
		rb.valLen[i] = e.ValueLen
		rb.valN[i] = int32(e.ValueN)
		rb.valBase[i] = e.ValueBase
		rb.valStrategy[i] = e.ValueStrategy
		if end := e.ValueOff + e.ValueLen; end > rb.dataEnd {
			rb.dataEnd = end
		}
		if end := e.TimestampOff + e.TimestampLen; end > rb.dataEnd {
			rb.dataEnd = end
		}
		for _, l := range e.Labels {
			ni := nameIdxOf(l.Name)
			if rb.labelCode[ni] == nil {
				codes := make([]uint32, n)
				for k := range codes {
					codes[k] = noLabelCode
				}
				rb.labelCode[ni] = codes
			}
			rb.labelCode[ni][i] = intern(l.Value)
		}
	}
	return rb
}

// Len reports the number of series in the block.
func (rb *ResidentBlock) Len() int { return rb.n }

// NameIndex returns the column index of a label name, or -1 if the block has no
// series carrying that name.
func (rb *ResidentBlock) NameIndex(name string) int {
	if idx, ok := rb.nameIndex[name]; ok {
		return idx
	}
	return -1
}

// ValueDict exposes the per-block value dictionary for predicate construction.
func (rb *ResidentBlock) ValueDict() []string { return rb.valueDict }

// ValueCode returns the dict code of a value and whether it is present.
func (rb *ResidentBlock) ValueCode(value string) (uint32, bool) {
	code, ok := rb.valueIndex[value]
	return code, ok
}

// groupValue resolves the value of column nameIdx for series i.
func (rb *ResidentBlock) groupValue(nameIdx, i int) string {
	if nameIdx < 0 {
		return ""
	}
	code := rb.labelCode[nameIdx][i]
	if code == noLabelCode {
		return ""
	}
	return rb.valueDict[code]
}

// ResidentPredicate is a per-block resolved label matcher. The op semantics are
// resolved against the block dict once (by the query package, which owns matcher
// interpretation), leaving the scan to do integer code lookups.
type ResidentPredicate struct {
	// NameIdx is the dict column for the matcher's label, or -1 when the block
	// has no series carrying that name.
	NameIdx int
	// Allowed[code] reports whether the matcher accepts that value code. Indexed
	// by dict code; len == len(valueDict). Nil when NameIdx < 0.
	Allowed []bool
	// AcceptMissing reports whether a series lacking the label matches (true for
	// negative matchers, matching matchLabels' "missing ⇒ continue" rule).
	AcceptMissing bool
}

// ResidentFilter carries the entry-level time/value bounds (mirrors matchTimeRange
// and matchZoneMap). Nil bounds disable the corresponding prune.
type ResidentFilter struct {
	StartMillis, EndMillis *int64
	MinValue, MaxValue     *int64
}

// ResidentSeriesFunc receives one matching series. Timestamps and values alias
// reusable scratch buffers — copy if retaining past the callback.
type ResidentSeriesFunc func(seriesID uint64, typ model.MetricType, groupValue string, timestamps, values []int64) error

func (rb *ResidentBlock) matchSeries(i int, preds []ResidentPredicate) bool {
	for p := range preds {
		pred := &preds[p]
		if pred.NameIdx < 0 {
			if !pred.AcceptMissing {
				return false
			}
			continue
		}
		code := rb.labelCode[pred.NameIdx][i]
		if code == noLabelCode {
			if !pred.AcceptMissing {
				return false
			}
			continue
		}
		if !pred.Allowed[code] {
			return false
		}
	}
	return true
}

// Scan walks the block, applying predicates and the time/value filter, decoding
// the chunk of each matching series and invoking fn with its resolved group
// value. groupNameIdx is the dict column of the group label (-1 ⇒ "").
func (rb *ResidentBlock) Scan(path string, preds []ResidentPredicate, filter ResidentFilter, groupNameIdx int, fn ResidentSeriesFunc) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	section, err := readResidentChunkSection(file, rb.dataEnd)
	if err != nil {
		return err
	}
	timestampCache := make(map[timestampCacheKey][]int64)
	var valueBuffer []int64
	var valueScratch []byte // per-series fallback when section == nil

	for i := 0; i < rb.n; i++ {
		if !rb.matchSeries(i, preds) {
			continue
		}
		if filter.StartMillis != nil && rb.timeMax[i] < *filter.StartMillis {
			continue
		}
		if filter.EndMillis != nil && rb.timeMin[i] > *filter.EndMillis {
			continue
		}
		if filter.MinValue != nil && rb.zoneMax[i] < *filter.MinValue {
			continue
		}
		if filter.MaxValue != nil && rb.zoneMin[i] > *filter.MaxValue {
			continue
		}

		var valPayload []byte
		var timestamps []int64
		if section != nil {
			if rb.valOff[i] < 0 || rb.valOff[i]+rb.valLen[i] > int64(len(section)) ||
				rb.tsOff[i] < 0 || rb.tsOff[i]+rb.tsLen[i] > int64(len(section)) {
				return errors.New("block: resident offset outside chunk section")
			}
			valPayload = section[rb.valOff[i] : rb.valOff[i]+rb.valLen[i]]
			tsPayload := section[rb.tsOff[i] : rb.tsOff[i]+rb.tsLen[i]]
			timestamps, err = rb.decodeTimestamps(i, timestampCache, tsPayload)
		} else {
			valueScratch, err = readAtReuse(file, valueScratch, rb.valOff[i], rb.valLen[i])
			if err != nil {
				return err
			}
			valPayload = valueScratch
			var tsPayload []byte
			tsPayload, err = readAt(file, rb.tsOff[i], rb.tsLen[i])
			if err != nil {
				return err
			}
			timestamps, err = rb.decodeTimestamps(i, timestampCache, tsPayload)
		}
		if err != nil {
			return err
		}

		enc := codec.ValueEncoding{
			Strategy: rb.valStrategy[i],
			Count:    int(rb.valN[i]),
			Base:     rb.valBase[i],
			Payload:  valPayload,
		}
		var values []int64
		values, valueBuffer, err = codec.DecodeIntegerValuesInto(enc, valueBuffer)
		if err != nil {
			return err
		}
		if err := fn(rb.seriesID[i], rb.typ[i], rb.groupValue(groupNameIdx, i), timestamps, values); err != nil {
			return err
		}
	}
	return nil
}

func (rb *ResidentBlock) decodeTimestamps(i int, cache map[timestampCacheKey][]int64, payload []byte) ([]int64, error) {
	key := timestampCacheKey{
		off:      rb.tsOff[i],
		n:        rb.tsLen[i],
		count:    int(rb.tsN[i]),
		base:     rb.tsBase[i],
		step:     rb.tsStep[i],
		strategy: rb.tsKind[i],
	}
	if timestamps, ok := cache[key]; ok {
		return timestamps, nil
	}
	timestamps, err := codec.DecodeTimestamps(codec.TimestampEncoding{
		Strategy: rb.tsKind[i],
		Count:    int(rb.tsN[i]),
		Base:     rb.tsBase[i],
		Step:     rb.tsStep[i],
		Payload:  payload,
	})
	if err != nil {
		return nil, err
	}
	// Heavy timestamp dedup (WriteFile shares one chunk across identical tick
	// vectors) means this miss runs ~once per block; cache an independent copy
	// and return the decoded slice (callers copy per-sample before the buffer
	// is reused).
	cache[key] = append([]int64(nil), timestamps...)
	return timestamps, nil
}

// readResidentChunkSection bulk-reads [0,dataEnd) (the contiguous timestamp+value
// region preceding the directory) so the scan slices each series' payload from
// memory. Returns nil (per-series fallback) for an empty or oversized region.
func readResidentChunkSection(file *os.File, dataEnd int64) ([]byte, error) {
	if dataEnd <= 0 || dataEnd > maxBulkSectionBytes {
		return nil, nil
	}
	buf := make([]byte, dataEnd)
	if _, err := file.ReadAt(buf, 0); err != nil {
		return nil, err
	}
	return buf, nil
}

// ResidentRetainedBytes estimates the heap a ResidentBlock holds, for the
// store's byte-budgeted residentCache.
func (rb *ResidentBlock) ResidentRetainedBytes() int64 {
	const perSeries = 8 + 1 + 8 + 8 + 8 + 8 + // id,typ,timeMin,timeMax,zoneMin,zoneMax
		8 + 8 + 4 + 8 + 8 + 1 + // ts fields
		8 + 8 + 4 + 8 + 1 // val fields
	var labelBytes int64
	for _, codes := range rb.labelCode {
		labelBytes += int64(len(codes)) * 4
	}
	var dictBytes int64
	for _, s := range rb.valueDict {
		dictBytes += int64(len(s)) + 16
	}
	return int64(rb.n)*perSeries + labelBytes + dictBytes
}
