package histogram

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"

	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

// Histogram block file layout (footer-last, mirroring the scalar block):
//
//	magic(4) || version(2)
//	|| sketch_section          # per-series payloads, packed back-to-back
//	|| directory(json)         # one HistEntry per series + shared bounds table
//	|| footer(24)              # dirOff i64 || dirLen i64 || dirCRC u32 || magic(4)
//
// Exp-histogram sketches live in the sketch_section (NOT a value column); each
// directory entry points at its payload offset and carries a Synopsis (zone-map)
// of min/max/sum/count for pruning without decoding the sketch.

const (
	histFileMagic   = "MHB1"
	histFooterMagic = "MHF1"
	histVersion     = uint16(1)
	histFooterSize  = 24
)

// Kind discriminates the two stored histogram representations.
type Kind uint8

const (
	KindExponential Kind = 1
	KindExplicit    Kind = 2
)

// Synopsis is the per-series zone-map: aggregate min/max/sum/count answerable
// without touching the sketch payload.
type Synopsis struct {
	Min       float64 `json:"min"`
	Max       float64 `json:"max"`
	Sum       float64 `json:"sum"`
	Count     uint64  `json:"count"`
	HasMinMax bool    `json:"has_min_max"`
}

// HistEntry is one series directory record.
type HistEntry struct {
	SeriesID   uint64         `json:"series_id"`
	Kind       Kind           `json:"kind"`
	Labels     model.LabelSet `json:"labels"`
	TimeMin    int64          `json:"time_min"`
	TimeMax    int64          `json:"time_max"`
	TickCount  int            `json:"tick_count"`
	PayloadOff int64          `json:"payload_off"`
	PayloadLen int64          `json:"payload_len"`
	BoundsRef  int            `json:"bounds_ref"` // index into Directory.Bounds; -1 for exp
	Synopsis   Synopsis       `json:"synopsis"`
}

// Directory is the histogram block directory.
type Directory struct {
	Version uint16      `json:"version"`
	Bounds  [][]float64 `json:"bounds"` // shared explicit boundary sets, stored once
	Series  []HistEntry `json:"series"`
}

// TimeRange returns the [min,max] timestamp span across all series.
func (d Directory) TimeRange() (int64, int64, bool) {
	if len(d.Series) == 0 {
		return 0, 0, false
	}
	mn, mx := d.Series[0].TimeMin, d.Series[0].TimeMax
	for _, e := range d.Series[1:] {
		if e.TimeMin < mn {
			mn = e.TimeMin
		}
		if e.TimeMax > mx {
			mx = e.TimeMax
		}
	}
	return mn, mx, true
}

// ExpSeries is an exp-histogram series to write: one sketch per tick.
type ExpSeries struct {
	ID         uint64
	Labels     model.LabelSet
	Timestamps []int64
	Sketches   []*ExponentialHistogram
}

// ExplicitSeries is an explicit-bucket series to write: one count vector per
// tick, all sharing the same Bounds.
type ExplicitSeries struct {
	ID         uint64
	Labels     model.LabelSet
	Timestamps []int64
	Buckets    []*ExplicitBucketHistogram
}

// DecodedExpSeries is a read-back exp-histogram series.
type DecodedExpSeries struct {
	Entry      HistEntry
	Timestamps []int64
	Sketches   []*ExponentialHistogram
}

// DecodedExplicitSeries is a read-back explicit-bucket series.
type DecodedExplicitSeries struct {
	Entry      HistEntry
	Bounds     []float64
	Timestamps []int64
	Buckets    []*ExplicitBucketHistogram
}

// WriteBlock encodes exp and explicit histogram series into a block at path,
// publishing atomically (temp -> fsync -> rename).
func WriteBlock(path string, exp []ExpSeries, explicit []ExplicitSeries) error {
	var buf bytes.Buffer
	buf.WriteString(histFileMagic)
	if err := binary.Write(&buf, binary.LittleEndian, histVersion); err != nil {
		return err
	}

	dir := Directory{Version: histVersion}
	boundsIndex := make(map[string]int)

	for _, s := range exp {
		if len(s.Timestamps) != len(s.Sketches) {
			return errors.New("histogram: exp series timestamp/sketch length mismatch")
		}
		off := int64(buf.Len())
		payload := encodeExpPayload(s)
		buf.Write(payload)
		dir.Series = append(dir.Series, HistEntry{
			SeriesID:   s.ID,
			Kind:       KindExponential,
			Labels:     s.Labels.Canonical(),
			TimeMin:    minTS(s.Timestamps),
			TimeMax:    maxTS(s.Timestamps),
			TickCount:  len(s.Sketches),
			PayloadOff: off,
			PayloadLen: int64(len(payload)),
			BoundsRef:  -1,
			Synopsis:   expSynopsis(s.Sketches),
		})
	}

	for _, s := range explicit {
		if len(s.Timestamps) != len(s.Buckets) {
			return errors.New("histogram: explicit series timestamp/bucket length mismatch")
		}
		bounds := sharedBounds(s.Buckets)
		ref := internBounds(&dir, boundsIndex, bounds)
		off := int64(buf.Len())
		payload := encodeExplicitPayload(s)
		buf.Write(payload)
		dir.Series = append(dir.Series, HistEntry{
			SeriesID:   s.ID,
			Kind:       KindExplicit,
			Labels:     s.Labels.Canonical(),
			TimeMin:    minTS(s.Timestamps),
			TimeMax:    maxTS(s.Timestamps),
			TickCount:  len(s.Buckets),
			PayloadOff: off,
			PayloadLen: int64(len(payload)),
			BoundsRef:  ref,
			Synopsis:   explicitSynopsis(s.Buckets),
		})
	}

	dirOff := int64(buf.Len())
	dirPayload, err := json.Marshal(dir)
	if err != nil {
		return err
	}
	buf.Write(dirPayload)
	if err := binary.Write(&buf, binary.LittleEndian, dirOff); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, int64(len(dirPayload))); err != nil {
		return err
	}
	if err := binary.Write(&buf, binary.LittleEndian, crc32.ChecksumIEEE(dirPayload)); err != nil {
		return err
	}
	buf.WriteString(histFooterMagic)

	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}

// ReadDirectory reads just the directory (cheap; enough for pruning by synopsis).
func ReadDirectory(path string) (Directory, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Directory{}, err
	}
	return parseDirectory(data)
}

func parseDirectory(data []byte) (Directory, error) {
	if len(data) < len(histFileMagic)+2+histFooterSize {
		return Directory{}, io.ErrUnexpectedEOF
	}
	if string(data[:len(histFileMagic)]) != histFileMagic {
		return Directory{}, errors.New("histogram: invalid file magic")
	}
	footer := data[len(data)-histFooterSize:]
	if string(footer[20:24]) != histFooterMagic {
		return Directory{}, errors.New("histogram: invalid footer magic")
	}
	dirOff := int64(binary.LittleEndian.Uint64(footer[0:8]))
	dirLen := int64(binary.LittleEndian.Uint64(footer[8:16]))
	dirCRC := binary.LittleEndian.Uint32(footer[16:20])
	if dirOff < 0 || dirLen < 0 || dirOff+dirLen > int64(len(data))-histFooterSize {
		return Directory{}, errors.New("histogram: invalid directory offset")
	}
	dirPayload := data[dirOff : dirOff+dirLen]
	if crc32.ChecksumIEEE(dirPayload) != dirCRC {
		return Directory{}, errors.New("histogram: directory checksum mismatch")
	}
	var dir Directory
	if err := json.Unmarshal(dirPayload, &dir); err != nil {
		return Directory{}, err
	}
	if dir.Version != histVersion {
		return Directory{}, errors.New("histogram: unsupported version")
	}
	return dir, nil
}

// ReadBlock decodes every series in the block.
func ReadBlock(path string) ([]DecodedExpSeries, []DecodedExplicitSeries, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}
	dir, err := parseDirectory(data)
	if err != nil {
		return nil, nil, err
	}
	var exps []DecodedExpSeries
	var explicits []DecodedExplicitSeries
	for _, e := range dir.Series {
		payload := data[e.PayloadOff : e.PayloadOff+e.PayloadLen]
		switch e.Kind {
		case KindExponential:
			ts, sk, err := decodeExpPayload(payload, e.TickCount)
			if err != nil {
				return nil, nil, err
			}
			exps = append(exps, DecodedExpSeries{Entry: e, Timestamps: ts, Sketches: sk})
		case KindExplicit:
			if e.BoundsRef < 0 || e.BoundsRef >= len(dir.Bounds) {
				return nil, nil, errors.New("histogram: bad bounds ref")
			}
			bounds := dir.Bounds[e.BoundsRef]
			ts, bk, err := decodeExplicitPayload(payload, e.TickCount, bounds)
			if err != nil {
				return nil, nil, err
			}
			explicits = append(explicits, DecodedExplicitSeries{Entry: e, Bounds: bounds, Timestamps: ts, Buckets: bk})
		default:
			return nil, nil, errors.New("histogram: unknown series kind")
		}
	}
	return exps, explicits, nil
}

// DecodeExpSeries decodes a single exp series payload from a directory entry.
func DecodeExpSeries(data []byte, e HistEntry) (DecodedExpSeries, error) {
	if e.Kind != KindExponential {
		return DecodedExpSeries{}, errors.New("histogram: entry is not exponential")
	}
	ts, sk, err := decodeExpPayload(data[e.PayloadOff:e.PayloadOff+e.PayloadLen], e.TickCount)
	if err != nil {
		return DecodedExpSeries{}, err
	}
	return DecodedExpSeries{Entry: e, Timestamps: ts, Sketches: sk}, nil
}

func internBounds(dir *Directory, idx map[string]int, bounds []float64) int {
	key := boundsKey(bounds)
	if i, ok := idx[key]; ok {
		return i
	}
	i := len(dir.Bounds)
	dir.Bounds = append(dir.Bounds, append([]float64(nil), bounds...))
	idx[key] = i
	return i
}

func boundsKey(bounds []float64) string {
	var b bytes.Buffer
	var tmp [8]byte
	for _, v := range bounds {
		binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v))
		b.Write(tmp[:])
	}
	return b.String()
}

func sharedBounds(buckets []*ExplicitBucketHistogram) []float64 {
	for _, b := range buckets {
		if b != nil {
			return b.Bounds
		}
	}
	return nil
}

func expSynopsis(sketches []*ExponentialHistogram) Synopsis {
	var s Synopsis
	for _, sk := range sketches {
		if sk == nil {
			continue
		}
		s.Sum += sk.Sum
		s.Count += sk.Count
		if sk.HasMinMax {
			if !s.HasMinMax {
				s.Min, s.Max, s.HasMinMax = sk.Min, sk.Max, true
			} else {
				if sk.Min < s.Min {
					s.Min = sk.Min
				}
				if sk.Max > s.Max {
					s.Max = sk.Max
				}
			}
		}
	}
	return s
}

func explicitSynopsis(buckets []*ExplicitBucketHistogram) Synopsis {
	var s Synopsis
	for _, b := range buckets {
		if b == nil {
			continue
		}
		s.Sum += b.Sum
		s.Count += b.Count
		if b.HasMinMax {
			if !s.HasMinMax {
				s.Min, s.Max, s.HasMinMax = b.Min, b.Max, true
			} else {
				if b.Min < s.Min {
					s.Min = b.Min
				}
				if b.Max > s.Max {
					s.Max = b.Max
				}
			}
		}
	}
	return s
}

func minTS(ts []int64) int64 {
	if len(ts) == 0 {
		return 0
	}
	mn := ts[0]
	for _, t := range ts[1:] {
		if t < mn {
			mn = t
		}
	}
	return mn
}

func maxTS(ts []int64) int64 {
	if len(ts) == 0 {
		return 0
	}
	mx := ts[0]
	for _, t := range ts[1:] {
		if t > mx {
			mx = t
		}
	}
	return mx
}

func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
