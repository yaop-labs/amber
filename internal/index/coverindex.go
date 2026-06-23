package index

// Covering span index (.cidx): a seekable side structure that lets trace-summary
// queries answer service[/operation/duration] searches WITHOUT decompressing row
// blocks. For each service value it stores a column-strided projection of its
// spans — (trace-ordinal, start, duration, status, operation, is-root) — in the
// SAME sorted-id order as the BID3 service posting, plus per-segment trace_id and
// operation dictionaries. The full spans stay in the row store; this is a derived
// covering index, not a columnar layout. See traces-covering-index-design.md.

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"
)

var coverIndexMagic = [4]byte{'C', 'I', 'X', '1'}

var (
	coverZstdEncOnce sync.Once
	coverZstdEnc     *zstd.Encoder
	coverZstdDecOnce sync.Once
	coverZstdDec     *zstd.Decoder
)

func coverEnc() *zstd.Encoder {
	coverZstdEncOnce.Do(func() {
		coverZstdEnc, _ = zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	})
	return coverZstdEnc
}

func coverDec() *zstd.Decoder {
	coverZstdDecOnce.Do(func() {
		coverZstdDec, _ = zstd.NewReader(nil)
	})
	return coverZstdDec
}

// Projection is one service's decoded covering rows, parallel arrays in sorted-id
// (≈ time) order. End is reconstructed as Start+Duration.
type Projection struct {
	TraceOrd []uint32
	Start    []int64 // unix nanos
	End      []int64 // unix nanos
	Status   []uint8
	OpID     []uint32
	IsRoot   []bool
}

func (p *Projection) Len() int { return len(p.TraceOrd) }

// --- builder (seal time) ---

type coverRow struct {
	id       uint64
	traceOrd uint32
	start    int64
	end      int64
	status   uint8
	opID     uint32
	isRoot   bool
}

// CoverBuilder accumulates per-service projection rows during a single seal scan.
type CoverBuilder struct {
	services  map[string][]coverRow
	traceDict map[[16]byte]uint32
	traceList [][16]byte
	opDict    map[string]uint32
	opList    []string
}

func NewCoverBuilder() *CoverBuilder {
	return &CoverBuilder{
		services:  make(map[string][]coverRow),
		traceDict: make(map[[16]byte]uint32),
		opDict:    make(map[string]uint32),
	}
}

func (b *CoverBuilder) traceOrdinal(t [16]byte) uint32 {
	if o, ok := b.traceDict[t]; ok {
		return o
	}
	o := uint32(len(b.traceList))
	b.traceDict[t] = o
	b.traceList = append(b.traceList, t)
	return o
}

func (b *CoverBuilder) opID(op string) uint32 {
	if id, ok := b.opDict[op]; ok {
		return id
	}
	id := uint32(len(b.opList))
	b.opDict[op] = id
	b.opList = append(b.opList, op)
	return id
}

// Add records one span under its service. service must be non-empty.
func (b *CoverBuilder) Add(service string, id uint64, traceID [16]byte, start, end int64, status uint8, op string, isRoot bool) {
	b.services[service] = append(b.services[service], coverRow{
		id:       id,
		traceOrd: b.traceOrdinal(traceID),
		start:    start,
		end:      end,
		status:   status,
		opID:     b.opID(op),
		isRoot:   isRoot,
	})
}

// Empty reports whether no spans were added (skip writing a file).
func (b *CoverBuilder) Empty() bool { return len(b.services) == 0 }

type coverLoc struct{ off, length int64 }

// Save writes the .cidx atomically in the CIX1 format.
func (b *CoverBuilder) Save(path string) error {
	return atomicWrite(path, func(f *os.File) error {
		var off int64
		crc := crc32.NewIEEE()
		write := func(p []byte) error {
			crc.Write(p)
			n, err := f.Write(p)
			off += int64(n)
			return err
		}
		var scratch [binary.MaxVarintLen64]byte
		uv := func(v uint64) error {
			n := binary.PutUvarint(scratch[:], v)
			return write(scratch[:n])
		}

		if err := write(coverIndexMagic[:]); err != nil {
			return err
		}

		services := make([]string, 0, len(b.services))
		for s := range b.services {
			services = append(services, s)
		}
		slices.Sort(services)

		// Payload region: one CRC-prefixed zstd blob per service.
		locs := make(map[string]coverLoc, len(services))
		for _, svc := range services {
			rows := b.services[svc]
			slices.SortFunc(rows, func(a, c coverRow) int {
				switch {
				case a.id < c.id:
					return -1
				case a.id > c.id:
					return 1
				default:
					return 0
				}
			})
			blob := encodeServiceBlob(rows)
			start := off
			if err := write(blob); err != nil {
				return err
			}
			locs[svc] = coverLoc{off: start, length: int64(len(blob))}
		}

		// Trace dictionary: fixed-width 16-byte ids, random-access by ordinal.
		traceOff := off
		for _, t := range b.traceList {
			if err := write(t[:]); err != nil {
				return err
			}
		}
		traceLoc := coverLoc{off: traceOff, length: off - traceOff}

		// Operation dictionary: varint-framed strings (small; loaded resident).
		opOff := off
		if err := uv(uint64(len(b.opList))); err != nil {
			return err
		}
		for _, op := range b.opList {
			if err := uv(uint64(len(op))); err != nil {
				return err
			}
			if err := write([]byte(op)); err != nil {
				return err
			}
		}
		opLoc := coverLoc{off: opOff, length: off - opOff}

		// Directory.
		dirOff := off
		if err := uv(uint64(len(services))); err != nil {
			return err
		}
		for _, svc := range services {
			l := locs[svc]
			if err := uv(uint64(len(svc))); err != nil {
				return err
			}
			if err := write([]byte(svc)); err != nil {
				return err
			}
			if err := uv(uint64(l.off)); err != nil {
				return err
			}
			if err := uv(uint64(l.length)); err != nil {
				return err
			}
		}
		if err := uv(uint64(traceLoc.off)); err != nil {
			return err
		}
		if err := uv(uint64(traceLoc.length)); err != nil {
			return err
		}
		if err := uv(uint64(opLoc.off)); err != nil {
			return err
		}
		if err := uv(uint64(opLoc.length)); err != nil {
			return err
		}

		var footer [12]byte
		binary.LittleEndian.PutUint64(footer[:8], uint64(dirOff))
		binary.LittleEndian.PutUint32(footer[8:], crc.Sum32())
		_, err := f.Write(footer[:])
		return err
	})
}

// encodeServiceBlob builds [crc32][zstd(SoA streams)] for one service's rows
// (already id-sorted). The id stream is omitted — it comes from the BID3 posting.
func encodeServiceBlob(rows []coverRow) []byte {
	var body []byte
	body = binary.AppendUvarint(body, uint64(len(rows)))

	var traceS, startS, durS, opS []byte
	var prevOrd int64
	var prevStart int64
	for i, r := range rows {
		ord := int64(r.traceOrd)
		if i == 0 {
			traceS = binary.AppendVarint(traceS, ord)
			startS = binary.AppendVarint(startS, r.start)
		} else {
			traceS = binary.AppendVarint(traceS, ord-prevOrd)
			startS = binary.AppendVarint(startS, r.start-prevStart)
		}
		prevOrd, prevStart = ord, r.start
		d := max(r.end-r.start, 0)
		durS = binary.AppendUvarint(durS, uint64(d))
		opS = binary.AppendUvarint(opS, uint64(r.opID))
	}

	appendStream := func(dst, s []byte) []byte {
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		return append(dst, s...)
	}
	body = appendStream(body, traceS)
	body = appendStream(body, startS)
	body = appendStream(body, durS)
	body = appendStream(body, opS)
	// status: one byte per row.
	for _, r := range rows {
		body = append(body, r.status)
	}
	// is-root: bitset.
	bitset := make([]byte, (len(rows)+7)/8)
	for i, r := range rows {
		if r.isRoot {
			bitset[i>>3] |= 1 << (uint(i) & 7)
		}
	}
	body = append(body, bitset...)

	compressed := coverEnc().EncodeAll(body, nil)
	out := make([]byte, 4, 4+len(compressed))
	binary.LittleEndian.PutUint32(out, crc32.ChecksumIEEE(compressed))
	return append(out, compressed...)
}

// --- reader (query time) ---

// CoverIndex reads a .cidx lazily: only the directory and the small operation
// dictionary are held; service projections and trace ids are pread on demand.
type CoverIndex struct {
	path     string
	dir      map[string]coverLoc
	traceLoc coverLoc
	opOff    int64
	opLen    int64
	ops      []string
}

// OpenCoverIndex reads the directory + operation dictionary of a CIX1 file.
func OpenCoverIndex(path string) (*CoverIndex, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("coverindex: open %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := info.Size()
	if size < 16 {
		return nil, errors.New("coverindex: too small")
	}
	var magic [4]byte
	if _, err := f.ReadAt(magic[:], 0); err != nil {
		return nil, err
	}
	if magic != coverIndexMagic {
		return nil, errors.New("coverindex: bad magic")
	}
	var footer [12]byte
	if _, err := f.ReadAt(footer[:], size-12); err != nil {
		return nil, err
	}
	dirOff := int64(binary.LittleEndian.Uint64(footer[:8]))
	if dirOff < 4 || dirOff > size-12 {
		return nil, errors.New("coverindex: bad directory offset")
	}
	dirBytes := make([]byte, size-12-dirOff)
	if _, err := f.ReadAt(dirBytes, dirOff); err != nil {
		return nil, err
	}
	ci := &CoverIndex{path: path, dir: make(map[string]coverLoc)}
	if err := ci.parseDirectory(dirBytes, size-12); err != nil {
		return nil, err
	}
	if err := ci.loadOps(f); err != nil {
		return nil, err
	}
	return ci, nil
}

func (ci *CoverIndex) parseDirectory(b []byte, payloadEnd int64) error {
	r := b
	take := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errors.New("coverindex: truncated directory varint")
		}
		r = r[n:]
		return v, nil
	}
	takeStr := func() (string, error) {
		l, err := take()
		if err != nil {
			return "", err
		}
		if l > uint64(len(r)) {
			return "", errors.New("coverindex: directory string out of range")
		}
		s := string(r[:l])
		r = r[l:]
		return s, nil
	}
	checkLoc := func(off, length uint64) (coverLoc, error) {
		if int64(off) < 4 || int64(off+length) > payloadEnd {
			return coverLoc{}, errors.New("coverindex: blob out of range")
		}
		return coverLoc{off: int64(off), length: int64(length)}, nil
	}

	svcCount, err := take()
	if err != nil {
		return err
	}
	for range svcCount {
		name, err := takeStr()
		if err != nil {
			return err
		}
		off, err := take()
		if err != nil {
			return err
		}
		length, err := take()
		if err != nil {
			return err
		}
		loc, err := checkLoc(off, length)
		if err != nil {
			return err
		}
		ci.dir[name] = loc
	}
	traceOff, err := take()
	if err != nil {
		return err
	}
	traceLen, err := take()
	if err != nil {
		return err
	}
	if ci.traceLoc, err = checkLoc(traceOff, traceLen); err != nil {
		return err
	}
	if ci.traceLoc.length%16 != 0 {
		return errors.New("coverindex: trace dict not 16-aligned")
	}
	opOff, err := take()
	if err != nil {
		return err
	}
	opLen, err := take()
	if err != nil {
		return err
	}
	if _, err := checkLoc(opOff, opLen); err != nil {
		return err
	}
	ci.opOff, ci.opLen = int64(opOff), int64(opLen)
	return nil
}

func (ci *CoverIndex) loadOps(f *os.File) error {
	if ci.opLen == 0 {
		return nil
	}
	buf := make([]byte, ci.opLen)
	if _, err := f.ReadAt(buf, ci.opOff); err != nil {
		return err
	}
	count, n := binary.Uvarint(buf)
	if n <= 0 {
		return errors.New("coverindex: truncated op dict")
	}
	buf = buf[n:]
	ci.ops = make([]string, 0, count)
	for range count {
		l, n := binary.Uvarint(buf)
		if n <= 0 || l > uint64(len(buf)-n) {
			return errors.New("coverindex: truncated op string")
		}
		buf = buf[n:]
		ci.ops = append(ci.ops, string(buf[:l]))
		buf = buf[l:]
	}
	return nil
}

// HasService reports whether the index covers a service value.
func (ci *CoverIndex) HasService(service string) bool {
	_, ok := ci.dir[service]
	return ok
}

// Operation maps an op id back to its string ("" if out of range).
func (ci *CoverIndex) Operation(id uint32) string {
	if int(id) < len(ci.ops) {
		return ci.ops[id]
	}
	return ""
}

// OpID maps an operation string to its id, ok=false if absent (so an operation
// filter for an op not in this segment can short-circuit).
func (ci *CoverIndex) OpID(op string) (uint32, bool) {
	for i, s := range ci.ops {
		if s == op {
			return uint32(i), true
		}
	}
	return 0, false
}

// ReadServiceProjection preads and decodes one service's covering rows.
func (ci *CoverIndex) ReadServiceProjection(service string) (*Projection, error) {
	loc, ok := ci.dir[service]
	if !ok {
		return nil, nil
	}
	f, err := os.Open(ci.path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	buf := make([]byte, loc.length)
	if _, err := f.ReadAt(buf, loc.off); err != nil {
		return nil, err
	}
	if crc32.ChecksumIEEE(buf[4:]) != binary.LittleEndian.Uint32(buf[:4]) {
		return nil, errors.New("coverindex: service blob crc mismatch")
	}
	return decodeServiceBlob(buf[4:])
}

// TraceIDs resolves ordinals (segment-local) to their 16-byte trace ids in one
// open, preading the fixed-width dictionary.
func (ci *CoverIndex) TraceIDs(ords []uint32) ([][16]byte, error) {
	f, err := os.Open(ci.path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	out := make([][16]byte, len(ords))
	n := ci.traceLoc.length / 16
	for i, ord := range ords {
		if int64(ord) >= n {
			return nil, fmt.Errorf("coverindex: trace ordinal %d out of range", ord)
		}
		if _, err := f.ReadAt(out[i][:], ci.traceLoc.off+int64(ord)*16); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func decodeServiceBlob(compressed []byte) (*Projection, error) {
	body, err := coverDec().DecodeAll(compressed, nil)
	if err != nil {
		return nil, fmt.Errorf("coverindex: decompress: %w", err)
	}
	r := body
	takeUv := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errors.New("coverindex: truncated varint")
		}
		r = r[n:]
		return v, nil
	}
	takeStream := func() ([]byte, error) {
		l, err := takeUv()
		if err != nil {
			return nil, err
		}
		if l > uint64(len(r)) {
			return nil, errors.New("coverindex: stream out of range")
		}
		s := r[:l]
		r = r[l:]
		return s, nil
	}

	rowCount, err := takeUv()
	if err != nil {
		return nil, err
	}
	traceS, err := takeStream()
	if err != nil {
		return nil, err
	}
	startS, err := takeStream()
	if err != nil {
		return nil, err
	}
	durS, err := takeStream()
	if err != nil {
		return nil, err
	}
	opS, err := takeStream()
	if err != nil {
		return nil, err
	}
	statusLen := int(rowCount)
	bitsetLen := (int(rowCount) + 7) / 8
	if len(r) != statusLen+bitsetLen {
		return nil, errors.New("coverindex: trailing length mismatch")
	}
	statusB := r[:statusLen]
	bitset := r[statusLen:]

	p := &Projection{
		TraceOrd: make([]uint32, rowCount),
		Start:    make([]int64, rowCount),
		End:      make([]int64, rowCount),
		Status:   make([]uint8, rowCount),
		OpID:     make([]uint32, rowCount),
		IsRoot:   make([]bool, rowCount),
	}
	var ord, start int64
	for i := range int(rowCount) {
		dOrd, n := binary.Varint(traceS)
		if n <= 0 {
			return nil, errors.New("coverindex: truncated trace stream")
		}
		traceS = traceS[n:]
		dStart, n := binary.Varint(startS)
		if n <= 0 {
			return nil, errors.New("coverindex: truncated start stream")
		}
		startS = startS[n:]
		if i == 0 {
			ord, start = dOrd, dStart
		} else {
			ord += dOrd
			start += dStart
		}
		dur, n := binary.Uvarint(durS)
		if n <= 0 {
			return nil, errors.New("coverindex: truncated dur stream")
		}
		durS = durS[n:]
		opv, n := binary.Uvarint(opS)
		if n <= 0 {
			return nil, errors.New("coverindex: truncated op stream")
		}
		opS = opS[n:]

		p.TraceOrd[i] = uint32(ord)
		p.Start[i] = start
		p.End[i] = start + int64(dur)
		p.Status[i] = statusB[i]
		p.OpID[i] = uint32(opv)
		p.IsRoot[i] = bitset[i>>3]&(1<<(uint(i)&7)) != 0
	}
	return p, nil
}
