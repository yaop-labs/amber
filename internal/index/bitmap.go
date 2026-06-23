package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"slices"
	"sync"
)

// BitmapIndex maps a field value to the sorted set of entry IDs carrying it.
//
// Sets are plain sorted []uint64, not roaring bitmaps: entry IDs derive from
// ULIDs, whose high bits are millisecond timestamps, so roaring64 degraded
// to one container per handful of IDs — in the first comparison run the
// parsed .bidx bitmaps were 76% of amber's live heap and 82% of all live
// objects (9.9M roaring containers for ~5M records). Sorted slices hold the
// same sets in a few flat allocations and intersect/union by merge.
type BitmapIndex struct {
	mu     sync.RWMutex
	values map[string]*valueBucket
}

type valueBucket struct {
	mu     sync.Mutex
	ids    []uint64
	sorted bool
	// normalized marks ids as sorted and deduplicated; reads hand out the
	// slice itself, so writers must never mutate it in place afterwards
	// (add appends, which is safe: readers hold their own len).
	normalized bool
}

func newBucket() *valueBucket {
	return &valueBucket{sorted: true}
}

func (v *valueBucket) add(id uint64) {
	v.mu.Lock()
	if v.sorted && len(v.ids) > 0 && id < v.ids[len(v.ids)-1] {
		v.sorted = false
	}
	v.ids = append(v.ids, id)
	v.normalized = false
	v.mu.Unlock()
}

func (v *valueBucket) addManySorted(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	v.mu.Lock()
	if v.sorted && len(v.ids) > 0 && ids[0] < v.ids[len(v.ids)-1] {
		v.sorted = false
	}
	v.ids = append(v.ids, ids...)
	v.normalized = false
	v.mu.Unlock()
}

// sortedShared returns the normalized ID set. The slice is shared: callers
// must treat it as read-only.
func (v *valueBucket) sortedShared() []uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	if !v.normalized {
		if !v.sorted {
			slices.Sort(v.ids)
			v.sorted = true
		}
		v.ids = slices.Compact(v.ids)
		v.normalized = true
	}
	return v.ids
}

func NewBitmapIndex() *BitmapIndex {
	return &BitmapIndex{
		values: make(map[string]*valueBucket),
	}
}

func (b *BitmapIndex) bucket(value string) *valueBucket {
	b.mu.RLock()
	vb, ok := b.values[value]
	b.mu.RUnlock()
	if ok {
		return vb
	}
	b.mu.Lock()
	vb, ok = b.values[value]
	if !ok {
		vb = newBucket()
		b.values[value] = vb
	}
	b.mu.Unlock()
	return vb
}

// Add records that entryID carries value.
func (b *BitmapIndex) Add(value string, entryID uint64) {
	b.bucket(value).add(entryID)
}

// AddMany records that every id in ids carries value.
func (b *BitmapIndex) AddMany(value string, ids []uint64) {
	if len(ids) == 0 {
		return
	}
	slices.Sort(ids)
	b.bucket(value).addManySorted(ids)
}

func (b *BitmapIndex) getSortedShared(value string) []uint64 {
	b.mu.RLock()
	vb, ok := b.values[value]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	return vb.sortedShared()
}

func (b *BitmapIndex) Values() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	result := make([]string, 0, len(b.values))
	for v := range b.values {
		result = append(result, v)
	}
	return result
}

func (b *BitmapIndex) Cardinality() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.values)
}

// MultiFieldIndex is a set of BitmapIndexes keyed by field name, the per-segment
// index that answers field=value (and AND/OR) entry-ID lookups for queries.
type MultiFieldIndex struct {
	mu     sync.RWMutex
	fields map[string]*BitmapIndex
}

func NewMultiFieldIndex() *MultiFieldIndex {
	return &MultiFieldIndex{
		fields: make(map[string]*BitmapIndex),
	}
}

func (m *MultiFieldIndex) FieldValues(field string) []string {
	m.mu.RLock()
	bi, ok := m.fields[field]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return bi.Values()
}

func (m *MultiFieldIndex) HasField(field string) bool {
	m.mu.RLock()
	_, ok := m.fields[field]
	m.mu.RUnlock()
	return ok
}

func (m *MultiFieldIndex) Add(field, value string, entryID uint64) {
	m.GetOrCreate(field).Add(value, entryID)
}

func (m *MultiFieldIndex) GetOrCreate(field string) *BitmapIndex {
	m.mu.RLock()
	bi, ok := m.fields[field]
	m.mu.RUnlock()
	if ok {
		return bi
	}
	m.mu.Lock()
	bi, ok = m.fields[field]
	if !ok {
		bi = NewBitmapIndex()
		m.fields[field] = bi
	}
	m.mu.Unlock()
	return bi
}

// Filter intersects single-value per-field conditions (AND). The result is
// freshly allocated unless it aliases one shared set (single condition).
func (m *MultiFieldIndex) Filter(conditions map[string]string) []uint64 {
	var result []uint64
	first := true
	for field, value := range conditions {
		m.mu.RLock()
		bi, ok := m.fields[field]
		m.mu.RUnlock()
		if !ok {
			return nil
		}
		ids := bi.getSortedShared(value)
		if len(ids) == 0 {
			return nil
		}
		if first {
			result = ids
			first = false
			continue
		}
		result = IntersectSorted(result, ids)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

// FilterMulti intersects per-field conditions where each field matches any of
// its values (OR within a field, AND across fields).
func (m *MultiFieldIndex) FilterMulti(conditions map[string][]string) []uint64 {
	var result []uint64
	first := true
	for field, values := range conditions {
		fieldIDs := m.FilterAny(field, values)
		if len(fieldIDs) == 0 {
			return nil
		}
		if first {
			result = fieldIDs
			first = false
			continue
		}
		result = IntersectSorted(result, fieldIDs)
		if len(result) == 0 {
			return nil
		}
	}
	return result
}

// FilterAny unions the value sets of one field (OR). Result aliases the
// shared set when only one value matches.
func (m *MultiFieldIndex) FilterAny(field string, values []string) []uint64 {
	m.mu.RLock()
	bi, ok := m.fields[field]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	var result []uint64
	for _, v := range values {
		ids := bi.getSortedShared(v)
		if len(ids) == 0 {
			continue
		}
		if result == nil {
			result = ids
			continue
		}
		result = UnionSorted(result, ids)
	}
	return result
}

// IntersectSorted returns the sorted intersection in a fresh slice — inputs
// are often shared cache state and must not be mutated.
//
// A plain linear merge costs O(len(a)+len(b)), which walks the larger set in
// full even when the smaller set is tiny. Span postings are highly skewed (a
// single-service trace means one service's posting ≈ 1/N of all spans), so when
// the sets differ a lot in size we gallop the small set through the large one in
// O(len(small)·log len(large)) instead. Balanced sizes stay on the linear merge,
// which is faster there and has no binary-search overhead.
func IntersectSorted(a, b []uint64) []uint64 {
	if len(b) < len(a) {
		a, b = b, a
	}
	// a is the smaller set. Gallop only when b dwarfs a enough that
	// len(a)·log2(len(b)) beats len(a)+len(b); the 8× gate is conservative.
	if len(a) > 0 && len(b) >= len(a)*8 {
		return intersectGalloping(a, b)
	}
	out := make([]uint64, 0, len(a))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}
	return out
}

// intersectGalloping intersects a sorted small set a into a sorted large set b
// by exponential+binary search, advancing the cursor in b so total work is
// O(len(a)·log len(b)). a must be the smaller set.
func intersectGalloping(a, b []uint64) []uint64 {
	out := make([]uint64, 0, len(a))
	j := 0
	for _, target := range a {
		j = gallopLowerBound(b, j, target)
		if j >= len(b) {
			break
		}
		if b[j] == target {
			out = append(out, target)
			j++
		}
	}
	return out
}

// gallopLowerBound returns the first index >= start whose value in b is >=
// target, using exponential search to bracket then binary search to land.
func gallopLowerBound(b []uint64, start int, target uint64) int {
	n := len(b)
	if start >= n || b[start] >= target {
		return start
	}
	// Exponential search for an upper bracket where b[start+hi] >= target.
	hi := 1
	for start+hi < n && b[start+hi] < target {
		hi *= 2
	}
	lo := start + hi/2 // b[lo] < target (hi/2 was the previous step, or start)
	h := min(start+hi, n)
	for lo < h {
		mid := lo + (h-lo)/2
		if b[mid] < target {
			lo = mid + 1
		} else {
			h = mid
		}
	}
	return lo
}

// UnionSorted returns the sorted union in a fresh slice.
func UnionSorted(a, b []uint64) []uint64 {
	out := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)
	return out
}

// On-disk formats:
//
//	"BIDX" — legacy roaring serialization. Rejected on load (rebuild/scan).
//	"BID2" — per field/value delta-varint postings, trailing whole-file CRC.
//	         Still read for back-compat, no longer written.
//	"BID3" — seekable: a postings region (each blob = CRC32 + count + deltas)
//	         followed by a directory (field → value → blob offset+length) and a
//	         12-byte footer (dirOffset:u64, fileCRC:u32). A reader can pull one
//	         field+value posting with a directory lookup and a single pread,
//	         instead of decoding the whole index — see SeekableBitmapIndex. The
//	         span query path consumed ~34% of QT2/QT3 CPU re-parsing full .bidx
//	         files on every query (a 32-slot cache vs 361 segments); BID3 reads
//	         only the postings a query actually needs.
var (
	bitmapIndexMagic   = [4]byte{'B', 'I', 'D', '2'}
	bitmapIndexMagicV3 = [4]byte{'B', 'I', 'D', '3'}
)

// appendPosting appends a posting blob payload (count + delta-varint ids) to dst.
func appendPosting(dst []byte, ids []uint64) []byte {
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(ids)))
	dst = append(dst, scratch[:n]...)
	var prev uint64
	for _, id := range ids {
		n := binary.PutUvarint(scratch[:], id-prev)
		dst = append(dst, scratch[:n]...)
		prev = id
	}
	return dst
}

// decodePosting decodes a posting payload (count + delta-varint ids).
func decodePosting(b []byte) ([]uint64, error) {
	count, n := binary.Uvarint(b)
	if n <= 0 {
		return nil, errors.New("bitmap: truncated posting count")
	}
	b = b[n:]
	ids := make([]uint64, 0, count)
	var prev uint64
	for range count {
		d, n := binary.Uvarint(b)
		if n <= 0 {
			return nil, errors.New("bitmap: truncated posting delta")
		}
		b = b[n:]
		prev += d
		ids = append(ids, prev)
	}
	return ids, nil
}

// Save writes the index to path atomically in the seekable BID3 format.
func (m *MultiFieldIndex) Save(path string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return atomicWrite(path, func(file *os.File) error {
		crc := crc32.NewIEEE()
		w := bufio.NewWriterSize(file, 1<<20)
		var off int64
		write := func(b []byte) error {
			crc.Write(b)
			n, err := w.Write(b)
			off += int64(n)
			return err
		}
		var scratch [binary.MaxVarintLen64]byte
		uv := func(v uint64) error {
			n := binary.PutUvarint(scratch[:], v)
			return write(scratch[:n])
		}

		if err := write(bitmapIndexMagicV3[:]); err != nil {
			return err
		}

		// Deterministic field/value order keeps output stable across saves.
		fields := make([]string, 0, len(m.fields))
		for f := range m.fields {
			fields = append(fields, f)
		}
		slices.Sort(fields)

		type loc struct{ off, length int64 }
		dir := make(map[string][]string, len(fields)) // field -> sorted values
		locs := make(map[string]map[string]loc, len(fields))

		// Postings region: one CRC-prefixed blob per (field, value).
		var blob []byte
		for _, field := range fields {
			bi := m.fields[field]
			bi.mu.RLock()
			values := make([]string, 0, len(bi.values))
			for v := range bi.values {
				values = append(values, v)
			}
			bi.mu.RUnlock()
			slices.Sort(values)
			dir[field] = values
			locs[field] = make(map[string]loc, len(values))
			for _, value := range values {
				bi.mu.RLock()
				vb := bi.values[value]
				bi.mu.RUnlock()
				ids := vb.sortedShared()
				blob = appendPosting(blob[:0], ids)
				var bc [4]byte
				binary.LittleEndian.PutUint32(bc[:], crc32.ChecksumIEEE(blob))
				blobOff := off
				if err := write(bc[:]); err != nil {
					return err
				}
				if err := write(blob); err != nil {
					return err
				}
				locs[field][value] = loc{off: blobOff, length: int64(4 + len(blob))}
			}
		}

		// Directory region.
		dirOff := off
		if err := uv(uint64(len(fields))); err != nil {
			return err
		}
		for _, field := range fields {
			if err := uv(uint64(len(field))); err != nil {
				return err
			}
			if err := write([]byte(field)); err != nil {
				return err
			}
			values := dir[field]
			if err := uv(uint64(len(values))); err != nil {
				return err
			}
			for _, value := range values {
				l := locs[field][value]
				if err := uv(uint64(len(value))); err != nil {
					return err
				}
				if err := write([]byte(value)); err != nil {
					return err
				}
				if err := uv(uint64(l.off)); err != nil {
					return err
				}
				if err := uv(uint64(l.length)); err != nil {
					return err
				}
			}
		}

		// Footer (not covered by fileCRC): dirOffset + CRC over magic..dirEnd.
		var footer [12]byte
		binary.LittleEndian.PutUint64(footer[:8], uint64(dirOff))
		binary.LittleEndian.PutUint32(footer[8:], crc.Sum32())
		if _, err := w.Write(footer[:]); err != nil {
			return err
		}
		return w.Flush()
	})
}

// LoadMultiFieldIndex reads a BID2 index from path, verifying the CRC. It
// rejects the legacy roaring ("BIDX") format and corrupt files with an error so
// the caller can fall back to a scan and rebuild.
func LoadMultiFieldIndex(path string) (*MultiFieldIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("bitmap: read %s: %w", path, err)
	}
	if len(data) >= 4 && bytes.Equal(data[:4], bitmapIndexMagicV3[:]) {
		return loadMultiFieldIndexV3(data)
	}
	if len(data) < 8 || !bytes.Equal(data[:4], bitmapIndexMagic[:]) {
		return nil, errors.New("bitmap: bad magic (old or corrupt .bidx)")
	}
	body, sum := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(sum) {
		return nil, errors.New("bitmap: crc mismatch")
	}

	r := body[4:]
	take := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errors.New("bitmap: truncated varint")
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
			return "", errors.New("bitmap: string length out of range")
		}
		s := string(r[:l])
		r = r[l:]
		return s, nil
	}

	fieldCount, err := take()
	if err != nil {
		return nil, err
	}
	m := NewMultiFieldIndex()
	for range fieldCount {
		field, err := takeStr()
		if err != nil {
			return nil, err
		}
		valueCount, err := take()
		if err != nil {
			return nil, err
		}
		bi := NewBitmapIndex()
		for range valueCount {
			value, err := takeStr()
			if err != nil {
				return nil, err
			}
			count, err := take()
			if err != nil {
				return nil, err
			}
			if count > uint64(len(r)) {
				return nil, errors.New("bitmap: id count out of range")
			}
			ids := make([]uint64, 0, count)
			var prev uint64
			for range count {
				d, err := take()
				if err != nil {
					return nil, err
				}
				prev += d
				ids = append(ids, prev)
			}
			bi.values[value] = &valueBucket{ids: ids, sorted: true, normalized: true}
		}
		m.fields[field] = bi
	}
	return m, nil
}
