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

func (b *BitmapIndex) Add(value string, entryID uint64) {
	b.bucket(value).add(entryID)
}

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
func IntersectSorted(a, b []uint64) []uint64 {
	if len(b) < len(a) {
		a, b = b, a
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

// On-disk format ("BID2"): per field, per value, delta-varint sorted IDs
// with a trailing CRC. Replaces the roaring serialization ("BIDX"), which
// is rejected on load — bootstrap rebuilds, queries fall back to scans
// until then.
var bitmapIndexMagic = [4]byte{'B', 'I', 'D', '2'}

func (m *MultiFieldIndex) Save(path string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return atomicWrite(path, func(file *os.File) error {
		crc := crc32.NewIEEE()
		w := bufio.NewWriterSize(file, 1<<20)
		mw := func(b []byte) error {
			crc.Write(b)
			_, err := w.Write(b)
			return err
		}
		var scratch [binary.MaxVarintLen64]byte
		uv := func(v uint64) error {
			n := binary.PutUvarint(scratch[:], v)
			return mw(scratch[:n])
		}

		if err := mw(bitmapIndexMagic[:]); err != nil {
			return err
		}
		if err := uv(uint64(len(m.fields))); err != nil {
			return err
		}
		for field, bi := range m.fields {
			if err := uv(uint64(len(field))); err != nil {
				return err
			}
			if err := mw([]byte(field)); err != nil {
				return err
			}
			bi.mu.RLock()
			values := bi.values
			bi.mu.RUnlock()
			if err := uv(uint64(len(values))); err != nil {
				return err
			}
			for value, vb := range values {
				ids := vb.sortedShared()
				if err := uv(uint64(len(value))); err != nil {
					return err
				}
				if err := mw([]byte(value)); err != nil {
					return err
				}
				if err := uv(uint64(len(ids))); err != nil {
					return err
				}
				var prev uint64
				for _, id := range ids {
					if err := uv(id - prev); err != nil {
						return err
					}
					prev = id
				}
			}
		}
		var sum [4]byte
		binary.LittleEndian.PutUint32(sum[:], crc.Sum32())
		if _, err := w.Write(sum[:]); err != nil {
			return err
		}
		return w.Flush()
	})
}

func LoadMultiFieldIndex(path string) (*MultiFieldIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("bitmap: read %s: %w", path, err)
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
