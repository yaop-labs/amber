package index

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"sync"

	"github.com/RoaringBitmap/roaring/v2/roaring64"
)

type BitmapIndex struct {
	mu     sync.RWMutex
	values map[string]*valueBucket
}

type valueBucket struct {
	mu      sync.Mutex
	ids     []uint64
	sorted  bool
	roaring *roaring64.Bitmap
	dirty   bool

	sortedFrozen []uint64
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
	v.dirty = true
	v.sortedFrozen = nil
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
	v.dirty = true
	v.sortedFrozen = nil
	v.mu.Unlock()
}

func (v *valueBucket) materializeLocked() *roaring64.Bitmap {
	if !v.dirty && v.roaring != nil {
		return v.roaring
	}
	if v.ids != nil {
		if !v.sorted {
			slices.Sort(v.ids)
			v.sorted = true
		}
		v.ids = slices.Compact(v.ids)
		bm := roaring64.New()
		bm.AddMany(v.ids)
		v.roaring = bm
	} else if v.roaring == nil {
		v.roaring = roaring64.New()
	}
	v.dirty = false
	return v.roaring
}

func (v *valueBucket) materialize() *roaring64.Bitmap {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.materializeLocked()
}

func (v *valueBucket) sortedShared() []uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.sortedFrozen != nil {
		return v.sortedFrozen
	}
	v.materializeLocked()
	if v.roaring == nil {
		return nil
	}
	v.sortedFrozen = v.roaring.ToArray()
	return v.sortedFrozen
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

func (b *BitmapIndex) getShared(value string) *roaring64.Bitmap {
	b.mu.RLock()
	vb, ok := b.values[value]
	b.mu.RUnlock()
	if !ok {
		return nil
	}
	return vb.materialize()
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

func (m *MultiFieldIndex) Filter(conditions map[string]string) *roaring64.Bitmap {
	var result *roaring64.Bitmap
	first := true
	for field, value := range conditions {
		m.mu.RLock()
		bi, ok := m.fields[field]
		m.mu.RUnlock()
		if !ok {
			return roaring64.New()
		}
		bm := bi.getShared(value)
		if bm == nil || bm.IsEmpty() {
			return roaring64.New()
		}
		if first {
			result = bm
			first = false
		} else {
			result = roaring64.And(result, bm)
			if result.IsEmpty() {
				return result
			}
		}
	}
	if result == nil {
		return roaring64.New()
	}
	return result
}

func (m *MultiFieldIndex) FilterWithSorted(conditions map[string]string) (*roaring64.Bitmap, []uint64) {
	if len(conditions) != 1 {
		return m.Filter(conditions), nil
	}
	var field, value string
	for f, v := range conditions {
		field, value = f, v
	}
	m.mu.RLock()
	bi, ok := m.fields[field]
	m.mu.RUnlock()
	if !ok {
		return roaring64.New(), nil
	}
	bm := bi.getShared(value)
	if bm == nil || bm.IsEmpty() {
		return roaring64.New(), nil
	}
	return bm, bi.getSortedShared(value)
}

func (m *MultiFieldIndex) FilterAny(field string, values []string) *roaring64.Bitmap {
	m.mu.RLock()
	bi, ok := m.fields[field]
	m.mu.RUnlock()
	if !ok {
		return roaring64.New()
	}
	result := roaring64.New()
	for _, v := range values {
		if bm := bi.getShared(v); bm != nil {
			result.Or(bm)
		}
	}
	return result
}

var bitmapIndexMagic = [4]byte{'B', 'I', 'D', 'X'}

func (m *MultiFieldIndex) Save(path string) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var buf bytes.Buffer
	buf.Write(bitmapIndexMagic[:])
	writeUint32Buf(&buf, uint32(len(m.fields)))

	for field, bi := range m.fields {
		writeStringBuf(&buf, field)
		bi.mu.RLock()
		writeUint32Buf(&buf, uint32(len(bi.values)))
		for value, vb := range bi.values {
			bm := vb.materialize()
			writeStringBuf(&buf, value)
			bitmapData, err := bm.MarshalBinary()
			if err != nil {
				bi.mu.RUnlock()
				return fmt.Errorf("bitmap: marshal %s=%s: %w", field, value, err)
			}
			writeUint32Buf(&buf, uint32(len(bitmapData)))
			buf.Write(bitmapData)
		}
		bi.mu.RUnlock()
	}

	if err := atomicWriteFile(path, buf.Bytes()); err != nil {
		return fmt.Errorf("bitmap: %w", err)
	}
	return nil
}

func LoadMultiFieldIndex(path string) (*MultiFieldIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("bitmap: read %s: %w", path, err)
	}

	r := bytes.NewReader(data)

	var magic [4]byte
	if _, err := r.Read(magic[:]); err != nil {
		return nil, fmt.Errorf("bitmap: read magic: %w", err)
	}
	if magic != bitmapIndexMagic {
		return nil, fmt.Errorf("bitmap: bad magic %v", magic)
	}

	fieldCount := readUint32Buf(r)
	m := NewMultiFieldIndex()

	for range fieldCount {
		field := readStringBuf(r)
		bi := NewBitmapIndex()

		valueCount := readUint32Buf(r)
		for range valueCount {
			value := readStringBuf(r)

			bitmapSize := readUint32Buf(r)
			bitmapData := make([]byte, bitmapSize)
			if _, err := r.Read(bitmapData); err != nil {
				return nil, fmt.Errorf("bitmap: read bitmap data: %w", err)
			}

			bm := roaring64.New()
			if err := bm.UnmarshalBinary(bitmapData); err != nil {
				return nil, fmt.Errorf("bitmap: unmarshal %s=%s: %w", field, value, err)
			}
			bi.values[value] = &valueBucket{roaring: bm, dirty: false, sorted: true}
		}

		m.fields[field] = bi
	}

	return m, nil
}

func writeUint32Buf(w *bytes.Buffer, v uint32) {
	w.WriteByte(byte(v))
	w.WriteByte(byte(v >> 8))
	w.WriteByte(byte(v >> 16))
	w.WriteByte(byte(v >> 24))
}

func writeStringBuf(w *bytes.Buffer, s string) {
	l := uint16(len(s))
	w.WriteByte(byte(l))
	w.WriteByte(byte(l >> 8))
	w.WriteString(s)
}

func readUint32Buf(r *bytes.Reader) uint32 {
	b := make([]byte, 4)
	_, _ = r.Read(b)
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

func readStringBuf(r *bytes.Reader) string {
	b := make([]byte, 2)
	_, _ = r.Read(b)
	l := int(b[0]) | int(b[1])<<8
	if l == 0 {
		return ""
	}
	s := make([]byte, l)
	_, _ = r.Read(s)
	return string(s)
}
