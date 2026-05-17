package index

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"slices"
)

// PostingList is an on-disk sorted inverted index for high-cardinality fixed-
// size keys (e.g. trace_id). It maps each key to the sorted list of record IDs
// in the segment that carry that key.
//
// Use this instead of a bitmap field when a field has near-unique values:
// bitmap-per-value overhead dominates index size for high-cardinality fields,
// whereas a posting list stores only the actual (key → ids) pairs.
//
// On-disk layout:
//
//	magic[4] | version[2] | key_size[2] | entry_count[4]   -- header (12 bytes)
//	for each entry (sorted by key):
//	  key[key_size] | id_count[4] | id[0..N][8 each]
//
// Lookup is a linear scan over entries; segments are small enough (≤100K
// records, ≤a few thousand unique trace_ids) that binary search is not worth
// the complexity. Add it if profiling shows this as a bottleneck.
const (
	postingMagic   = uint32(0x504C4958) // "PLIX"
	postingVersion = uint16(1)
)

// PostingList holds the in-memory representation loaded from a .pidx file.
type PostingList struct {
	keySize int
	entries []postingEntry
}

type postingEntry struct {
	key []byte
	ids []uint64 // sorted
}

// Lookup returns the sorted record IDs for the given key, or nil if not found.
func (p *PostingList) Lookup(key []byte) []uint64 {
	if p == nil || len(p.entries) == 0 || len(key) != p.keySize {
		return nil
	}
	for i := range p.entries {
		if string(p.entries[i].key) == string(key) {
			return p.entries[i].ids
		}
	}
	return nil
}

// rawPair is a flat (key, id) pair used during building. Using a flat slice
// of fixed-size structs instead of map[string][]uint64 avoids map overhead
// (~12 bytes/entry) and string allocations (~32 bytes/key), reducing peak
// RSS during sealing from ~7.6 MB to ~2.4 MB per 100K-record segment.
type rawPair struct {
	key [16]byte // fixed: trace_id is always 16 bytes
	id  uint64
}

// PostingListBuilder accumulates (key → record IDs) pairs during a segment
// scan and produces a PostingList.
type PostingListBuilder struct {
	keySize int
	pairs   []rawPair
}

func NewPostingListBuilder(keySize int) *PostingListBuilder {
	return &PostingListBuilder{keySize: keySize}
}

// Add records that recordID carries the given key.
func (b *PostingListBuilder) Add(key []byte, recordID uint64) {
	if len(key) != b.keySize || b.keySize != 16 {
		return
	}
	var p rawPair
	copy(p.key[:], key)
	p.id = recordID
	b.pairs = append(b.pairs, p)
}

// Build produces a PostingList from accumulated data. Sorts pairs by key then
// id, then groups runs of equal keys into postingEntry slices.
func (b *PostingListBuilder) Build() *PostingList {
	if len(b.pairs) == 0 {
		return &PostingList{keySize: b.keySize}
	}

	slices.SortFunc(b.pairs, func(a, c rawPair) int {
		if n := sliceCompare(a.key[:], c.key[:]); n != 0 {
			return n
		}
		if a.id < c.id {
			return -1
		}
		if a.id > c.id {
			return 1
		}
		return 0
	})

	entries := make([]postingEntry, 0, 64)
	i := 0
	for i < len(b.pairs) {
		j := i + 1
		for j < len(b.pairs) && b.pairs[j].key == b.pairs[i].key {
			j++
		}
		ids := make([]uint64, j-i)
		for k, p := range b.pairs[i:j] {
			ids[k] = p.id
		}
		key := make([]byte, 16)
		copy(key, b.pairs[i].key[:])
		entries = append(entries, postingEntry{key: key, ids: ids})
		i = j
	}

	return &PostingList{keySize: b.keySize, entries: entries}
}

// Save writes the PostingList to path atomically.
func (p *PostingList) Save(path string) error {
	return atomicWrite(path, func(f *os.File) error {
		return p.writeTo(f)
	})
}

func (p *PostingList) writeTo(w io.Writer) error {
	var hdr [12]byte
	binary.LittleEndian.PutUint32(hdr[0:4], postingMagic)
	binary.LittleEndian.PutUint16(hdr[4:6], postingVersion)
	binary.LittleEndian.PutUint16(hdr[6:8], uint16(p.keySize))
	binary.LittleEndian.PutUint32(hdr[8:12], uint32(len(p.entries)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}

	idBuf := make([]byte, 8)
	cntBuf := make([]byte, 4)
	for _, e := range p.entries {
		if _, err := w.Write(e.key); err != nil {
			return err
		}
		binary.LittleEndian.PutUint32(cntBuf, uint32(len(e.ids)))
		if _, err := w.Write(cntBuf); err != nil {
			return err
		}
		for _, id := range e.ids {
			binary.LittleEndian.PutUint64(idBuf, id)
			if _, err := w.Write(idBuf); err != nil {
				return err
			}
		}
	}
	return nil
}

// LoadPostingList reads a PostingList from path.
func LoadPostingList(path string) (*PostingList, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return readPostingList(f)
}

func readPostingList(r io.Reader) (*PostingList, error) {
	var hdr [12]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, fmt.Errorf("posting: read header: %w", err)
	}
	if binary.LittleEndian.Uint32(hdr[0:4]) != postingMagic {
		return nil, fmt.Errorf("posting: bad magic")
	}
	if binary.LittleEndian.Uint16(hdr[4:6]) != postingVersion {
		return nil, fmt.Errorf("posting: unsupported version")
	}
	keySize := int(binary.LittleEndian.Uint16(hdr[6:8]))
	entryCount := int(binary.LittleEndian.Uint32(hdr[8:12]))

	entries := make([]postingEntry, entryCount)
	cntBuf := make([]byte, 4)
	idBuf := make([]byte, 8)

	for i := range entries {
		key := make([]byte, keySize)
		if _, err := io.ReadFull(r, key); err != nil {
			return nil, fmt.Errorf("posting: read key %d: %w", i, err)
		}
		if _, err := io.ReadFull(r, cntBuf); err != nil {
			return nil, fmt.Errorf("posting: read id_count %d: %w", i, err)
		}
		idCount := int(binary.LittleEndian.Uint32(cntBuf))
		ids := make([]uint64, idCount)
		for j := range ids {
			if _, err := io.ReadFull(r, idBuf); err != nil {
				return nil, fmt.Errorf("posting: read id %d/%d: %w", i, j, err)
			}
			ids[j] = binary.LittleEndian.Uint64(idBuf)
		}
		entries[i] = postingEntry{key: key, ids: ids}
	}

	return &PostingList{keySize: keySize, entries: entries}, nil
}

func sliceCompare(a, b []byte) int {
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			if a[i] < b[i] {
				return -1
			}
			return 1
		}
	}
	return len(a) - len(b)
}
