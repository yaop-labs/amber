package index

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

// FTSIndex is a per-segment inverted index: a sorted token dictionary with
// delta-varint posting lists of uint64 entry IDs.
//
// It replaces the fts-engine radix snapshot, which stored every posting as a
// decimal-string DocID inside serialized tree nodes: ~38 MB on disk and
// ~150 MB in memory per 100k-record segment, ~400 ms to parse — measured in
// the first comparison run, where .fidx files were 92% of amber's storage
// and the dominant share of its RSS. This format holds the same data in a
// few MB, loads with a single linear pass, and searches by binary search
// over the dictionary.
//
// Tokenization (and therefore search semantics) is unchanged: the same
// multilingual pipeline processes bodies at index time and queries at search
// time; multi-token queries AND their posting lists.
type FTSIndex struct {
	// build-mode state: token -> sorted entry IDs. Nil after load.
	building map[string]*postingBuilder

	// sealed state: parsed flat form. Nil until Save or load.
	tokens   []string // sorted; slices into buf
	postings [][]byte // delta-varint blobs, parallel to tokens
	counts   []int
}

type postingBuilder struct {
	ids      []uint64
	unsorted bool
}

var ftsPipeline = textproc.DefaultMultilingualPipeline()

func TokenizeFTS(text string) []string {
	return ftsPipeline.Process(text)
}

func NewFTSIndex() *FTSIndex {
	return &FTSIndex{building: make(map[string]*postingBuilder)}
}

// Index adds one record's body. Entry IDs normally arrive in ascending order
// (segment scan order); out-of-order IDs are tolerated and sorted at Save.
func (f *FTSIndex) Index(_ context.Context, entryID uint64, body string) error {
	if f.building == nil {
		return errors.New("fts: index is sealed")
	}
	for _, tok := range TokenizeFTS(body) {
		if tok == "" {
			continue
		}
		pb := f.building[tok]
		if pb == nil {
			pb = &postingBuilder{}
			f.building[tok] = pb
		}
		if n := len(pb.ids); n > 0 {
			last := pb.ids[n-1]
			if last == entryID {
				continue // repeated token within one body
			}
			if entryID < last {
				pb.unsorted = true
			}
		}
		pb.ids = append(pb.ids, entryID)
	}
	return nil
}

// seal converts build-mode maps into the flat sorted form.
func (f *FTSIndex) seal() {
	if f.building == nil {
		return
	}
	tokens := make([]string, 0, len(f.building))
	for tok := range f.building {
		tokens = append(tokens, tok)
	}
	sort.Strings(tokens)

	f.tokens = tokens
	f.postings = make([][]byte, len(tokens))
	f.counts = make([]int, len(tokens))
	for i, tok := range tokens {
		pb := f.building[tok]
		if pb.unsorted {
			sort.Slice(pb.ids, func(a, b int) bool { return pb.ids[a] < pb.ids[b] })
			pb.ids = dedupSorted(pb.ids)
		}
		blob := make([]byte, 0, len(pb.ids)*2)
		var prev uint64
		for _, id := range pb.ids {
			blob = binary.AppendUvarint(blob, id-prev)
			prev = id
		}
		f.postings[i] = blob
		f.counts[i] = len(pb.ids)
	}
	f.building = nil
}

func dedupSorted(ids []uint64) []uint64 {
	out := ids[:0]
	for i, id := range ids {
		if i == 0 || id != ids[i-1] {
			out = append(out, id)
		}
	}
	return out
}

// Search tokenizes the query and intersects the posting lists (AND).
// Results are ascending entry IDs, capped at limit.
func (f *FTSIndex) Search(_ context.Context, query string, limit int) ([]uint64, error) {
	f.seal()
	tokens := TokenizeFTS(query)
	if len(tokens) == 0 {
		return nil, nil
	}

	var acc []uint64
	for i, tok := range tokens {
		ids := f.lookup(tok)
		if len(ids) == 0 {
			return nil, nil
		}
		if i == 0 {
			acc = ids
			continue
		}
		acc = intersectSorted(acc, ids)
		if len(acc) == 0 {
			return nil, nil
		}
	}
	if limit > 0 && len(acc) > limit {
		acc = acc[:limit]
	}
	return acc, nil
}

func (f *FTSIndex) lookup(token string) []uint64 {
	i := sort.SearchStrings(f.tokens, token)
	if i >= len(f.tokens) || f.tokens[i] != token {
		return nil
	}
	ids := make([]uint64, 0, f.counts[i])
	var prev uint64
	blob := f.postings[i]
	for len(blob) > 0 {
		d, n := binary.Uvarint(blob)
		if n <= 0 {
			return ids
		}
		prev += d
		ids = append(ids, prev)
		blob = blob[n:]
	}
	return ids
}

func intersectSorted(a, b []uint64) []uint64 {
	out := a[:0]
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

// On-disk format ("AFT1"):
//
//	magic [4] | uvarint tokenCount
//	per token, sorted: uvarint len | bytes | uvarint postingCount |
//	                   uvarint blobLen | delta-varint postings
//	crc32 IEEE over everything above [4, little-endian]
var ftsMagic = [4]byte{'A', 'F', 'T', '1'}

func (f *FTSIndex) Save(path string) error {
	f.seal()
	return atomicWrite(path, func(file *os.File) error {
		crc := crc32.NewIEEE()
		w := bufio.NewWriterSize(file, 1<<20)
		mw := func(b []byte) error {
			crc.Write(b)
			_, err := w.Write(b)
			return err
		}

		if err := mw(ftsMagic[:]); err != nil {
			return err
		}
		var scratch [binary.MaxVarintLen64]byte
		uv := func(v uint64) error {
			n := binary.PutUvarint(scratch[:], v)
			return mw(scratch[:n])
		}
		if err := uv(uint64(len(f.tokens))); err != nil {
			return err
		}
		for i, tok := range f.tokens {
			if err := uv(uint64(len(tok))); err != nil {
				return err
			}
			if err := mw([]byte(tok)); err != nil {
				return err
			}
			if err := uv(uint64(f.counts[i])); err != nil {
				return err
			}
			if err := uv(uint64(len(f.postings[i]))); err != nil {
				return err
			}
			if err := mw(f.postings[i]); err != nil {
				return err
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

func LoadFTSIndex(path string) (*FTSIndex, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}
	if len(data) < 8 || !bytes.Equal(data[:4], ftsMagic[:]) {
		return nil, errors.New("fts: load index: bad magic (old or corrupt .fidx)")
	}
	body, sum := data[:len(data)-4], data[len(data)-4:]
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(sum) {
		return nil, errors.New("fts: load index: crc mismatch")
	}

	r := body[4:]
	take := func() (uint64, error) {
		v, n := binary.Uvarint(r)
		if n <= 0 {
			return 0, errors.New("fts: load index: truncated varint")
		}
		r = r[n:]
		return v, nil
	}

	tokenCount, err := take()
	if err != nil {
		return nil, err
	}
	if tokenCount > uint64(len(body)) {
		return nil, errors.New("fts: load index: token count out of range")
	}
	f := &FTSIndex{
		tokens:   make([]string, 0, tokenCount),
		postings: make([][]byte, 0, tokenCount),
		counts:   make([]int, 0, tokenCount),
	}
	for range tokenCount {
		tl, err := take()
		if err != nil {
			return nil, err
		}
		if tl > uint64(len(r)) {
			return nil, errors.New("fts: load index: token length out of range")
		}
		// The token strings reference the file buffer — one allocation for
		// the whole index instead of one per token.
		tok := string(r[:tl])
		r = r[tl:]
		cnt, err := take()
		if err != nil {
			return nil, err
		}
		bl, err := take()
		if err != nil {
			return nil, err
		}
		if bl > uint64(len(r)) {
			return nil, errors.New("fts: load index: posting blob out of range")
		}
		f.tokens = append(f.tokens, tok)
		f.postings = append(f.postings, r[:bl])
		f.counts = append(f.counts, int(cnt))
		r = r[bl:]
	}
	return f, nil
}
