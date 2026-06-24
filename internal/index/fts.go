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
	"slices"
	"sort"

	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

// FTSIndex is a per-segment inverted index: a sorted token dictionary with
// delta-varint posting lists of uint64 entry IDs.
//
// It replaces the fts-engine radix snapshot, which stored every posting as a
// decimal-string DocID inside serialized tree nodes: ~38 MB on disk and
// ~150 MB in memory per 100k-record segment, ~400 ms to parse - measured in
// the first comparison run, where .fidx files were 92% of amber's storage
// and the dominant share of its RSS. This format holds the same data in a
// few MB, loads with a single linear pass, and searches by binary search
// over the dictionary.
//
// Tokenization (and therefore search semantics) is unchanged: the same
// multilingual pipeline processes bodies at index time and queries at search
// time; multi-token queries AND their posting lists.
type FTSIndex struct {
	// Build-mode state, nil after seal or load. Unique tokens are
	// deduplicated through a hash -> entry-index map with byte-verified
	// collision chains; token bytes live in one shared arena. The previous
	// map[string]*postingBuilder cost four allocations per unique token
	// (map key string, builder, its ids slice, map growth) - ~1M tokens per
	// 100k-record segment made the build state the dominant transient heap
	// during seals. byHash has pointer-free keys and values, so the GC never
	// scans it.
	arena   []byte
	entries []ftsBuildEntry
	byHash  map[uint64]int32

	// sealed dictionary: tokens with df >= 2 (template words; tens of
	// thousands per segment).
	tokens   []string // sorted; slices into buf
	postings [][]byte // delta-varint blobs, parallel to tokens
	counts   []int

	// sealed unique-token section. UUID-bearing bodies make ~80% of a
	// segment's tokens df==1; keeping them in the string dictionary cost
	// ~40 bytes of string/slice headers per token (~50 MB of headers per
	// 100k-record segment in the LRU). Here they are two flat byte arrays
	// searched in place: fnv64a(token) sorted ascending and the parallel
	// full entry ID, 8 bytes each. Colliding hashes stay adjacent and
	// lookup returns all of them, so a 64-bit collision within a segment
	// (~1M tokens -> P ~ 3e-8) can only surface one foreign record in a
	// result, never lose one; accepted deliberately - the result-count
	// equality gate in benchmarks would flag it.
	uniqHashes []byte // 8-byte big-endian hashes, sorted
	uniqIDs    []byte // 4-byte big-endian ORDINALS (index into table), parallel

	// Ordinal table (AFT3): the sorted distinct entry IDs of the segment.
	// Postings and the unique section store a token's records as ordinals
	// (positions in this table) instead of full 8-byte ULID-derived IDs:
	// ordinals are dense 0..N-1, so delta-varint posting gaps shrink to ~1
	// byte and a unique-token record fits in 4 bytes instead of 8. Lookups
	// intersect in ordinal space (order-preserving, the table is sorted by
	// ID) and map only the final <=limit results back to real IDs, so the
	// table is read on demand (pread when file-backed) and never inflates
	// resident memory - amber trades disk for nothing on RSS here.
	table []uint64 // resident only between seal and Save (then file-backed)

	// Loaded (file-backed) mode: the unique section and ordinal table are NOT
	// resident - they are read with pread on demand. Explicit reads, not mmap,
	// on purpose: page faults would make tail latency depend on page-cache
	// state and turn storage errors into SIGBUS process kills ("Are You Sure
	// You Want to Use MMAP in Your DBMS?", CIDR 2022), while pread returns
	// errors as values and still benefits from the OS page cache.
	path       string
	uniqOff    int64
	uniqCount  int
	tableOff   int64
	tableCount int
}

func tokenHash(tok string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(tok); i++ {
		h ^= uint64(tok[i])
		h *= 1099511628211
	}
	return h
}

// ftsBuildEntry is one unique token's build state. The first entry ID is
// inlined: ~80% of tokens are df==1 (UUID stems) and never allocate a rest
// slice. next chains entries whose token hashes collide; -1 ends the chain.
type ftsBuildEntry struct {
	firstID  uint64
	rest     []uint64
	tokOff   uint32
	tokLen   uint32
	next     int32
	unsorted bool
}

var ftsPipeline = textproc.DefaultMultilingualPipeline()

// TokenizeFTS splits and stems text into searchable tokens, the pipeline
// applied identically at index time and query time.
func TokenizeFTS(text string) []string {
	return ftsPipeline.Process(text)
}

// NewFTSIndex returns an empty index in build mode, ready for Add.
func NewFTSIndex() *FTSIndex {
	return &FTSIndex{byHash: make(map[uint64]int32)}
}

func (f *FTSIndex) tokenBytes(i int32) []byte {
	e := &f.entries[i]
	return f.arena[e.tokOff : e.tokOff+e.tokLen]
}

// Index adds one record's body. Entry IDs normally arrive in ascending order
// (segment scan order); out-of-order IDs are tolerated and sorted at Save.
func (f *FTSIndex) Index(_ context.Context, entryID uint64, body string) error {
	if f.byHash == nil {
		return errors.New("fts: index is sealed")
	}
	for _, tok := range TokenizeFTS(body) {
		if tok == "" {
			continue
		}
		h := tokenHash(tok)
		ei := int32(-1)
		head, ok := f.byHash[h]
		if ok {
			for i := head; i >= 0; i = f.entries[i].next {
				if string(f.tokenBytes(i)) == tok {
					ei = i
					break
				}
			}
		}
		if ei < 0 {
			off := uint32(len(f.arena))
			f.arena = append(f.arena, tok...)
			next := int32(-1)
			if ok {
				next = head
			}
			f.entries = append(f.entries, ftsBuildEntry{
				firstID: entryID,
				tokOff:  off,
				tokLen:  uint32(len(tok)),
				next:    next,
			})
			f.byHash[h] = int32(len(f.entries) - 1)
			continue
		}
		e := &f.entries[ei]
		last := e.firstID
		if n := len(e.rest); n > 0 {
			last = e.rest[n-1]
		}
		if last == entryID {
			continue // repeated token within one body
		}
		if entryID < last {
			e.unsorted = true
		}
		e.rest = append(e.rest, entryID)
	}
	return nil
}

// TokenKeys returns one key per unique token, for the FTS ribbon filter.
// Build-mode only (empty after seal or load); keys alias the token arena
// and stay valid as long as the caller holds them, even across seal.
func (f *FTSIndex) TokenKeys() [][]byte {
	keys := make([][]byte, len(f.entries))
	for i := range f.entries {
		keys[i] = f.tokenBytes(int32(i))
	}
	return keys
}

// seal converts build-mode state into the flat sorted form: df>=2 tokens go
// to the dictionary, df==1 tokens to the unique hash section.
func (f *FTSIndex) seal() {
	if f.byHash == nil {
		return
	}

	type uniqPair struct{ hash, id uint64 }
	var uniqs []uniqPair
	var dict []int32
	for h, head := range f.byHash {
		for i := head; i >= 0; i = f.entries[i].next {
			e := &f.entries[i]
			if e.rest == nil {
				uniqs = append(uniqs, uniqPair{h, e.firstID})
				continue
			}
			ids := make([]uint64, 0, len(e.rest)+1)
			ids = append(ids, e.firstID)
			ids = append(ids, e.rest...)
			if e.unsorted {
				slices.Sort(ids)
				ids = dedupSorted(ids)
			}
			if len(ids) == 1 {
				uniqs = append(uniqs, uniqPair{h, ids[0]})
				continue
			}
			e.rest = ids
			dict = append(dict, i)
		}
	}
	slices.SortFunc(dict, func(a, b int32) int {
		return bytes.Compare(f.tokenBytes(a), f.tokenBytes(b))
	})

	// Ordinal table: every distinct entry ID that appears in the index, sorted.
	// A token's records are then stored as ordinals (positions in this table)
	// rather than full 8-byte IDs.
	var allIDs []uint64
	for _, i := range dict {
		allIDs = append(allIDs, f.entries[i].rest...)
	}
	for _, u := range uniqs {
		allIDs = append(allIDs, u.id)
	}
	slices.Sort(allIDs)
	f.table = dedupSorted(allIDs)
	ordOf := func(id uint64) uint64 {
		return uint64(sort.Search(len(f.table), func(k int) bool { return f.table[k] >= id }))
	}

	f.tokens = make([]string, len(dict))
	f.postings = make([][]byte, len(dict))
	f.counts = make([]int, len(dict))
	for di, i := range dict {
		e := &f.entries[i]
		f.tokens[di] = string(f.tokenBytes(i))
		blob := make([]byte, 0, len(e.rest)*2)
		var prev uint64
		for _, id := range e.rest {
			ord := ordOf(id)
			blob = binary.AppendUvarint(blob, ord-prev)
			prev = ord
		}
		f.postings[di] = blob
		f.counts[di] = len(e.rest)
	}

	sort.Slice(uniqs, func(a, b int) bool { return uniqs[a].hash < uniqs[b].hash })
	f.uniqHashes = make([]byte, len(uniqs)*8)
	f.uniqIDs = make([]byte, len(uniqs)*4)
	for i, u := range uniqs {
		binary.BigEndian.PutUint64(f.uniqHashes[i*8:], u.hash)
		binary.BigEndian.PutUint32(f.uniqIDs[i*4:], uint32(ordOf(u.id)))
	}
	f.arena, f.entries, f.byHash = nil, nil, nil
}

// lookupUnique returns the ORDINALS of df==1 tokens matching the hash (adjacent
// duplicates included, so hash collisions can't drop records). The in-memory
// arrays serve freshly built indexes; loaded indexes search the file with pread
// instead of keeping the unique section resident. Hashes are 8 bytes, ordinals
// 4 bytes; Search maps the final results back to entry IDs.
func (f *FTSIndex) lookupUnique(token string) []uint64 {
	if f.uniqHashes != nil {
		n := len(f.uniqHashes) / 8
		if n == 0 {
			return nil
		}
		h := tokenHash(token)
		i := sort.Search(n, func(i int) bool {
			return binary.BigEndian.Uint64(f.uniqHashes[i*8:]) >= h
		})
		var ords []uint64
		for ; i < n && binary.BigEndian.Uint64(f.uniqHashes[i*8:]) == h; i++ {
			ords = append(ords, uint64(binary.BigEndian.Uint32(f.uniqIDs[i*4:])))
		}
		if len(ords) > 1 {
			slices.Sort(ords)
		}
		return ords
	}
	if f.uniqCount == 0 || f.path == "" {
		return nil
	}
	file, err := os.Open(f.path)
	if err != nil {
		return nil
	}
	defer file.Close()

	h := tokenHash(token)
	var buf8 [8]byte
	var buf4 [4]byte
	readHash := func(off int64) (uint64, bool) {
		if _, err := file.ReadAt(buf8[:], off); err != nil {
			return 0, false
		}
		return binary.BigEndian.Uint64(buf8[:]), true
	}
	readOrd := func(off int64) (uint64, bool) {
		if _, err := file.ReadAt(buf4[:], off); err != nil {
			return 0, false
		}
		return uint64(binary.BigEndian.Uint32(buf4[:])), true
	}

	n := f.uniqCount
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		v, ok := readHash(f.uniqOff + int64(mid)*8)
		if !ok {
			return nil
		}
		if v < h {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	ordsOff := f.uniqOff + int64(n)*8
	var ords []uint64
	for i := lo; i < n; i++ {
		v, ok := readHash(f.uniqOff + int64(i)*8)
		if !ok || v != h {
			break
		}
		o, ok := readOrd(ordsOff + int64(i)*4)
		if !ok {
			break
		}
		ords = append(ords, o)
	}
	if len(ords) > 1 {
		slices.Sort(ords)
	}
	return ords
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

	// Intersect in ordinal space (order-preserving - the table is sorted by ID),
	// then map only the final <=limit results back to entry IDs.
	var acc []uint64
	for i, tok := range tokens {
		ords := f.lookup(tok)
		if len(ords) == 0 {
			ords = f.lookupUnique(tok)
		}
		if len(ords) == 0 {
			return nil, nil
		}
		if i == 0 {
			acc = ords
			continue
		}
		acc = intersectSorted(acc, ords)
		if len(acc) == 0 {
			return nil, nil
		}
	}
	if limit > 0 && len(acc) > limit {
		acc = acc[:limit]
	}
	return f.mapOrdinalsToIDs(acc)
}

// lookup returns the df>=2 token's records as ascending ORDINALS (positions in
// the table); Search maps the final intersection back to entry IDs.
func (f *FTSIndex) lookup(token string) []uint64 {
	i := sort.SearchStrings(f.tokens, token)
	if i >= len(f.tokens) || f.tokens[i] != token {
		return nil
	}
	ords := make([]uint64, 0, f.counts[i])
	var prev uint64
	blob := f.postings[i]
	for len(blob) > 0 {
		d, n := binary.Uvarint(blob)
		if n <= 0 {
			return ords
		}
		prev += d
		ords = append(ords, prev)
		blob = blob[n:]
	}
	return ords
}

// mapOrdinalsToIDs resolves ordinals to entry IDs via the table - resident for a
// freshly sealed index, pread on demand once file-backed (so the table never
// inflates resident memory). Only the final <=limit results are mapped.
func (f *FTSIndex) mapOrdinalsToIDs(ords []uint64) ([]uint64, error) {
	out := make([]uint64, len(ords))
	if f.table != nil {
		for i, o := range ords {
			if o >= uint64(len(f.table)) {
				return nil, errors.New("fts: ordinal out of range")
			}
			out[i] = f.table[o]
		}
		return out, nil
	}
	if f.path == "" || f.tableCount == 0 {
		return nil, errors.New("fts: ordinal table unavailable")
	}
	if len(ords) == 0 {
		return out, nil
	}
	// The result ordinals span a contiguous slice of the table; read that range
	// once instead of one pread per result. A single sequential read beats N
	// random reads even when the matches are spread across the segment, and the
	// buffer is transient so resident memory stays flat.
	minOrd, maxOrd := ords[0], ords[0]
	for _, o := range ords {
		if o < minOrd {
			minOrd = o
		}
		if o > maxOrd {
			maxOrd = o
		}
	}
	if int(maxOrd) >= f.tableCount {
		return nil, errors.New("fts: ordinal out of range")
	}
	file, err := os.Open(f.path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	// One ranged read when the results are clustered (the common case - a query
	// returns the lowest ordinals of a posting, so the span is tight). If the
	// results are sparse across the table (e.g. after an intersection), reading
	// the whole span would pull far more than needed, so fall back to a pread per
	// result - never worse than the per-result baseline either way.
	span := int(maxOrd-minOrd) + 1
	if span <= 8*len(ords) {
		buf := make([]byte, span*8)
		if _, err := file.ReadAt(buf, f.tableOff+int64(minOrd)*8); err != nil {
			return nil, err
		}
		for i, o := range ords {
			out[i] = binary.BigEndian.Uint64(buf[int(o-minOrd)*8:])
		}
		return out, nil
	}
	var word [8]byte
	for i, o := range ords {
		if _, err := file.ReadAt(word[:], f.tableOff+int64(o)*8); err != nil {
			return nil, err
		}
		out[i] = binary.BigEndian.Uint64(word[:])
	}
	return out, nil
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

// On-disk format ("AFT3"):
//
//	magic [4] | uvarint dictTokenCount
//	per dict token, sorted: uvarint len | bytes | uvarint postingCount |
//	                        uvarint blobLen | delta-varint ORDINALS
//	uvarint uniqCount | uniqCountx8B BE sorted hashes | uniqCountx4B BE ordinals
//	uvarint tableCount | tableCountx8B BE sorted entry IDs (ordinal -> id)
//	crc32 IEEE over everything above [4, little-endian]
//
// AFT3 replaces AFT2's full 8-byte entry IDs with ordinals (positions in the
// table) in both postings and the unique section; an AFT2 file fails the magic
// check and falls back to a scan.
var ftsMagic = [4]byte{'A', 'F', 'T', '3'}

// Save seals the build state and writes the index to path in the AFT3 format,
// then demotes the index to file-backed mode so the resident unique-token
// arrays and ordinal table can be released.
func (f *FTSIndex) Save(path string) error {
	f.seal()
	written := 0
	err := atomicWrite(path, func(file *os.File) error {
		crc := crc32.NewIEEE()
		w := bufio.NewWriterSize(file, 1<<20)
		mw := func(b []byte) error {
			crc.Write(b)
			written += len(b)
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
		if err := uv(uint64(len(f.uniqHashes) / 8)); err != nil {
			return err
		}
		uniqOff := written
		if err := mw(f.uniqHashes); err != nil {
			return err
		}
		if err := mw(f.uniqIDs); err != nil {
			return err
		}
		if err := uv(uint64(len(f.table))); err != nil {
			return err
		}
		tableOff := written
		var tbuf [8]byte
		for _, id := range f.table {
			binary.BigEndian.PutUint64(tbuf[:], id)
			if err := mw(tbuf[:]); err != nil {
				return err
			}
		}
		var sum [4]byte
		binary.LittleEndian.PutUint32(sum[:], crc.Sum32())
		if _, err := w.Write(sum[:]); err != nil {
			return err
		}
		f.uniqOff = int64(uniqOff)
		f.tableOff = int64(tableOff)
		return w.Flush()
	})
	if err != nil {
		return err
	}
	// Demote to file-backed: the unique section and ordinal table now live on
	// disk, so a freshly sealed index registered in the executor cache costs the
	// same few MB as a loaded one instead of pinning the hash arrays and table.
	f.path = path
	f.uniqCount = len(f.uniqHashes) / 8
	f.tableCount = len(f.table)
	f.uniqHashes = nil
	f.uniqIDs = nil
	f.table = nil
	return nil
}

// LoadFTSIndex opens an AFT3 index at path in file-backed mode: the dictionary
// is resident but the df==1 unique section and the ordinal table are read on
// disk with pread.
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
		// Clone dictionary bytes out of the file buffer: retaining
		// sub-slices would pin the whole file - including the unique
		// section, which deliberately stays on disk.
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
		f.postings = append(f.postings, bytes.Clone(r[:bl]))
		f.counts = append(f.counts, int(cnt))
		r = r[bl:]
	}

	uniqCount, err := take()
	if err != nil {
		return nil, err
	}
	// Unique section: uniqCount x (8B hash + 4B ordinal). Searched via pread;
	// record its position only.
	if uniqCount*12 > uint64(len(r)) {
		return nil, errors.New("fts: load index: unique section out of range")
	}
	f.path = path
	f.uniqCount = int(uniqCount)
	f.uniqOff = int64(len(data) - 4 - len(r))
	r = r[uniqCount*12:]

	tableCount, err := take()
	if err != nil {
		return nil, err
	}
	if tableCount*8 > uint64(len(r)) {
		return nil, errors.New("fts: load index: ordinal table out of range")
	}
	// The ordinal table is read via pread; record its position only.
	f.tableCount = int(tableCount)
	f.tableOff = int64(len(data) - 4 - len(r))
	return f, nil
}
