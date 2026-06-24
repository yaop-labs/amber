package index

import (
	"os"

	"github.com/dariasmyr/fts-engine/pkg/filter"
)

// RibbonFilter is an approximate set-membership filter over a segment's keys
// (e.g. service names, FTS tokens), used to skip segments that cannot match a
// query before any scan. It is best-effort: a build failure degrades to a scan.
type RibbonFilter struct {
	inner *filter.RibbonFilter
}

// BuildRibbonFilter builds a filter over keys. An empty key set yields an
// empty filter that matches nothing. The second parameter is unused (the
// window width is fixed; see the comment within).
func BuildRibbonFilter(keys [][]byte, _ uint8) (*RibbonFilter, error) {
	if len(keys) == 0 {
		return &RibbonFilter{}, nil
	}
	n := uint32(len(keys))
	// Window width must grow with log(n) for the ribbon solve to converge;
	// empirically (param sweep on UUID-like keys): w=16 starts failing at
	// ~10k keys, w=24 at ~200k, while w=32 (the library max) with n/4 slack
	// builds reliably up to at least 2M keys - which is the realistic FTS
	// token cardinality of one 100k-record segment with UUID-bearing bodies.
	// The wider window costs 16 extra bytes per lookup and w extra cells.
	rf, err := filter.NewRibbonFilter(n, n/4, 32, 0)
	if err != nil {
		return nil, err
	}
	err = rf.BuildWithRetriesFromKeyStream(func(emit func([]byte) bool) error {
		for _, k := range keys {
			if !emit(k) {
				break
			}
		}
		return nil
	}, 32)
	if err != nil {
		return nil, err
	}
	return &RibbonFilter{inner: rf}, nil
}

// Contains reports whether key may be present. False is definitive (the key is
// absent); true may be a false positive, so callers still verify by scanning.
func (f *RibbonFilter) Contains(key []byte) bool {
	if f.inner == nil {
		return false
	}
	return f.inner.Contains(key)
}

// Save writes the filter to path atomically.
func (f *RibbonFilter) Save(path string) error {
	if f.inner == nil {
		return atomicWriteFile(path, nil)
	}
	return atomicWrite(path, func(file *os.File) error {
		return f.inner.Serialize(file)
	})
}

// LoadRibbonFilter reads a filter previously written by Save.
func LoadRibbonFilter(path string) (*RibbonFilter, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	rf, err := filter.LoadRibbonFilter(file)
	if err != nil {
		return nil, err
	}
	return &RibbonFilter{inner: rf}, nil
}
