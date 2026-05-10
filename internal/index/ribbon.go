package index

import (
	"os"

	"github.com/dariasmyr/fts-engine/pkg/filter"
)

type RibbonFilter struct {
	inner *filter.RibbonFilter
}

func BuildRibbonFilter(keys [][]byte, _ uint8) (*RibbonFilter, error) {
	if len(keys) == 0 {
		return &RibbonFilter{}, nil
	}
	n := uint32(len(keys))
	rf, err := filter.NewRibbonFilter(n, n/4, 16, 0)
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

func (f *RibbonFilter) Contains(key []byte) bool {
	if f.inner == nil {
		return false
	}
	return f.inner.Contains(key)
}

func (f *RibbonFilter) Save(path string) error {
	if f.inner == nil {
		return atomicWriteFile(path, nil)
	}
	return atomicWrite(path, func(file *os.File) error {
		return f.inner.Serialize(file)
	})
}

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
