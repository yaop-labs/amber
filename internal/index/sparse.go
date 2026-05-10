package index

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

type SegmentTimeRange struct {
	SegmentID uint32
	FileName  string
	MinTS     int64
	MaxTS     int64
}

type SparseIndex struct {
	mu     sync.RWMutex
	ranges []SegmentTimeRange
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{}
}

func (s *SparseIndex) Add(r SegmentTimeRange) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.ranges {
		if s.ranges[i].SegmentID == r.SegmentID {
			s.ranges[i] = r
			return
		}
	}
	s.ranges = append(s.ranges, r)
}

func (s *SparseIndex) Touch(segmentID uint32, fileName string, ts int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.ranges {
		if s.ranges[i].SegmentID == segmentID {
			if ts < s.ranges[i].MinTS {
				s.ranges[i].MinTS = ts
			}
			if ts > s.ranges[i].MaxTS {
				s.ranges[i].MaxTS = ts
			}
			return
		}
	}
	s.ranges = append(s.ranges, SegmentTimeRange{
		SegmentID: segmentID,
		FileName:  fileName,
		MinTS:     ts,
		MaxTS:     ts,
	})
}

func (s *SparseIndex) TouchRange(segmentID uint32, fileName string, minTS, maxTS int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.ranges {
		if s.ranges[i].SegmentID == segmentID {
			if minTS < s.ranges[i].MinTS {
				s.ranges[i].MinTS = minTS
			}
			if maxTS > s.ranges[i].MaxTS {
				s.ranges[i].MaxTS = maxTS
			}
			return
		}
	}
	s.ranges = append(s.ranges, SegmentTimeRange{
		SegmentID: segmentID,
		FileName:  fileName,
		MinTS:     minTS,
		MaxTS:     maxTS,
	})
}

func (s *SparseIndex) Remove(segmentID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := range s.ranges {
		if s.ranges[i].SegmentID == segmentID {
			s.ranges = append(s.ranges[:i], s.ranges[i+1:]...)
			return
		}
	}
}

func (s *SparseIndex) Lookup(from, to int64) []SegmentTimeRange {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]SegmentTimeRange, 0)
	for _, r := range s.ranges {
		if r.MaxTS < from || r.MinTS > to {
			continue
		}
		result = append(result, r)
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].MinTS < result[j].MinTS
	})
	return result
}

func (s *SparseIndex) All() []SegmentTimeRange {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]SegmentTimeRange, len(s.ranges))
	copy(result, s.ranges)
	sort.Slice(result, func(i, j int) bool {
		return result[i].MinTS < result[j].MinTS
	})
	return result
}

func (s *SparseIndex) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.ranges)
}

const sparseIndexFile = "sparse.idx"

func (s *SparseIndex) Save(dir string) error {
	s.mu.RLock()
	data, err := json.MarshalIndent(s.ranges, "", "  ")
	s.mu.RUnlock()
	if err != nil {
		return fmt.Errorf("sparse: marshal: %w", err)
	}

	dst := dir + "/" + sparseIndexFile
	if err := atomicWriteFile(dst, data); err != nil {
		return fmt.Errorf("sparse: %w", err)
	}
	return nil
}

func LoadSparseIndex(dir string) (*SparseIndex, error) {
	path := dir + "/" + sparseIndexFile

	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return NewSparseIndex(), nil
	}
	if err != nil {
		return nil, fmt.Errorf("sparse: read: %w", err)
	}

	var ranges []SegmentTimeRange
	if err := json.Unmarshal(data, &ranges); err != nil {
		return nil, fmt.Errorf("sparse: parse: %w", err)
	}

	return &SparseIndex{ranges: ranges}, nil
}
