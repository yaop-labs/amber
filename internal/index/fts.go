package index

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	"github.com/dariasmyr/fts-engine/pkg/fts"
	"github.com/dariasmyr/fts-engine/pkg/index/slicedradix"
	"github.com/dariasmyr/fts-engine/pkg/keygen"
	"github.com/dariasmyr/fts-engine/pkg/textproc"
)

func init() {
	_ = fts.RegisterIndexSnapshotCodec("slicedradix",
		func(index fts.Index, w io.Writer) error {
			s, ok := index.(fts.Serializable)
			if !ok {
				return fmt.Errorf("slicedradix: index does not implement Serializable")
			}
			return s.Serialize(w)
		},
		slicedradix.Load,
	)
}

var ftsPipeline = textproc.DefaultMultilingualPipeline()

func TokenizeFTS(text string) []string {
	return ftsPipeline.Process(text)
}

type FTSIndex struct {
	svc *fts.Service
}

func NewFTSIndex() *FTSIndex {
	svc := fts.New(
		slicedradix.New(),
		keygen.Word,
		fts.WithPipeline(textproc.DefaultMultilingualPipeline()),
	)
	return &FTSIndex{svc: svc}
}

func (f *FTSIndex) Index(ctx context.Context, entryID uint64, body string) error {
	docID := fts.DocID(strconv.FormatUint(entryID, 10))
	if err := f.svc.IndexDocument(ctx, docID, body); err != nil {
		return fmt.Errorf("fts: index: %w", err)
	}
	return nil
}

func (f *FTSIndex) Search(ctx context.Context, query string, limit int) ([]uint64, error) {
	results, err := f.svc.SearchDocuments(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("fts: search: %w", err)
	}

	ids := make([]uint64, 0, len(results.Results))
	for _, r := range results.Results {
		id, err := strconv.ParseUint(string(r.ID), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("fts: search: parse id %q: %w", r.ID, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (f *FTSIndex) Save(path string) error {
	return atomicWrite(path, func(file *os.File) error {
		if err := f.svc.SaveSnapshot(file, "slicedradix", ""); err != nil {
			return fmt.Errorf("fts: save: %w", err)
		}
		return nil
	})
}

func LoadFTSIndex(path string) (*FTSIndex, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}
	defer file.Close()
	svc, err := fts.NewFromSnapshot(file, keygen.Word,
		fts.WithPipeline(textproc.DefaultMultilingualPipeline()),
	)
	if err != nil {
		return nil, fmt.Errorf("fts: load index: %w", err)
	}
	return &FTSIndex{svc: svc}, nil
}
