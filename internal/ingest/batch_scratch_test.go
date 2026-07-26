package ingest

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/model"
)

func TestPrepareLogBatchUsesContiguousScratchPayload(t *testing.T) {
	b := NewBatcher(Deps{Logger: discardLogger()}, Config{})
	entries := []model.LogEntry{
		{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Unix(0, 100),
			Level:     model.LevelInfo,
			Service:   "api",
			Body:      "first",
			Attrs:     []model.Attr{{Key: "env", Value: "test"}},
		},
		{
			ID:        model.MustNewEntryID(),
			Timestamp: time.Unix(0, 200),
			Level:     model.LevelWarn,
			Service:   "worker",
			Body:      "second",
			Attrs:     []model.Attr{{Key: "zone", Value: "a"}},
		},
	}
	batch := []item{{log: &entries[0]}, {log: &entries[1]}}

	var scratch logBatchScratch
	items, indexEntries, replayEntries, err := b.prepareLogBatch(batch, &scratch)
	if err != nil {
		t.Fatalf("prepareLogBatch: %v", err)
	}
	defer scratch.finish()

	if len(items) != len(entries) {
		t.Fatalf("items = %d, want %d", len(items), len(entries))
	}
	if indexEntries != nil {
		t.Fatalf("index entries = %d, want disabled", len(indexEntries))
	}
	if replayEntries != nil {
		t.Fatalf("replay entries = %d, want disabled", len(replayEntries))
	}

	joined := append(append([]byte(nil), items[0].Data...), items[1].Data...)
	if !bytes.Equal(joined, scratch.payload) {
		t.Fatal("batch items do not partition the scratch payload")
	}
	original := scratch.payload[0]
	scratch.payload[0] ^= 0xff
	if items[0].Data[0] != scratch.payload[0] {
		t.Fatal("batch item doesn't reference the scratch payload")
	}
	scratch.payload[0] = original

	for i := range entries {
		var got model.LogEntry
		if err := got.DecodeBytes(items[i].Data); err != nil {
			t.Fatalf("DecodeBytes[%d]: %v", i, err)
		}
		if got.ID != entries[i].ID ||
			!got.Timestamp.Equal(entries[i].Timestamp) ||
			got.Level != entries[i].Level ||
			got.Service != entries[i].Service ||
			got.Host != entries[i].Host ||
			got.Body != entries[i].Body ||
			!reflect.DeepEqual(got.Attrs, entries[i].Attrs) {
			t.Fatalf("decoded entry[%d] differs:\ngot  %+v\nwant %+v", i, got, entries[i])
		}
	}
}

func TestBatchScratchDropsOversizedRetainedMemory(t *testing.T) {
	logScratch := logBatchScratch{
		payload: make([]byte, 1, maxRetainedBatchPayload+1),
	}
	logScratch.finish()
	if logScratch.payload != nil {
		t.Fatalf("oversized payload cap = %d, want dropped", cap(logScratch.payload))
	}

	spanScratch := spanBatchScratch{
		payload: make([]byte, 1, maxRetainedBatchPayload+1),
	}
	spanScratch.finish()
	if spanScratch.payload != nil {
		t.Fatalf("oversized span payload cap = %d, want dropped", cap(spanScratch.payload))
	}
}
