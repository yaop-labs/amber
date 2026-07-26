package indexer

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/model"
	"github.com/yaop-labs/amber/internal/storage"
)

func TestIDGroupScratchReusesBuffersAndClearsKeys(t *testing.T) {
	var groups idGroupScratch
	idx := index.NewMultiFieldIndex()

	groups.add("service-a", 2)
	groups.add("service-a", 1)
	groups.flush(idx, "service")

	if len(groups.slots) != 0 || groups.used != 0 {
		t.Fatalf("scratch not reset: slots=%d used=%d", len(groups.slots), groups.used)
	}
	if len(groups.ids) != 1 || cap(groups.ids[0]) == 0 {
		t.Fatalf("ID buffer not retained: %#v", groups.ids)
	}
	backing := &groups.ids[0][:1][0]

	groups.add("service-b", 3)
	if got := &groups.ids[0][0]; got != backing {
		t.Fatal("ID buffer was not reused for the next key")
	}
	groups.flush(idx, "service")

	if got := idx.Filter(map[string]string{"service": "service-a"}); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("service-a IDs = %v, want [1 2]", got)
	}
	if got := idx.Filter(map[string]string{"service": "service-b"}); len(got) != 1 || got[0] != 3 {
		t.Fatalf("service-b IDs = %v, want [3]", got)
	}
}

func TestIDGroupScratchDropsHighCardinalityState(t *testing.T) {
	var groups idGroupScratch
	idx := index.NewMultiFieldIndex()

	for i := 0; i <= maxRetainedIndexGroups; i++ {
		groups.add(fmt.Sprintf("service-%d", i), uint64(i))
	}
	groups.flush(idx, "service")

	if groups.slots != nil || groups.ids != nil || groups.used != 0 {
		t.Fatalf("high-cardinality scratch retained state: slots=%d ids=%d used=%d", len(groups.slots), len(groups.ids), groups.used)
	}
}

func TestIDGroupScratchDropsOversizedIDBuffer(t *testing.T) {
	var groups idGroupScratch
	idx := index.NewMultiFieldIndex()

	for i := 0; i <= maxRetainedGroupIDs; i++ {
		groups.add("service-a", uint64(i))
	}
	groups.flush(idx, "service")

	if len(groups.ids) != 1 || groups.ids[0] != nil {
		t.Fatalf("oversized ID buffer retained with capacity %d", cap(groups.ids[0]))
	}
}

func TestActiveIndexBatchGroupingConcurrent(t *testing.T) {
	logManager, err := storage.OpenSegmentManager(t.TempDir()+"/logs", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer logManager.Close()
	spanManager, err := storage.OpenSegmentManager(t.TempDir()+"/spans", storage.DefaultRotationPolicy)
	if err != nil {
		t.Fatal(err)
	}
	defer spanManager.Close()
	now := time.Now().UnixNano()
	if err := logManager.WriteBatch([]storage.BatchItem{{Data: []byte("seed"), TS: now}}); err != nil {
		t.Fatal(err)
	}
	if err := spanManager.WriteBatch([]storage.BatchItem{{Data: []byte("seed"), TS: now}}); err != nil {
		t.Fatal(err)
	}

	active := New(logManager, spanManager)
	const (
		workers = 8
		perCall = 32
	)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		entries := make([]*model.LogEntry, perCall)
		for i := range entries {
			var id model.EntryID
			binary.BigEndian.PutUint64(id[2:10], uint64(worker*perCall+i+1))
			entries[i] = &model.LogEntry{ID: id, Level: model.LevelInfo, Service: "api"}
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			active.IndexLogEntries(entries)
		}()
	}
	wg.Wait()

	meta, ok := logManager.ActiveSegmentMeta()
	if !ok {
		t.Fatal("active log segment missing")
	}
	idx, ok := active.LookupLog(meta.FileName)
	if !ok {
		t.Fatal("active log index missing")
	}
	got := idx.Filter(map[string]string{"service": "api"})
	if len(got) != workers*perCall {
		t.Fatalf("indexed IDs = %d, want %d", len(got), workers*perCall)
	}
}
