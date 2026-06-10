package storage

import (
	"testing"
	"time"
)

func TestS3StoreKeyNormalizesPrefix(t *testing.T) {
	store := &S3Store{cfg: S3StoreConfig{Prefix: "amber/logs"}}
	if got := store.key("seg_00000001.alog"); got != "amber/logs/seg_00000001.alog" {
		t.Fatalf("key = %q, want amber/logs/seg_00000001.alog", got)
	}
	store.cfg.Prefix = ""
	if got := store.key("seg_00000001.alog"); got != "seg_00000001.alog" {
		t.Fatalf("key without prefix = %q, want seg_00000001.alog", got)
	}
	store.cfg.Prefix = "/spans/"
	if got := store.key("seg_00000001.alog"); got != "spans/seg_00000001.alog" {
		t.Fatalf("key with slashy prefix = %q, want spans/seg_00000001.alog", got)
	}
}

func TestS3StoreOperationContextHasTimeout(t *testing.T) {
	store := &S3Store{cfg: S3StoreConfig{OperationTimeout: time.Second}}
	ctx, cancel := store.operationContext()
	defer cancel()
	if _, ok := ctx.Deadline(); !ok {
		t.Fatal("operation context has no deadline")
	}
}
