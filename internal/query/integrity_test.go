package query

import (
	"context"
	"strings"
	"testing"

	"github.com/yaop-labs/amber/internal/index"
)

func TestExecutorRejectsUndecodableLogicalRecords(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Executor) error
		span bool
		want string
	}{
		{
			name: "log",
			run: func(exec *Executor) error {
				_, err := exec.ExecLog(context.Background(), &LogQuery{Limit: 10})
				return err
			},
			want: "decode log record",
		},
		{
			name: "span",
			run: func(exec *Executor) error {
				_, err := exec.ExecSpan(context.Background(), &SpanQuery{Limit: 10})
				return err
			},
			span: true,
			want: "decode span record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, _, _ := setupTestStore(t)
			const ts = int64(123456789)
			if err := mgr.Write([]byte{1}, ts); err != nil {
				t.Fatalf("write malformed logical record: %v", err)
			}
			meta, ok := mgr.ActiveSegmentMeta()
			if !ok {
				t.Fatal("active segment missing")
			}
			if err := mgr.Rotate(); err != nil {
				t.Fatalf("seal malformed logical record: %v", err)
			}
			sparse := index.NewSparseIndex()
			sparse.Add(index.SegmentTimeRange{
				SegmentID: meta.ID,
				FileName:  meta.FileName,
				MinTS:     ts,
				MaxTS:     ts,
			})

			logSparse, spanSparse := sparse, index.NewSparseIndex()
			if tt.span {
				logSparse, spanSparse = spanSparse, logSparse
			}
			exec := NewExecutor(mgr, mgr, logSparse, spanSparse)
			err := tt.run(exec)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one containing %q", err, tt.want)
			}
		})
	}
}
