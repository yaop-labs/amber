package index

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yaop-labs/amber/internal/storage"
)

func TestSealBuildersRejectUndecodableLogicalRecords(t *testing.T) {
	tests := []struct {
		name  string
		build func(string) error
		want  string
	}{
		{
			name: "log",
			build: func(path string) error {
				_, err := BuildLogSealIndexes(path, nil)
				return err
			},
			want: "decode log record",
		},
		{
			name: "span",
			build: func(path string) error {
				_, err := BuildSpanSealIndexes(path, nil)
				return err
			},
			want: "decode span record",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "segment")
			writer, err := storage.OpenSegmentWriter(path)
			if err != nil {
				t.Fatalf("OpenSegmentWriter: %v", err)
			}
			if err := writer.WriteRecord([]byte{1}, 123456789); err != nil {
				t.Fatalf("WriteRecord: %v", err)
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			err = tt.build(path)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error = %v, want one containing %q", err, tt.want)
			}
			for _, ext := range storage.SegmentSidecarExts[1:] {
				if _, statErr := os.Stat(path + ext); !os.IsNotExist(statErr) {
					t.Errorf("partial sidecar %s exists after failed build (stat err %v)", ext, statErr)
				}
			}
		})
	}
}
