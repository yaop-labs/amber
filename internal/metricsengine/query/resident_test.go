package query

import (
	"path/filepath"
	"sort"
	"testing"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/index"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
)

func residentTestBlock(t *testing.T) (string, block.Directory) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "resident.meb")
	series := []block.Series{
		{ID: 1, Type: model.MetricTypeCounter, Labels: model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "service", Value: "api"}, {Name: "route", Value: "/a"}}, Timestamps: []int64{0, 1000, 2000}, Values: []int64{10, 20, 30}},
		{ID: 2, Type: model.MetricTypeCounter, Labels: model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "service", Value: "web"}, {Name: "route", Value: "/b"}}, Timestamps: []int64{0, 1000, 2000}, Values: []int64{100, 200, 300}},
		{ID: 3, Type: model.MetricTypeCounter, Labels: model.LabelSet{{Name: "__name__", Value: "http_requests_total"}, {Name: "service", Value: "api"}, {Name: "route", Value: "/c"}}, Timestamps: []int64{0, 1000, 2000}, Values: []int64{5, 15, 25}},
		{ID: 4, Type: model.MetricTypeGauge, Labels: model.LabelSet{{Name: "__name__", Value: "other_metric"}, {Name: "service", Value: "api"}}, Timestamps: []int64{0, 1000}, Values: []int64{7, 7}},
	}
	if err := block.WriteFile(path, series); err != nil {
		t.Fatal(err)
	}
	dir, err := block.ReadDirectory(path)
	if err != nil {
		t.Fatal(err)
	}
	return path, dir
}

type scanRow struct {
	seriesID   uint64
	groupValue string
	values     []int64
}

func collectDirectory(t *testing.T, path string, dir block.Directory, selector index.Selector, opts Options, group string) []scanRow {
	t.Helper()
	var rows []scanRow
	err := ScanBlockWithDirectoryShared(path, dir, selector, opts, func(s block.DecodedSeries) error {
		gv, _ := s.Entry.Labels.Get(group)
		rows = append(rows, scanRow{seriesID: s.Entry.SeriesID, groupValue: gv, values: append([]int64(nil), s.Values...)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sortRows(rows)
	return rows
}

func collectResident(t *testing.T, path string, rb *block.ResidentBlock, selector index.Selector, opts Options, group string) []scanRow {
	t.Helper()
	var rows []scanRow
	err := ScanResidentBlock(path, rb, selector, opts, group, func(id uint64, _ model.MetricType, gv string, _, values []int64) error {
		rows = append(rows, scanRow{seriesID: id, groupValue: gv, values: append([]int64(nil), values...)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sortRows(rows)
	return rows
}

func sortRows(rows []scanRow) {
	sort.Slice(rows, func(i, j int) bool { return rows[i].seriesID < rows[j].seriesID })
}

func TestScanResidentBlockMatchesDirectory(t *testing.T) {
	path, dir := residentTestBlock(t)
	rb := block.BuildResidentFromDirectory(dir)

	cases := []struct {
		name     string
		selector index.Selector
	}{
		{"name-equal", index.NewSelector(index.MetricName("http_requests_total"))},
		{"service-equal", index.NewSelector(index.MetricName("http_requests_total"), index.LabelEqual("service", "api"))},
		{"service-regexp", index.NewSelector(index.MetricName("http_requests_total"), index.LabelRegexp("service", "ap.*"))},
		{"service-notequal", index.NewSelector(index.MetricName("http_requests_total"), index.LabelNotEqual("service", "api"))},
		{"route-notregexp", index.NewSelector(index.MetricName("http_requests_total"), index.LabelNotRegexp("route", "/a"))},
		{"absent-name-equal", index.NewSelector(index.LabelEqual("zone", "eu"))},
		{"absent-value-equal", index.NewSelector(index.LabelEqual("service", "ghost"))},
		{"negative-absent-name", index.NewSelector(index.MetricName("http_requests_total"), index.LabelNotEqual("zone", "eu"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			want := collectDirectory(t, path, dir, tc.selector, Options{}, "service")
			got := collectResident(t, path, rb, tc.selector, Options{}, "service")
			if len(want) != len(got) {
				t.Fatalf("row count: directory=%d resident=%d", len(want), len(got))
			}
			for i := range want {
				if want[i].seriesID != got[i].seriesID || want[i].groupValue != got[i].groupValue {
					t.Fatalf("row %d: directory=%+v resident=%+v", i, want[i], got[i])
				}
				if len(want[i].values) != len(got[i].values) {
					t.Fatalf("row %d value len: directory=%d resident=%d", i, len(want[i].values), len(got[i].values))
				}
				for j := range want[i].values {
					if want[i].values[j] != got[i].values[j] {
						t.Fatalf("row %d value %d: directory=%d resident=%d", i, j, want[i].values[j], got[i].values[j])
					}
				}
			}
		})
	}
}

func TestScanResidentBlockTimeFilter(t *testing.T) {
	path, dir := residentTestBlock(t)
	rb := block.BuildResidentFromDirectory(dir)
	selector := index.NewSelector(index.MetricName("http_requests_total"))
	start := int64(1500)
	opts := Options{StartMillis: &start}
	want := collectDirectory(t, path, dir, selector, opts, "service")
	got := collectResident(t, path, rb, selector, opts, "service")
	if len(want) != len(got) {
		t.Fatalf("row count: directory=%d resident=%d", len(want), len(got))
	}
}
