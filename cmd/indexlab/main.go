// Command indexlab runs the reproducible static interval-index experiment.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/yaop-labs/amber/internal/index"
	"github.com/yaop-labs/amber/internal/index/indexlab"
)

const (
	datasetSeed = uint64(42)
	querySeed   = uint64(84)
)

type config struct {
	Sizes            []int  `json:"sizes"`
	MaxQueries       int    `json:"max_queries"`
	LinearScanBudget int64  `json:"linear_scan_budget"`
	Repeats          int    `json:"repeats"`
	BuildRepeats     int    `json:"build_repeats"`
	PointsPerModel   int    `json:"points_per_model"`
	DatasetSeed      uint64 `json:"dataset_seed"`
	QuerySeed        uint64 `json:"query_seed"`
}

type report struct {
	GeneratedAt string       `json:"generated_at"`
	GoVersion   string       `json:"go_version"`
	GOOS        string       `json:"goos"`
	GOARCH      string       `json:"goarch"`
	CPUs        int          `json:"cpus"`
	TimerP50NS  int64        `json:"timer_overhead_p50_ns"`
	TimerP99NS  int64        `json:"timer_overhead_p99_ns"`
	Config      config       `json:"config"`
	Results     []caseResult `json:"results"`
}

type caseResult struct {
	Dataset          string  `json:"dataset"`
	Segments         int     `json:"segments"`
	Algorithm        string  `json:"algorithm"`
	QueriesPerRepeat int     `json:"queries_per_repeat"`
	Samples          int     `json:"samples"`
	BuildNS          int64   `json:"build_ns"`
	BaseBytes        int64   `json:"base_bytes"`
	AuxBytes         int64   `json:"aux_bytes"`
	AuxBytesPerSeg   float64 `json:"aux_bytes_per_segment"`
	ModelCount       int     `json:"model_count"`
	LookupP50NS      int64   `json:"lookup_p50_ns"`
	LookupP95NS      int64   `json:"lookup_p95_ns"`
	LookupP99NS      int64   `json:"lookup_p99_ns"`
	LookupMeanNS     float64 `json:"lookup_mean_ns"`
	AvgMatches       float64 `json:"avg_matches"`
	AvgScanned       float64 `json:"avg_scanned"`
	AvgOverfetch     float64 `json:"avg_overfetch"`
	Fallbacks        uint64  `json:"fallbacks"`
	Checksum         uint64  `json:"checksum"`
}

type measurement struct {
	candidate indexlab.IntervalIndex
	buildNS   int64
	buildRuns []int64
	samples   []int64
	elapsedNS int64
	matches   int64
	scanned   int64
	overfetch int64
	checksum  uint64
}

func main() {
	var (
		sizeList         = flag.String("sizes", "10000,100000,1000000", "comma-separated segment counts")
		maxQueries       = flag.Int("queries", 10_000, "maximum queries per repeat")
		linearScanBudget = flag.Int64("linear-scan-budget", 500_000_000, "maximum linear interval inspections per size, approximately")
		repeats          = flag.Int("repeats", 3, "timed repeats")
		buildRepeats     = flag.Int("build-repeats", 5, "index builds used for the median")
		pointsPerModel   = flag.Int("points-per-model", 256, "piecewise-linear rank partition size")
		outputPath       = flag.String("out", "", "JSON output path; stdout when empty")
	)
	flag.Parse()

	sizes, err := parseSizes(*sizeList)
	if err != nil {
		fatal(err)
	}
	if *maxQueries <= 0 || *linearScanBudget <= 0 || *repeats <= 0 ||
		*buildRepeats <= 0 || *pointsPerModel <= 0 {
		fatal(errors.New("queries, scan budget, repeats, build repeats, and model size must be positive"))
	}

	cfg := config{
		Sizes:            sizes,
		MaxQueries:       *maxQueries,
		LinearScanBudget: *linearScanBudget,
		Repeats:          *repeats,
		BuildRepeats:     *buildRepeats,
		PointsPerModel:   *pointsPerModel,
		DatasetSeed:      datasetSeed,
		QuerySeed:        querySeed,
	}
	result, err := run(cfg)
	if err != nil {
		fatal(err)
	}
	if err := writeReport(*outputPath, result); err != nil {
		fatal(err)
	}
}

func run(cfg config) (report, error) {
	timerP50, timerP99 := timerOverhead()
	out := report{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		GoVersion:   runtime.Version(),
		GOOS:        runtime.GOOS,
		GOARCH:      runtime.GOARCH,
		CPUs:        runtime.NumCPU(),
		TimerP50NS:  timerP50,
		TimerP99NS:  timerP99,
		Config:      cfg,
	}

	for datasetNumber, dataset := range indexlab.DatasetNames {
		for sizeNumber, size := range cfg.Sizes {
			queryCount := queriesForSize(cfg, size)
			fmt.Fprintf(
				os.Stderr,
				"indexlab: dataset=%s segments=%d queries=%d repeats=%d\n",
				dataset,
				size,
				queryCount,
				cfg.Repeats,
			)

			ranges, err := indexlab.GenerateDataset(dataset, size, cfg.DatasetSeed)
			if err != nil {
				return report{}, err
			}
			queries := indexlab.GenerateQueries(ranges, queryCount, cfg.QuerySeed)
			measurements := buildCandidates(
				ranges,
				cfg.PointsPerModel,
				cfg.BuildRepeats,
			)
			if err := verify(measurements, queries); err != nil {
				return report{}, fmt.Errorf("%s/%d: %w", dataset, size, err)
			}
			warm(measurements, queries)
			measure(
				measurements,
				queries,
				cfg.Repeats,
				(datasetNumber+sizeNumber)%len(measurements),
			)

			for _, m := range measurements {
				out.Results = append(out.Results, summarize(
					dataset,
					size,
					queryCount,
					cfg.Repeats,
					m,
				))
			}
		}
	}
	return out, nil
}

func buildCandidates(
	ranges []index.SegmentTimeRange,
	pointsPerModel, repeats int,
) []*measurement {
	builders := []func() indexlab.IntervalIndex{
		func() indexlab.IntervalIndex { return indexlab.NewLinear(ranges) },
		func() indexlab.IntervalIndex { return indexlab.NewBinary(ranges) },
		func() indexlab.IntervalIndex {
			return indexlab.NewPiecewiseLinear(ranges, pointsPerModel)
		},
	}
	result := make([]*measurement, len(builders))
	for i := range result {
		result[i] = &measurement{}
	}
	for repeat := range repeats {
		for offset := range builders {
			i := (repeat + offset) % len(builders)
			runtime.GC()
			start := time.Now()
			result[i].candidate = builders[i]()
			result[i].buildRuns = append(
				result[i].buildRuns,
				time.Since(start).Nanoseconds(),
			)
		}
	}
	for _, m := range result {
		sort.Slice(m.buildRuns, func(i, j int) bool {
			return m.buildRuns[i] < m.buildRuns[j]
		})
		m.buildNS = percentile(m.buildRuns, 0.50)
	}
	return result
}

func verify(measurements []*measurement, queries []indexlab.Query) error {
	oracle := measurements[0].candidate
	for queryNumber, query := range queries {
		want, _ := oracle.Lookup(query)
		wantIDs := indexlab.SortedSegmentIDs(want)
		for _, m := range measurements[1:] {
			got, _ := m.candidate.Lookup(query)
			gotIDs := indexlab.SortedSegmentIDs(got)
			if !slices.Equal(gotIDs, wantIDs) {
				return fmt.Errorf(
					"correctness failure at query %d %+v: %s returned %v, linear returned %v",
					queryNumber,
					query,
					m.candidate.Name(),
					gotIDs,
					wantIDs,
				)
			}
		}
	}
	return nil
}

func warm(measurements []*measurement, queries []indexlab.Query) {
	count := min(256, len(queries))
	for _, m := range measurements {
		for _, query := range queries[:count] {
			result, _ := m.candidate.Lookup(query)
			m.checksum = checksum(m.checksum, result)
		}
	}
}

func measure(measurements []*measurement, queries []indexlab.Query, repeats, offset int) {
	for repeat := range repeats {
		for algorithmOffset := range measurements {
			m := measurements[(offset+repeat+algorithmOffset)%len(measurements)]
			for _, query := range queries {
				start := time.Now()
				result, stats := m.candidate.Lookup(query)
				elapsed := time.Since(start).Nanoseconds()

				m.samples = append(m.samples, elapsed)
				m.elapsedNS += elapsed
				m.matches += int64(len(result))
				m.scanned += int64(stats.Scanned)
				m.overfetch += int64(stats.Overfetch)
				m.checksum = checksum(m.checksum, result)
			}
		}
	}
}

func summarize(
	dataset string,
	segments, queries, repeats int,
	m *measurement,
) caseResult {
	slices.Sort(m.samples)
	sampleCount := int64(len(m.samples))
	fallbacks := uint64(0)
	if learned, ok := m.candidate.(*indexlab.PiecewiseLinearIndex); ok {
		fallbacks = learned.Fallbacks()
	}
	return caseResult{
		Dataset:          dataset,
		Segments:         segments,
		Algorithm:        m.candidate.Name(),
		QueriesPerRepeat: queries,
		Samples:          queries * repeats,
		BuildNS:          m.buildNS,
		BaseBytes:        indexlab.BaseBytes(segments),
		AuxBytes:         m.candidate.AuxBytes(),
		AuxBytesPerSeg:   float64(m.candidate.AuxBytes()) / float64(segments),
		ModelCount:       m.candidate.ModelCount(),
		LookupP50NS:      percentile(m.samples, 0.50),
		LookupP95NS:      percentile(m.samples, 0.95),
		LookupP99NS:      percentile(m.samples, 0.99),
		LookupMeanNS:     float64(m.elapsedNS) / float64(sampleCount),
		AvgMatches:       float64(m.matches) / float64(sampleCount),
		AvgScanned:       float64(m.scanned) / float64(sampleCount),
		AvgOverfetch:     float64(m.overfetch) / float64(sampleCount),
		Fallbacks:        fallbacks,
		Checksum:         m.checksum,
	}
}

func percentile(sortedValues []int64, quantile float64) int64 {
	if len(sortedValues) == 0 {
		return 0
	}
	position := int(math.Ceil(quantile*float64(len(sortedValues)))) - 1
	position = max(0, min(position, len(sortedValues)-1))
	return sortedValues[position]
}

func timerOverhead() (int64, int64) {
	samples := make([]int64, 10_000)
	for i := range samples {
		start := time.Now()
		samples[i] = time.Since(start).Nanoseconds()
	}
	slices.Sort(samples)
	return percentile(samples, 0.50), percentile(samples, 0.99)
}

func queriesForSize(cfg config, size int) int {
	budgetCount := cfg.LinearScanBudget / int64(size) / int64(cfg.Repeats)
	return min(cfg.MaxQueries, max(256, int(budgetCount)))
}

func checksum(current uint64, ranges []index.SegmentTimeRange) uint64 {
	const prime = uint64(1099511628211)
	if current == 0 {
		current = 1469598103934665603
	}
	for _, r := range ranges {
		current ^= uint64(r.SegmentID)
		current *= prime
	}
	current ^= uint64(len(ranges))
	return current * prime
}

func parseSizes(value string) ([]int, error) {
	parts := strings.Split(value, ",")
	sizes := make([]int, 0, len(parts))
	for _, part := range parts {
		size, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || size <= 0 {
			return nil, fmt.Errorf("invalid segment count %q", part)
		}
		sizes = append(sizes, size)
	}
	if len(sizes) == 0 {
		return nil, errors.New("at least one segment count is required")
	}
	return sizes, nil
}

func writeReport(path string, value report) error {
	var writer io.Writer = os.Stdout
	if path != "" {
		file, err := os.Create(path)
		if err != nil {
			return fmt.Errorf("create report: %w", err)
		}
		defer file.Close()
		writer = file
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("encode report: %w", err)
	}
	return nil
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "indexlab: %v\n", err)
	os.Exit(1)
}
