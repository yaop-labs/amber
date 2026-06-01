package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/yaop-labs/amber/internal/metricsengine/block"
	"github.com/yaop-labs/amber/internal/metricsengine/engine"
	"github.com/yaop-labs/amber/internal/metricsengine/model"
	"github.com/yaop-labs/amber/internal/metricsengine/query"
	"github.com/yaop-labs/amber/internal/metricsengine/store"
)

func main() {
	demoPath := flag.String("demo-block", "", "write a small demo block to this path")
	demoStore := flag.String("demo-store", "", "write, flush, and query a small directory-backed store")
	queryPath := flag.String("query-block", "", "read a block and print matching series")
	statsStore := flag.String("stats-store", "", "print stats for a directory-backed store")
	compactStore := flag.String("compact-store", "", "compact all blocks in a directory-backed store")
	serveStore := flag.String("serve-store", "", "serve a directory-backed store over HTTP")
	listenAddr := flag.String("listen", "127.0.0.1:9099", "listen address for -serve-store")
	rateStore := flag.String("rate-store", "", "run a grouped rate query against a directory-backed store")
	expr := flag.String("expr", "", "range selector expression for -rate-store")
	by := flag.String("by", "job", "grouping label for -rate-store")
	endMillis := flag.Int64("end-ms", 0, "evaluation end time in unix milliseconds for -rate-store; defaults to now")
	selectorText := flag.String("selector", "{}", "debug label selector for -query-block")
	flag.Parse()

	if *rateStore != "" {
		runRateQuery(*rateStore, *expr, *by, *endMillis)
		return
	}

	if *serveStore != "" {
		runHTTPServer(*serveStore, *listenAddr)
		return
	}

	if *compactStore != "" {
		st, err := store.Open(*compactStore)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer st.Close()
		path, err := st.Compact()
		if errors.Is(err, store.ErrNoSamples) {
			fmt.Println("compaction skipped: need at least two blocks")
			return
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("compacted into %s\n", path)
		return
	}

	if *statsStore != "" {
		st, err := store.Open(*statsStore)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer st.Close()
		stats, err := st.Stats()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("blocks=%d series=%d samples=%d bytes=%d\n", stats.Blocks, stats.Series, stats.Samples, stats.Bytes)
		return
	}

	if *demoStore != "" {
		runStoreDemo(*demoStore)
		return
	}

	if *queryPath != "" {
		selector, err := parseDebugSelector(*selectorText)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		series, err := query.SelectBlock(*queryPath, selector, query.Options{})
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		rates, err := query.Rate(series)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Printf("matched %d series, computed %d rates\n", len(series), len(rates))
		return
	}

	if *demoPath == "" {
		fmt.Println("metrics-engine: library-first metrics storage engine")
		fmt.Println("use -demo-block <path> to write and read a small block")
		fmt.Println("use -demo-store <dir> to exercise WAL + manifest + query")
		fmt.Println("use -compact-store <dir> to merge local blocks")
		fmt.Println("use -serve-store <dir> -listen 127.0.0.1:9099 for HTTP stats/query")
		fmt.Println(`use -query-block <path> -selector '{job="demo"}' to scan a block`)
		fmt.Println(`use -rate-store <dir> -expr 'metric_name{job="api"}[5m]' -by job`)
		return
	}

	e := engine.New()
	labels := model.LabelSet{{Name: "__name__", Value: "demo_counter_total"}, {Name: "job", Value: "demo"}, {Name: "instance", Value: "local"}}
	start := time.Now().UnixMilli() - 4000
	for i := range int64(5) {
		if _, err := e.Append(labels, model.MetricTypeCounter, start+i*1000, 100+i*i); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := e.FlushBlock(*demoPath); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	series, err := block.ReadFile(*demoPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s with %d series\n", *demoPath, len(series))
}

func runStoreDemo(dir string) {
	st, err := store.Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer st.Close()

	labels := model.LabelSet{{Name: "__name__", Value: "demo_counter_total"}, {Name: "job", Value: "demo"}, {Name: "instance", Value: "local"}}
	start := time.Now().UnixMilli() - 4000
	var samples []model.Sample
	for i := range int64(5) {
		samples = append(samples, model.Sample{
			Labels:    labels,
			Type:      model.MetricTypeCounter,
			Timestamp: start + i*1000,
			Value:     100 + i*i,
		})
	}
	if _, err := st.AppendBatch(samples); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	path, err := st.Flush()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	selector, err := parseDebugSelector(`{job="demo"}`)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	series, err := st.Select(selector, query.Options{})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rates, err := query.Rate(series)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s; matched %d series, computed %d rates\n", path, len(series), len(rates))
}

func runRateQuery(dir string, expr string, by string, endMillis int64) {
	if expr == "" {
		fmt.Fprintln(os.Stderr, "-expr is required for -rate-store")
		os.Exit(1)
	}
	if endMillis == 0 {
		endMillis = time.Now().UnixMilli()
	}
	st, err := store.Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer st.Close()
	rs, err := parseDebugRangeSelector(expr)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	rates, err := st.RateByLabelRange(rs, endMillis, by)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for label, rate := range rates {
		fmt.Printf("%s=%s rate=%f\n", by, label, rate)
	}
}

func runHTTPServer(dir string, addr string) {
	st, err := store.Open(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer st.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := st.Stats()
		writeJSON(w, stats, err)
	})
	mux.HandleFunc("/rate", func(w http.ResponseWriter, r *http.Request) {
		expr := r.URL.Query().Get("expr")
		by := r.URL.Query().Get("by")
		if by == "" {
			by = "job"
		}
		endMillis := time.Now().UnixMilli()
		if raw := r.URL.Query().Get("end_ms"); raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			endMillis = parsed
		}
		rs, err := parseDebugRangeSelector(expr)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		rates, err := st.RateByLabelRange(rs, endMillis, by)
		writeJSON(w, rates, err)
	})

	fmt.Printf("serving %s on http://%s\n", dir, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func writeJSON(w http.ResponseWriter, value any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
