# v0.4.0 release benchmark baseline

Run on 2026-07-22 with:

```text
go test ./benchmarks/ -bench='BenchmarkQuery_FullScan_(1k|10k|100k)$' -benchtime=1s -run='^$'
```

Environment: Linux amd64, AMD Ryzen 5 2600 Six-Core Processor.

| Dataset | ns/op | queries/sec | bytes/op | allocs/op |
|---:|---:|---:|---:|---:|
| 1k | 0.96 ms | 1,040 | 271,581 | 6,329 |
| 10k | 4.89 ms | 204.6 | 1,284,035 | 60,332 |
| 100k | 35.69 ms | 28.02 | 11,467,791 | 600,355 |

These figures are a baseline, not a cross-machine performance promise. Release
comparisons must repeat the same command and report hardware, dataset, config,
and any regression budget. Full mixed-signal/RSS campaigns remain environment-
dependent and should run on the release host or CI benchmark runner.

## v0.4.0 release verification — 2026-07-25

Repeated on the same AMD Ryzen 5 2600 host:

| Dataset | ns/op | queries/sec | bytes/op | allocs/op |
|---:|---:|---:|---:|---:|
| 1k | 0.84 ms | 1,190 | 274,828 | 6,330 |
| 10k | 4.56 ms | 219.5 | 1,293,757 | 60,332 |
| 100k | 34.19 ms | 29.25 | 11,520,823 | 600,356 |

Latency/throughput improved relative to the July 22 baseline. Allocation
differences are below 2%; this is accepted as noise from response/result
bookkeeping rather than a material regression.

The release-host workflow also runs a deterministic mixed-query RSS smoke
campaign (`1000` series, `30` ticks, `6` blocks, two 2-second phases). Local
release evidence:

| Phase | Peak RSS | Average RSS | GC CPU | Operations |
|---|---:|---:|---:|---:|
| default caches | 26 MiB | 24 MiB | 11.0% | 6,814 |
| 800 MiB limit / 400 MiB cache budget | 26 MiB | 24 MiB | 11.0% | 7,036 |

This smoke fixture catches functional/RSS regressions in CI. It is not a
capacity claim; production sizing still requires the full configurable
campaign on representative hardware.
