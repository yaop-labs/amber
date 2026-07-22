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
