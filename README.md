<p align="center">
  <img src="media/readme.png" alt="Amber" width="900">
</p>

<p align="center">
  <a href="https://github.com/yaop-labs/amber/actions/workflows/ci.yaml"><img src="https://github.com/yaop-labs/amber/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://goreportcard.com/report/github.com/yaop-labs/amber"><img src="https://goreportcard.com/badge/github.com/yaop-labs/amber" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/yaop-labs/amber"><img src="https://pkg.go.dev/badge/github.com/yaop-labs/amber.svg" alt="Go Reference"></a>
  <img src="https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go&logoColor=white" alt="Go 1.25+">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue" alt="License"></a>
  <a href="https://github.com/yaop-labs/amber/releases"><img src="https://img.shields.io/github/v/release/yaop-labs/amber?include_prereleases&sort=semver" alt="Release"></a>
  <img src="https://img.shields.io/badge/status-alpha-orange" alt="Status">
</p>

Append-only storage for logs, traces, and metrics — all three in one process.
One binary, one directory, HTTP + gRPC API.
Think "SQLite for observability".

## Features

- **One store for logs, traces, and metrics** — counters, gauges, and exponential histograms share the same WAL, flush gate, retention, and compaction
- **Append-only segments** with zstd compression and per-block min/max stats in segment footer
- **Write-Ahead Log** for crash recovery (fail-stop writers, CRC over length+seq+payload)
- **Bitmap indexes** (sorted `uint64` sets, not roaring — ULID-derived keys shattered roaring into one container per few IDs) for fast field filtering (service, level, host)
- **Full-text search** index for log body
- **Ribbon filters** for high-cardinality fields (trace_id)
- **Sparse index** for time-based segment pruning (skips 95%+ data without I/O)
- **OTLP compatible** — gRPC (:4317) and HTTP endpoints for logs, traces, and metrics
- **Log-trace correlation** — trace viewer with span tree and linked logs
- **Retention policies** — max age, max bytes, max segments
- **Embedded mode** — use as a Go library without HTTP server
- **amberctl** — command-line client and interactive terminal UI (logs, traces, span waterfall, live tail)

## Quick Start

### Binary

```bash
git clone https://github.com/yaop-labs/amber.git
cd amber
make build
cp config.example.yaml config.yaml  # edit as needed
./amber config.yaml
```

### Docker

```bash
docker build -t amber .
docker run -p 8080:8080 -p 4317:4317 \
  -v amber-data:/data \
  -v ./config.yaml:/data/config.yaml \
  amber
```

### Embedded (Go library)

```go
import "github.com/yaop-labs/amber"

db, err := amber.Open("./data", nil) // nil = default Options
defer db.Close()

db.Log(ctx, amber.LogEntry{
    Level:   amber.LevelError,
    Service: "api-gateway",
    Body:    "connection refused",
})

result, err := db.QueryLogs(ctx, &amber.LogQuery{
    Services: []string{"api-gateway"},
    Limit:    100,
})
```

## amberctl (CLI & TUI)

`amberctl` is the terminal client. It speaks amber's HTTP read API, so it works
the same against a local dev server (default `http://localhost:8080`, no auth)
or a remote one (`--addr` / `--api-key`, or `AMBER_ADDR` / `AMBER_API_KEY`).

```bash
make build                                  # builds ./amber and ./amberctl

# one-shot, scriptable
amberctl logs --service api --level ERROR --since 1h
amberctl logs -q "connection refused" --json | jq .
amberctl logs --service api -f              # live tail
amberctl traces --service checkout --since 6h
amberctl trace <trace_id>                   # span waterfall + linked logs
amberctl services
amberctl stats

# interactive terminal UI
amberctl tui
```

In the TUI: `↑/↓` move, `enter` or click opens a trace, `space` expands a row,
`/` searches, `t` cycles the time range, `f` toggles live tail, `tab` switches
logs/traces, `q` quits.

## API

### Ingest

```bash
# JSON
curl -X POST http://localhost:8080/api/v1/logs \
  -H "Authorization: Bearer <key>" \
  -d '[{"level":"ERROR","service":"api","body":"connection refused"}]'

# OTLP HTTP
curl -X POST http://localhost:8080/v1/logs \
  -H "Authorization: Bearer <key>" \
  -H "Content-Type: application/json" \
  -d @otlp_payload.json
```

### Query

```bash
# Logs
curl "http://localhost:8080/api/v1/logs?service=api-gateway&level=ERROR&limit=50" \
  -H "Authorization: Bearer <key>"

# Trace
curl "http://localhost:8080/api/v1/traces/<trace_id>" \
  -H "Authorization: Bearer <key>"

# Services list
curl "http://localhost:8080/api/v1/services" \
  -H "Authorization: Bearer <key>"
```

### Log Query Parameters

`GET /api/v1/logs` supports:

| Parameter | Description |
|-----------|-------------|
| `service` | Comma-separated service names |
| `level` | Comma-separated levels (`ERROR,WARN,...`) |
| `host` | Comma-separated host names |
| `q` | Full-text search in log body |
| `from` / `to` | RFC3339 time range |
| `limit` | Page size (default `100`) |
| `offset` | Result offset |
| `attr.<key>` | Exact match on structured attribute |

Examples:

```bash
# Time-bounded search
curl "http://localhost:8080/api/v1/logs?service=checkout&from=2026-05-07T10:00:00Z&to=2026-05-07T11:00:00Z&q=timeout"

# Filter by structured attribute
curl "http://localhost:8080/api/v1/logs?service=checkout&attr.env=prod&attr.region=eu-west-1"

# NDJSON streaming-friendly output
curl "http://localhost:8080/api/v1/logs?service=checkout&limit=100" \
  -H "Accept: application/x-ndjson"
```

### Trace API

```bash
# Trace list
curl "http://localhost:8080/api/v1/traces?service=checkout&limit=20&offset=0" \
  -H "Authorization: Bearer <key>"

# Single trace with span tree + correlated logs
curl "http://localhost:8080/api/v1/traces/<trace_id>" \
  -H "Authorization: Bearer <key>"
```

`GET /api/v1/traces` supports:

| Parameter | Description |
|-----------|-------------|
| `service` | Comma-separated service names |
| `from` / `to` | RFC3339 time range |
| `limit` | Number of traces to return (default `20`) |
| `offset` | Trace offset |

Response fields for each trace summary:

- `trace_id`
- `service`
- `operation`
- `start_time`
- `duration_ms`
- `span_count`
- `has_errors`

### Metrics API

Metrics are ingested over OTLP (`POST /v1/metrics`, HTTP or gRPC) and queried
through a small purpose-built API — counters and gauges as rates, exponential
histograms as quantiles:

```bash
# Instant rate by label (counters), evaluated at `time`
curl "http://localhost:8080/api/v1/metrics/rate?metric=http_requests_total&by=service&time=2026-06-16T10:00:00Z" \
  -H "Authorization: Bearer <key>"

# Per-step range rate over a window
curl "http://localhost:8080/api/v1/metrics/rate_range?metric=http_requests_total&by=route&from=2026-06-16T09:00:00Z&to=2026-06-16T10:00:00Z&step=60s" \
  -H "Authorization: Bearer <key>"

# Quantile from exponential histograms
curl "http://localhost:8080/api/v1/metrics/quantile?metric=http_request_duration&q=0.95&by=service" \
  -H "Authorization: Bearer <key>"

# Series list and metrics store stats
curl "http://localhost:8080/api/v1/metrics" -H "Authorization: Bearer <key>"
curl "http://localhost:8080/api/v1/metrics/stats" -H "Authorization: Bearer <key>"
```

Sample values are stored as `int64` — see [Metrics value model](#metrics-value-model-alpha) below for the scale/precision trade-off.

### Admin API

```bash
# Runtime + storage stats
curl "http://localhost:8080/api/v1/admin/stats" \
  -H "Authorization: Bearer <key>"

# Segment metadata
curl "http://localhost:8080/api/v1/admin/segments" \
  -H "Authorization: Bearer <key>"
```

`/api/v1/admin/stats` includes:

- segment counts and total records
- active segment metadata
- sparse index size
- heap usage snapshot

## Durability semantics (embedded API)

`db.Log()` and `db.Span()` are **asynchronous**: a nil error means the entry
was accepted into the in-process ingest queue (10k entries by default), not
that it reached disk. Entries are batched, written to the WAL with an fsync,
and acknowledged internally — but a crash (`kill -9`, OOM, power loss) loses
whatever was still in the queue or an unflushed batch.

When you need a durability barrier, call:

```go
if err := db.Flush(ctx); err != nil { /* ctx expired */ }
```

`Flush` returns once every `Log`/`Span` call that completed before it was
invoked has been written to the WAL and synced (or counted as dropped —
write failures surface via the ingest circuit breaker and the
`ingest_dropped_total` self-metric, not via `Flush`). `db.Close()` drains the
queue the same way on shutdown.

The HTTP ingest API has the same model and makes it explicit with status
`202 Accepted`.

## Metrics value model (alpha)

The embedded metrics engine stores every sample as an **int64**. OTLP float
points are converted to `round(value × scale)`:

- the default scale is **1000** (three decimal digits of precision);
- values smaller than `1/scale` collapse to `0`;
- `NaN` (OTLP staleness marker), `±Inf`, and values whose scaled form
  overflows int64 are **dropped at ingest** and counted in the
  `metrics_ingest_rejected{reason="value_unencodable"}` self-metric;
- the scale is recorded in the reserved `__scale__` label, so the same metric
  sent with different scales produces different series.

If you need more than 3 decimal digits, set an explicit per-point scale.
A native float codec is on the roadmap; until then size your scale to the
precision your data actually carries.

## Configuration

See [config.example.yaml](config.example.yaml) for all options. Key settings:

| Setting | Default | Description |
|---------|---------|-------------|
| `storage.data_dir` | `./data` | Data directory |
| `storage.segment_max_records` | `1000000` | Records per segment before rotation |
| `storage.index_cache_size` | `32` | Max sealed index readers kept in memory |
| `ingest.batch_size` | `1000` | WAL batch size |
| `ingest.batch_timeout` | `100ms` | Max wait before flushing batch |
| `ingest.queue_size` | `100000` | Buffered ingest queue length |
| `api.http_addr` | `:8080` | HTTP listen address |
| `api.grpc_addr` | `:4317` | gRPC listen address (OTLP) |
| `api.api_key` | _(empty)_ | Bearer token (empty = auth disabled) |
| `retention.max_age` | `0s` | Max segment age (0 = disabled) |

## Benchmarks

Measured with the in-repo `obsbench` suite: rate-capped ingest (20k/s, so every
system holds identical data), a fixed 600s settle, sequential runs, pinned
competitor versions, and a **result-count equality gate** per run (same seeds →
same query instances → systems must agree before any latency is published).
amber runs with an 800 MB soft memory limit (`runtime.memory_limit`). All
latencies are HTTP end-to-end, client-measured. Numbers are medians across runs.

These are honest, current results — amber is a deliberately experimental engine
(sorted-slice bitmaps, no roaring/columnar): it wins logs outright, and on
metrics/traces it trades raw scan/aggregate speed for low memory and a compact,
unified store. Where a columnar competitor wins, the table says so.

### Logs — 5M records, 3 runs × 3 systems

Loki 3.4.2 · VictoriaLogs v1.36.0. Query p50/p95 (ms), medians of 3 runs:

| Scenario | amber | Loki | VictoriaLogs |
|----------|------:|-----:|-------------:|
| q1 — point (service + level) | **0.88** / 5.5 | 9.4 / 16 | 9.3 / 14 |
| q2 — range (service sub-window) | **1.72** / 6.4 | 48.5 / 69 | 13.8 / 19.5 |
| q3 — full-text (common token) | **11.0** / 16.5 | 51 / 71 | 25.2 / 25 |
| q4 — full-text (rare token) | **0.45** / 0.49 | 1927 / 1954 | 10.0 / 12.7 |

| Resource | amber | Loki | VictoriaLogs |
|----------|------:|-----:|-------------:|
| RSS peak (MB) | 810 | 1134 | **356** |
| Storage (MB) | 653 | 260 | **226** |

**amber wins all four query scenarios** — q4 (rare-token full-text) by three
orders of magnitude over Loki, the FTS index paying off. It still loses on RSS and
on-disk size, but the gap narrowed: the AFT3 ordinal-delta `.fidx` format cut
amber's storage from 875 → **653 MB** (3.9× → 2.9× VictoriaLogs). The same change
moved q3 (common-token full-text) from 4 → 11 ms — the ordinal table is mapped
back to record IDs with a per-result `pread` rather than held resident, trading a
few ms on wide result sets for flat memory; amber still answers q3 well ahead of
both competitors. (Re-campaign on the fixed engine, 2026-06-23, equality gate
green across all three runs.)

### Metrics — 100k scalar + 10k histogram series, 4 systems

Mimir 3.1.0 · VictoriaMetrics v1.145.0 · Prometheus v3.12.0. Query p50/p95 (ms).
The amber column is an amber-only re-run on the reworked engine (2026-06-23);
competitors are the prior fully-comparable run (sequential methodology — same
workload, same equality gate).

| Scenario | amber | Mimir | VictoriaMetrics | Prometheus |
|----------|------:|------:|----------------:|-----------:|
| qm1 — instant rate | **76 / 546** | 768 / 883 | 187 / 227 | 458 / 490 |
| qm2 — range rate | 539 / 1194 | 1325 / 1529 | **267 / 337** | 868 / 927 |
| qm3 — histogram quantile | 990 / 1370 | 495 / 654 | 291 / 609 | **259 / 344** |
| qm4 — group-by rate | **80 / 564** | 760 / 922 | 179 / 206 | 423 / 506 |

| Resource | amber | Mimir | VictoriaMetrics | Prometheus |
|----------|------:|------:|----------------:|-----------:|
| RSS peak (MB) | 912 | 2076 | **576** | 718 |
| Storage (MB) | 539 | 370 | **152** | 255 |

**The query rework flipped the metrics story.** At campaign time (2026-06-13)
amber was the slowest on all four; on the reworked engine it now **wins qm1 and
qm4 outright** — instant- and group-by-rate moved onto a resident block index,
2139 → 76 ms and 2239 → 80 ms (~28×), ahead of even VictoriaMetrics. qm2 (range
rate) dropped 6.3 s → 0.54 s (resident index + series-partitioned parallelism),
second to VM at ~2× and past Mimir/Prometheus. qm3 (histogram quantile) is the
remaining laggard but improved 1.30 → 0.99 s (map-free exp-bucket decode, −79%
allocations). RSS peak fell 1110 → 912 MB — moving the rate paths off the
directory cache removed the cache-coexistence overhead. amber still trails VM on
the columnar scan/aggregate shape (qm3) and on storage.

### Traces — 3.6M traces / 36M spans, 2 runs × 3 systems

Tempo 2.10.1 · VictoriaTraces v0.9.2. Query p50 (ms):

amber is an amber-only re-run after adding span tag/duration indexes (2026-06-23);
competitors are the prior comparable run. Query p50 (ms):

| Scenario | amber | Tempo | VictoriaTraces |
|----------|------:|------:|---------------:|
| QT1 — trace-ID lookup | 99 | 94 | **18** |
| QT2 — service + operation search | 3720 | 108 | **26** |
| QT3 — service + duration search | 2398 | 67 | **40** |

| Resource | amber | Tempo | VictoriaTraces |
|----------|------:|------:|---------------:|
| RSS peak (MB) | **780** | 2249 | 1537 |
| Storage (MB) | 2113 | 5166 | **1639** |

**amber wins RSS decisively** (≈2.9× lighter than Tempo) and is second on storage.
The scan-search scenarios improved with the new span bitmaps (service/operation/
status + log2 duration buckets, replacing a blind full-scan): QT2 7.6 → 3.7 s,
QT3 6.1 → 2.4 s (~2–2.5×). They still trail VictoriaTraces' columnar engine by a
wide margin, and the bench exposed why: with one service per trace the service
posting is a large fraction of all spans, and the bitmap intersect is a full
merge over it — so a low-selectivity predicate still pays an O(set) cost
regardless of how few spans finally match. Closing the rest needs a
selectivity-aware intersect (galloping/binary-search for skewed sets, or skipping
a field whose set is most of the segment), not more index fields. An honest
partial win: indexing took scan-search from *embarrassing* to *2–2.5× better*,
and named the next bottleneck precisely. Point lookup (QT1) is on par with Tempo.

<details>
<summary>Methodology and notes</summary>

- **Equality gate**: each run compares result counts across systems before
  publishing latency; a mismatch fails the run. Logs use exact set equality;
  metrics/traces use coarse gates (group cardinality / full-page count) where
  cross-system value identity isn't well defined.
- **Ingest**: rate-capped at 20k/s for every system so they all index the same
  data; partial queue-full rejects are parsed from amber's `503` body for true
  acked counts.
- **`TotalHits` is a lower bound** in amber (heap-threshold block skip) — it is
  never used for verification; admin `segments.total_records` is.
- **All OTLP labels are datapoint attributes** for metrics (resource attrs are
  renamed per system and would break cross-system label identity); for traces
  `service.name` rides as the OTLP resource attribute (backend standard).
- Full methodology and per-run artifacts live with the `obsbench` harness.

</details>

## License

Apache License 2.0
