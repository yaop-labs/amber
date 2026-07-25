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
invoked has been written to the WAL and synced. Covered serialization,
storage, or sync failures are returned by `Flush` and also feed the ingest
circuit breaker. `db.Close()` drains the queue the same way on shutdown.

The HTTP ingest API has the same model and makes it explicit with status
`202 Accepted`.

### OTLP durability and retry contract

An OTLP HTTP/gRPC success means the accepted subset is durable in both the
query projection and the AOT4 replay journal. Amber uses at-least-once
delivery: the projection is synced first, then the accepted original OTLP
subset is appended and synced to the journal.

If the second write fails, Amber returns retryable HTTP `503` / gRPC
`Unavailable`. The projection may already contain the first attempt, so the
required exporter retry can create a duplicate. Amber 0.4 does not promise
exactly-once ingest or perform online deduplication. The successful retry is
present in the canonical journal and journal replay is the reconciliation
source for rebuilding projections. Exporters must retry retryable responses;
consumers that require exactly-once results must deduplicate using their own
stable event identity.

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
| `storage.segment_max_records` | `100000` | Records per segment before rotation |
| `storage.segment_max_bytes` | `134217728` | Uncompressed payload bytes before rotation |
| `storage.disk_warning_free_bytes` | `2147483648` | Degrade status below this free-space watermark |
| `storage.disk_stop_free_bytes` | `1073741824` | Reject ingest and fail readiness below this watermark |
| `storage.index_cache_size` | `0` | Max sealed index readers; 0 uses the executor default |
| `ingest.batch_size` | `1000` | WAL batch size |
| `ingest.batch_timeout` | `100ms` | Max wait before flushing batch |
| `ingest.queue_size` | `100000` | Buffered ingest queue length |
| `api.http_addr` | `localhost:8080` | HTTP listen address |
| `api.grpc_addr` | _(empty)_ | gRPC listen address (OTLP); empty disables it |
| `api.api_key` | _(empty)_ | Bearer token (empty = auth disabled) |
| `retention.logs.max_age` | `0s` | Terminal log projection age (0 = disabled) |
| `retention.spans.max_age` | `0s` | Terminal span projection age (0 = disabled) |
| `retention.journal.max_age` | `0s` | Physical raw replay age from Amber acceptance time (0 = disabled) |

### Replay journal retention

Amber keeps accepted OTLP payloads in the canonical `otlp_v4` replay journal
in addition to the query projections. Configure `retention.journal` to bound
that physical raw copy by age, bytes, or sealed-segment count:

```yaml
retention:
  interval: 1h
  journal:
    max_age: 720h
    max_bytes: 0
    max_segments: 0
```

Journal age uses the time Amber accepted a record, not timestamps carried by
the telemetry. Backfilled or clock-skewed events therefore receive the same raw
retention window as live data. Pruning is crash-retryable and operates on
sealed segments; when an active segment is wholly expired it is rotated before
deletion. Backups and offline replay see only the retained journal.

`retention.logs` and `retention.spans` bound the query projections; the journal
policy is the physical raw-payload/compliance boundary shared by all signals.
If raw payloads must disappear no later than a fixed deadline, configure the
journal deadline explicitly. With all journal limits at zero, raw replay is
unbounded. Disk admission still stops new writes before ENOSPC, but operators
must configure journal retention or provision capacity to resume ingest.

## License

Apache License 2.0
