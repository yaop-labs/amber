<p align="center">
  <img src="media/readme.png" alt="Amber" width="400">
</p>

<h1 align="center">Amber</h1>

<p align="center">
  <a href="https://github.com/hnlbs/amber/actions/workflows/ci.yml"><img src="https://github.com/hnlbs/amber/actions/workflows/ci.yaml/badge.svg" alt="CI"></a>
  <a href="https://github.com/hnlbs/amber/actions/workflows/lint.yml"><img src="https://github.com/hnlbs/amber/actions/workflows/lint.yaml/badge.svg" alt="Lint"></a>
  <a href="https://goreportcard.com/report/github.com/hnlbs/amber"><img src="https://goreportcard.com/badge/github.com/hnlbs/amber" alt="Go Report Card"></a>
  <a href="https://pkg.go.dev/github.com/hnlbs/amber"><img src="https://pkg.go.dev/badge/github.com/hnlbs/amber.svg" alt="Go Reference"></a>
  <img src="https://img.shields.io/badge/Go-1.25+-00ADD8?logo=go&logoColor=white" alt="Go 1.25+">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue" alt="License"></a>
  <a href="https://github.com/hnlbs/amber/releases"><img src="https://img.shields.io/github/v/release/hnlbs/amber?include_prereleases&sort=semver" alt="Release"></a>
  <img src="https://img.shields.io/badge/status-alpha-orange" alt="Status">
</p>

Append-only storage for logs and traces. 
One binary, one directory, HTTP + gRPC API.
Think "SQLite for observability".

## Features

- **Append-only segments** with zstd compression and per-block min/max stats in segment footer
- **Write-Ahead Log** for crash recovery
- **Bitmap indexes** (Roaring Bitmap) for fast field filtering (service, level, host)
- **Full-text search** index for log body
- **Ribbon filters** for high-cardinality fields (trace_id)
- **Sparse index** for time-based segment pruning (skips 95%+ data without I/O)
- **OTLP compatible** — gRPC (:4317) and HTTP endpoints for logs and traces
- **Log-trace correlation** — trace viewer with span tree and linked logs
- **Retention policies** — max age, max bytes, max segments
- **Embedded mode** — use as a Go library without HTTP server
- **amberctl** — command-line client and interactive terminal UI (logs, traces, span waterfall, live tail)

## Quick Start

### Binary

```bash
git clone https://github.com/hnlbs/amber.git
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
import "github.com/hnlbs/amber"

db, err := amber.Open("./data")
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

100M records, VPS (8 vCPU, 16 GB RAM), [obs-bench](https://codeberg.org/HoneyLabs/obs-bench) suite. All numbers are HTTP end-to-end (client-measured), p50 latency.

### Query latency (p50, ms)

| Query | Amber | Loki | ClickHouse | OpenSearch |
|-------|------:|-----:|-----------:|----------:|
| R1 — point (service + level) | 55 | 24 | 224 | 380 |
| R2 — time range (1h window) | 59 | 8.6 | 249 | 51 |
| R3 — full-text search | 57 | 28,941 | 197 | 25 |
| R4 — rare token FTS | 49 | 66,404 | 123 | 5.1 |
| R5 — trace ID lookup | 84 | 8.1 | 179 | 4.0 |

> Amber server-side latency (excluding JSON serialization + network): R1 2.2 ms, R2 0.9 ms, R3 0.9 ms, R5 2.2 ms. The ~50 ms overhead is Go JSON encoding of 100 entries per response — same overhead applies to all systems but varies by serialization library.

### Ingest throughput (W1)

| System | rec/s | 
|--------|------:|
| ClickHouse | 336K |
| Loki | 224K |
| OpenSearch | 129K |
| Amber | 118K |

### Storage efficiency (100M records, 30 GB raw)

| System | Storage | Ratio | Idle RSS |
|--------|--------:|------:|---------:|
| Amber | 5.9 GB | 0.20 | 14.8 MiB |
| OpenSearch | 20.8 GB | 0.69 | 1,410 MiB |
| Loki | 23.7 GB | 0.79 | 96.8 MiB |
| ClickHouse | 27.9 GB | 0.93 | 462.6 MiB |

<details>
<summary>Methodology and notes</summary>

- **Dataset**: 100M synthetic log entries (10 services, 6 levels, realistic bodies with UUIDs), pre-generated NDJSON
- **Loadgen**: 8 workers, 500 rec/batch, max throughput (no rate limit)
- **Queries**: 20 qps, 4 workers, 60s per scenario, randomized parameters
- **VictoriaLogs** excluded: bulk ingest via `/insert/jsonline` silently dropped records (storage = 8 KB after 100M ingest). Single-record inserts work; bulk persistence bug not investigated. Results would be misleading
- **ClickHouse FTS** uses `position(body, ?)` instead of `hasToken` because `hasToken` treats `_` as a token separator, rejecting UUIDs. This bypasses the `tokenbf_v1` index — R3/R4 numbers reflect a full scan
- **Loki R3** had 11 errors (timeouts on 100M full-text scan)

</details>

## License

Apache License 2.0
