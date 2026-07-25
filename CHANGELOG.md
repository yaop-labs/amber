# Changelog

## 0.4.0 — 2026-07-25

- Added strict YAML configuration loading and explicit missing-file behavior.
- Added Reef-managed HTTP/gRPC edges and Gyre lifecycle management for storage,
  listeners, and pprof.
- Added OTLP v3 to v4 migration tooling and replay-journal retention controls.
- Added fail-closed disk admission with warning/stop watermarks, readiness
  conditions, retryable OTLP responses, and operational metrics.
- Documented and fault-tested the OTLP at-least-once dual-write contract.
- Added query budgets for page sizes, offsets, and metric range steps.
- Oversized request bodies now return HTTP 413.
- Added backup/restore and isolated restore-drill release procedures.
- Added tagged-release automation for four binaries and `SHA256SUMS`.
