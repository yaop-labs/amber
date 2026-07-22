# Changelog

## Unreleased

- Added strict YAML configuration loading and explicit missing-file behavior.
- Added Reef-managed HTTP/gRPC edges and Gyre lifecycle management for storage,
  listeners, and pprof.
- Added OTLP v3 to v4 migration tooling and replay-journal retention controls.
- Added query budgets for page sizes, offsets, and metric range steps.
- Oversized request bodies now return HTTP 413.
- Added backup/restore and isolated restore-drill release procedures.
