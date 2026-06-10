// Package store implements metric catalog, manifest, block lifecycle,
// retention, and index eviction.
//
// The catalog and retention paths rely on these invariants:
//
//   - Torn-tail recovery truncates catalog logs to the last valid record
//     boundary and is idempotent across repeated boots.
//   - Index eviction and block retention use the same time cutoff.
//   - A lost catalog REGISTER from a group-commit window can be rebuilt from
//     block directories during startup reconciliation.
//   - Snapshot rotation is recoverable after each file-system step.
//   - Block-derived reconciliation imports series that are present on disk but
//     missing from the catalog.
//
// See the catalog log, retention, and reconciliation tests before changing
// those paths.
package store
