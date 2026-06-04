// Package store implements the metric storage layer: catalog,
// manifest, block lifecycle, retention, and the index-eviction sweep.
//
// Five load-bearing invariants live across this package. Each one is
// the kind of thing a future refactor can silently violate, so each
// is pinned by a named test and called out here as a single map of
// "where the load-bearing logic lives, and what fails if it bends":
//
// (1) Append-log torn-tail recovery is idempotent across reboots.
//
//	replayCatalogFile truncates the file to the last good record
//	boundary on the first torn-tail recovery. A second immediate boot
//	must produce identical state and not shrink the file further.
//	Without this property, a sequence of crashes could compound, each
//	chopping a few more records. Pinned by:
//	  TestCatalogLogTornTailIsIdempotentAcrossReboots
//
// (2) Eviction-boundary equals block-retention boundary.
//
//	The index-eviction threshold MUST equal the block-retention
//	threshold (both derived from opts.Retention here). If they drift,
//	the dangerous direction is "block exists, series evicted from
//	index" — the active-series gauge then lies and label-routing-into-
//	block-data only survives because matchLabels-on-block-directory is
//	an independent path. The opposite direction (eviction lagging
//	retention) is impossible by construction. Pinned by:
//	  TestStoreEvictionBoundaryEqualsBlockRetention
//	  TestStoreEvictionBoundaryShapeIsStable
//
// (3) Group-commit batches catalog fsyncs IFF a lost REGISTER is
//
//	recoverable via block reconcile. AppendRegister returns only after
//	its record is fsync'd by the committer goroutine — but the
//	committer batches fsyncs across concurrent appenders, so a crash
//	in the last <= flushInterval window MAY drop a record. That is
//	safe ONLY because the engine WAL group-commit fsync'd the
//	associated samples on an independent path, and on next boot
//	reconcileLastTouchFromBlocks resurrects the series by labels via
//	GetOrCreateAt. The two paths are independent — that is what makes
//	the batching safe. Pinned by:
//	  TestCatalogLogLostRegisterRecoveredFromBlocks
//	  TestCatalogLogLostRegisterByBlockReconcileAlone
//
// (4) Snapshot+rotation crash matrix covers every boundary.
//
//	The lifecycle has four observable transitions (post-3a, post-3b,
//	post-3c, post-3d). Recovery yields the same logical catalog from
//	any of them, with concurrently-appended records preserved.
//	Pinned by:
//	  TestCatalogLogRotationCrashMatrix
//	  TestCatalogLogOrphanedTmpRecovery
//
// (5) Block-derived lastTouch reconcile resurrects orphan series.
//
//	On boot, every series found in block directories whose ID is not
//	in the registry is re-Imported by labels via GetOrCreateAt.
//	Without this, a series whose only on-disk evidence is a block
//	(catalog log lost) would never appear in active-series count and
//	the eviction sweep would never see it. Pinned by:
//	  TestReconcileLastTouchFromBlocks
//
// Any change touching the catalog log, the eviction sweep, or the
// retention threshold should re-read this file first.
package store
