# Amber upgrade and restore policy

Amber `0.X.0` releases may change configuration and on-disk implementation,
but preserve the documented snapshot restore contract.

## v0.3.0 to v0.4.0

The v0.4 upgrade is offline and copy-on-write. Do not start v0.4 against an
unmigrated v0.3 data root.

1. stop Amber cleanly (offline backup is the consistency boundary);
2. with the v0.3 binary, run
   `amber-backup create -data-dir <v3-data> -snapshot <snapshot>`;
3. run `amber-backup verify -snapshot <snapshot>` and retain its checkpoint;
4. run `amber-migrate otlp-v4 <v3-data> <v4-data>`;
5. retain the printed database ID, record count, and SHA-256 semantic digest;
6. start the v0.4 binary with `<v4-data>`, then verify `/readyz`, `/status`,
   representative log/trace queries, and the metric catalog;
7. keep `<v3-data>` unchanged through the rollback window.

`amber-migrate` requires a cleanly closed source, refuses a non-empty metrics
WAL or incomplete flush transition, verifies the new journal before atomically
publishing `<v4-data>`, and never modifies `<v3-data>`.

Rollback is therefore direct: stop v0.4 and restart v0.3 with the original
`<v3-data>`. If that root is unavailable, restore the verified pre-upgrade
snapshot into a new directory and point v0.3 at it.

| Source | Target | Supported path |
|---|---|---|
| v0.3.0 | v0.4.0 | Offline `amber-migrate otlp-v4`, copy-on-write |
| fresh root | v0.4.0 | Start normally; AOT4 is created on first open |
| v0.4.0 | v0.3.0 | No in-place downgrade; use untouched v0.3 root/snapshot |

Never restore over an existing directory. `amber-backup restore` publishes only
to a new destination after verifying every file and records the verified
checkpoint in the restored operational state.

Patch releases (`0.0.x`) are limited to compatible fixes. Minor releases may
require configuration migration; unknown YAML fields fail fast, and legacy
configuration shapes are rejected with an actionable error.

For S3-backed snapshots, run the `amber-backup drill` command on an isolated
workspace at least once per release. A drill must verify database identity,
checkpoint, journal replay, log/trace queries, and metric catalog readability.
