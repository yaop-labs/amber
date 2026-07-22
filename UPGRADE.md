# Amber upgrade and restore policy

Amber `0.X.0` releases may change configuration and on-disk implementation,
but preserve the documented snapshot restore contract. Before upgrading:

1. stop Amber cleanly (offline backup is the consistency boundary);
2. run `amber-backup create -data-dir <data> -snapshot <snapshot>`;
3. run `amber-backup verify -snapshot <snapshot>` and retain its checkpoint;
4. upgrade the binaries and start with the same data directory;
5. if rollback is required, stop Amber and restore the verified snapshot into a
   new directory, then point the previous binary at that directory.

Never restore over an existing directory. `amber-backup restore` publishes only
to a new destination after verifying every file and records the verified
checkpoint in the restored operational state.

Patch releases (`0.0.x`) are limited to compatible fixes. Minor releases may
require configuration migration; unknown YAML fields fail fast, and legacy
configuration shapes are rejected with an actionable error.

For S3-backed snapshots, run the `amber-backup drill` command on an isolated
workspace at least once per release. A drill must verify database identity,
checkpoint, journal replay, log/trace queries, and metric catalog readability.
