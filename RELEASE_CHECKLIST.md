# Amber v0.4.0 release checklist

## Automated local gates

- [x] `make release-check` (targeted tests and `go vet`)
- [x] `make release VERSION=0.4.0-rc1` (four binaries + `SHA256SUMS`)
- [x] Gyre adapter race tests, including injected listener failures
- [x] backup create/verify/restore and corruption rejection tests
- [x] query budget and HTTP 413 contract tests
- [x] secure `amberctl` invalid-TLS fail-closed test
- [x] query benchmark baseline in `BENCHMARKS.md`
- [x] CI workflow runs full race suite, MinIO integration, artifact checksums,
  clean-host smoke, secure TLS/token-file `amberctl` smoke, and benchmark
  artifact upload

## Required release-host gates

- [ ] obtain a green CI run for full `go test ./... -race -count=1`;
- [ ] obtain a green CI clean-host artifact smoke run: start `amber`, query
  `/healthz`, `/readyz` and `/status`, run `amberctl`, then graceful shutdown;
- [ ] obtain a green S3 restore drill run against the production-compatible
  object store;
- [ ] approve benchmark output from the release runner against regression
  budgets;
- [ ] publish binaries and checksums using the platform release/signing policy.

The unchecked items require external infrastructure or a host-level network
permission and are intentionally not marked green by local unit tests.
