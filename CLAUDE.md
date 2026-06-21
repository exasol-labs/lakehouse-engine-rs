# lakehouse-vs

DataFusion-in-Rust-UDF Virtual Schema PoC for Exasol. Mission: @specs/mission.md (read it first —
scope, hypothesis, and explicit non-goals live there).

Spec-driven development via the `speq` skill (`/speq:plan`, `/speq:implement`, `/speq:record`).

## Performance

- **Stream, don't materialize.** Read DataFusion result batches one at a time and `ctx.emit` each
  immediately, then drop the batch before fetching the next. Never collect the whole result set.
  The emit buffer flushes at ~4,000,000 bytes — let it.
- **Resolve metadata once per query, not once per node.** N nodes must not trigger N catalog/snapshot
  fetches.
- **Push down before you scan.** Projection + filter + LIMIT must reach the Parquet scan; aggregate
  node-local where feasible so only partial results cross the wire to Exasol.
- **Shard by file, no overlap.** Each Exasol node scans only its assigned files (IPROC-aware).

## Boundaries

- **UDFs are stateless and disposable.** No caching, no metadata persistence, no cross-call state —
  every query starts from source metadata. (PoC explicitly excludes all of that; see mission.)
- **Only SDK `Value` types cross the `.so` boundary** — never Arrow types (the `.so` links its own
  arrow copy with different `TypeId`s). Convert Arrow → `Value` inside the UDF before emitting.
- **VS stays thin.** Translation, pushdown analysis, parallelization planning, result schema mapping
  only. Execution logic lives in DataFusion.

## Build / Test

- Build the UDF `.so` only inside `rust:1.92-bookworm` (glibc 2.36, matches the SLC) via
  `make cross-musl-udf-build`. **Never `cargo build --release` on the host** — it writes a
  host-glibc `.so` that fails to load in Exasol. Host `cargo test` (debug) is fine.
- E2E tests run against a local Exasol Docker container and must **fail** (not skip) if unavailable.
- All DSN/connection strings include `validateservercertificate=0` (self-signed Docker cert).

## Convergence

Long run: this likely converges with `strata-rs` (sibling project, possibly a monorepo). Mirror its
UDF programming model, workspace layout, and Makefile/E2E conventions rather than inventing new ones.
