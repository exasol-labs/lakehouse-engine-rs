# Tasks: fix-createvs-cores-nodecount

## Phase 2: Implementation (Group A)
- [x] 2.1 Dependency bump: `exasol-udf-sdk`/`exasol-udf-macros` 0.19.1 → 0.20.0 in root `Cargo.toml` + `crates/lakehouse-engine/Cargo.toml` (keep `emit-arrow`); update stale SDK version line in `CLAUDE.md` to 0.20.0

## Phase 2: Implementation (Group B)
- [x] 2.2 Rewrite `resolve_cluster_nodes`: node count from `ctx.node_count()` (map 0→1), core count from `parse_nr_of_cores_override` else `available_parallelism()` (Err→0); drop connect-back closure + queries; update doc comment; preserve `(u32,u32)` signature + `parse_nr_of_cores_override` [expert]
- [x] 2.3 Delete dead code + stale comments: `PROP_CONNECTION_NAME`, `nproc_value_to_count`, `varchar_value_to_u32`, and all stale comments referencing connect-back topology/NPROC/PARAM_VALUE/CONNECTION_NAME; preserve credential path (`ctx.connection`, `connect_back::ConnectionObject` import)
- [x] 2.4 Update `NoopCtx` test double: configurable `node_count()` override to drive 0→1 and >1 paths [expert]

## Phase 2: Implementation (Group C)
- [x] 2.5 Rewrite affected unit tests + E2E doc comment to exercise new sources (see plan Task 5) [expert]

## Phase 2: Implementation (Group D)
- [x] 2.6 Zero-trace gate: rg for NPROC/PARAM_VALUE/CONNECTION_NAME/connect_back/session.query/dead helpers → 0 matches in crates/; confirm specs (non-_recorded) clean

## Phase 3: Verification
- [x] 3.1 Code review of changed files → 0 blocking findings, approved
- [x] 3.2 Build (`make cross-musl-udf-build`) → exit 0 (SDK 0.20.0, rust:1.92-bookworm)
- [x] 3.3 Test (`cargo test`) → 0 failures (312 lib + 53 vs-expression + integration, all ok)
- [x] 3.4 Lint (`cargo clippy --all-targets`) → 0 warnings
- [x] 3.5 Format (`cargo fmt --check`) → no changes
- [x] 3.6 E2E (`make test-e2e`) → green (7 capability + 28 scan = 35 passed, 0 failed; SLC 0.20.0 fingerprint matched)
