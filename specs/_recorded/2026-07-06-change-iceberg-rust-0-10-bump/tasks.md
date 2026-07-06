# Tasks: change-iceberg-rust-0-10-bump

## Phase 2: Implementation (Group A — research)
- [x] 2.1 API-diff research: diff 0.9.1 vs 0.10.0-rc.2 signatures against tag source for every call site; produce per-site change list [expert]

## Phase 2: Implementation (Group B — pin)
- [x] 2.2 Update workspace pins in `Cargo.toml [workspace.dependencies]` to git-tag triple; rewrite/remove arrow-57 split comment block; re-verify StorageFactory comment

## Phase 2: Implementation (Group C — production fixes)
- [x] 2.3 Fix `adapter/pushdown.rs` (catalog build, list_tables, load_table/LoadTableResult/TableMetadata, Table, scan()/plan_files(), FileScanTask accessors) [expert]
- [x] 2.4 Fix predicate + schema + identifier + type-mapping call sites: `iceberg_predicate.rs`, `tables.rs`, `mod.rs`, `types/mapping.rs` [expert]

## Phase 2: Implementation (Group D — test fixtures)
- [x] 2.5 Fix `tests/common/seed.rs` to feed arrow-58 into 0.10 writer stack; drop 57 aliases; update writer-builder API [expert]
- [x] 2.6 Rework `tests/tpch_loader.rs` to drop `tpchgen-arrow` and build arrow-58 batches from `tpchgen` core [expert]
- [x] 2.7 Clean dev-dependencies in `crates/lakehouse-engine/Cargo.toml`: remove 57 aliases + tpchgen-arrow; update rationale comments

## Phase 3: Verification
- [x] 3.1 Host unit tests green (`cargo test`) — 387 + 56 passed, 0 failed
- [x] 3.2 E2E tests green (`make test-e2e`) — 43 + 7 + 6 passed, 0 failed after 3.2a fix
- [x] 3.2a [FIX] Add `.runtime(iceberg::Runtime::try_current()?)` to `Table::builder()` in `pushdown.rs:2423`; rebuild `.so`; re-run e2e green [expert]
- [x] 3.3 Lint + format clean (`cargo clippy --all-targets` 0 warnings; `cargo fmt` clean after reformat)
- [x] 3.4 Verify single arrow-58 tree in `Cargo.lock` (only 58.3.0; no 57/59; tpchgen-arrow absent)
- [x] 3.5 Refresh stale version prose in `CLAUDE.md` + Cargo.toml comments (not specs); commit references `Closes #65`

## Phase 4: Code Review
- [x] 4.1 Review all changed files — 0 defects, behavior-preserving
