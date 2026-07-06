# Verification Report: change-iceberg-rust-0-10-bump

## Verdict: ✅ PASS

The iceberg-rust 0.9.1 → 0.10.0-rc.2 bump is complete and fully verified. The entire
workspace (production + dev/e2e) now resolves a **single arrow-58 tree**, all host unit
tests and the full e2e suite pass, and lint/format are clean. One runtime regression that
the compile-time API diff could not catch (`TableBuilder` gained a mandatory `.runtime()`)
was surfaced by the e2e gate and fixed.

## Summary of Changes

| Area | Change |
|------|--------|
| Workspace pins | `iceberg` / `iceberg-catalog-rest` / `iceberg-storage-opendal` → git tag `v0.10.0-rc.2` (commit `be6cc96e`) |
| Production | `pushdown.rs`: dropped `OpenDalStorageFactory::S3.configured_scheme` (2 sites); added `.runtime(iceberg::Runtime::try_current()?)` to `Table::builder()` |
| Test fixtures | `seed.rs` swapped to arrow-58 + field drop; `tpch_loader.rs` dropped `tpchgen-arrow`, hand-builds arrow-58 batches for all 8 TPC-H tables from `tpchgen` core |
| Dev-deps | Removed arrow-57 aliases (`ice_arrow_array`/`ice_arrow_schema`/`ice_parquet`) and `tpchgen-arrow`; comments refreshed |
| Arrow tree | Collapsed from two trees (57.3.1 + 58.3.0) to a single arrow-58 tree, prod + dev |

## Automated Checks (Phase 5a)

| Step | Command | Result |
|------|---------|--------|
| Unit tests | `cargo test` | ✅ 387 (lakehouse-engine) + 56 (vs-expression) + integration passed; **0 failed** |
| E2E — scan | `make test-e2e` | ✅ `e2e_scan_test`: 43 passed, 0 failed |
| E2E — capability | `make test-e2e` | ✅ `e2e_capability_test`: 7 passed, 0 failed |
| E2E — count distinct | `make test-e2e` | ✅ `e2e_count_distinct_test`: 6 passed, 0 failed |
| UDF `.so` build | `make cross-musl-udf-build` | ✅ Exit 0 (rust:1.94-bookworm) |
| Lint | `cargo clippy --all-targets` | ✅ 0 warnings |
| Format | `cargo fmt --check` | ✅ No diff (after one reformat of `tpch_loader.rs` macro calls) |

## Manual Testing (Phase 5c)

| Feature | Command | Result |
|---------|---------|--------|
| Single arrow-58 tree | `grep -A1 '^name = "arrow"' Cargo.lock` | ✅ Only `58.3.0` |
| No arrow 57/59 anywhere | `grep 'version = "5[79]\.' Cargo.lock` | ✅ No matches |
| tpchgen-arrow removed | `grep -c tpchgen-arrow Cargo.lock` | ✅ 0 |
| Tag resolves | `Cargo.lock` iceberg source | ✅ `git+…?tag=v0.10.0-rc.2#be6cc96e…` |

## Scenario Coverage Audit (Phase 5b)

Pure dependency bump — no new/changed scenarios. The existing suite is the regression oracle;
every affected call site is guarded by a test that passed **unchanged**:

| Behavior guarded | Test | Result |
|------------------|------|--------|
| Catalog build, list_tables, load_table, plan_files, FileScanTask | `adapter/pushdown.rs` unit (145) | ✅ |
| Predicate/Reference/Datum + spec::Schema | `adapter/iceberg_predicate.rs` (21) | ✅ |
| TableIdent/NamespaceIdent | `adapter/tables.rs` (11) | ✅ |
| spec::{Type,PrimitiveType} → Arrow/Exasol | `types/mapping.rs` (14) | ✅ |
| E2E scan/pushdown, writer stack + plan_files | `e2e_scan_test` (43) | ✅ |
| Capability negotiation e2e | `e2e_capability_test` (7) | ✅ |
| COUNT DISTINCT pushdown e2e | `e2e_count_distinct_test` (6) | ✅ |

## Code Review (Phase 4)

Independent review of all 6 changed files: **0 defects**. The hand-built TPC-H batches
reproduce `tpchgen-arrow` 2.0.2's semantics column-for-column (decimal scaling incl. the
`l_quantity * 100` special case, `Date32` via `to_unix_epoch()`, `Utf8` producing identical
bytes to the former `Utf8View`); the `configured_scheme` drop is clean at all 3 sites; scope
discipline held (no `ArrowReaderBuilder` touch); guardrails clean. The subsequent runtime fix
(4 lines, added after review) matches iceberg's own `load_table` pattern and is proven correct
by the green e2e run.

## Key Finding: Runtime-surfaced API change

iceberg 0.10 added a **mandatory** `Runtime` to `TableBuilder`. The field is `Option<Runtime>`
in the type (so the builder chain still type-checks unset) but `build()` returns
`DataInvalid => Runtime must be provided with TableBuilder.runtime()` at runtime. The
compile-only API diff correctly reported the chain as source-compatible but could not see the
runtime requirement. Caught by the e2e gate, fixed with `Runtime::try_current()`. Recorded in
`api-diff.md`. Lesson: compile-only diffing of `Option`-typed-but-required builder fields is
necessary but not sufficient — an e2e pass is required to catch them.

## Tracking

Implementing commit references `Closes #65` (cross-links #11). Delete-application work
(`ArrowReaderBuilder`) remains scoped to the separate #11 follow-on plan.

## Ready for: `/speq:record change-iceberg-rust-0-10-bump`
