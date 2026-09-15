# Code Review Findings: remove-emit-exa-types

## Summary
- Files reviewed: 53
- Total findings: 11 (standard: 8, expert: 3)

## Standard fixes

### crates/lakehouse-engine/src/scan/emit.rs

#### [SHRINKABLE] Every column's target Arrow type is derived twice
- Location: lines 145-148 and 185-192
- Issue: `coerce_batch_to_exa_types` builds `targets: Vec<DataType>` by calling `target_arrow_type` for every declared column, uses it only for the fast-path equality check, and then in the rebuild loop calls `coerce_column`, which calls `target_arrow_type(declared)` a second time for the same column. The same decision is expressed in two places in one function, and the `ExaType::TimestampTz` arm allocates an `Arc<str>` on each derivation.
- Fix: In `crates/lakehouse-engine/src/scan/emit.rs`, change `coerce_column` to take the already-resolved target as a parameter — signature `pub(crate) fn coerce_column(column: &ArrayRef, declared: &ColumnInfo, target: &DataType) -> Result<ArrayRef, UdfError>` — and drop the `target_arrow_type` call from its body. In `coerce_batch_to_exa_types`, zip `&targets` into the rebuild loop and pass `target` through. In `crates/lakehouse-engine/src/scan/partial_agg.rs`, have `coerce_partial_agg_columns` resolve the target once per aggregate column via the re-exported `target_arrow_type` (make it `pub(crate)`) and pass it to `coerce_column`.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [TACTICAL_SHORTCUT] A load-bearing alignment invariant is guarded only by a `debug_assert`
- Location: lines 1014-1020
- Issue: `build_qualified_single_table_fallback_sql` guards the "`proj_types` is positionally aligned with `fan_out_spec.common.projection`" precondition with `debug_assert_eq!`. `make cross-udf-build` and every release build compile that out, so the only build that runs inside Exasol carries no check at all. The function already returns `Result<String, UdfError>`, so a real guard costs nothing. The assertion message also claims the mismatch "silently truncates … the EMITS clause", which is now stale: the new emit-boundary arity check in `scan/emit.rs` turns a truncated EMITS clause into a named runtime failure.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, replace the `debug_assert_eq!` in `build_qualified_single_table_fallback_sql` with a guard clause returning `Err(UdfError::User(...))` that names both lengths, placed as the first statement of the function body (above the `// ONE leg, onto which …` comment, which describes the `JoinLegs::for_single_scan` call that follows it, not the assertion).

### crates/lakehouse-engine/src/scan/emit_tests.rs

#### [SHRINKABLE] The declared-column fixture builder is copied into four files
- Location: `emit_tests.rs` lines 17-45; `partial_agg_tests.rs` lines 600-625; `tests/scan_fixture/mod.rs` lines 33-70; `tests/micro_bench.rs` lines 45-66
- Issue: `declared(...)`, `numeric(p, s)` and `varchar()` now exist in four near-identical copies. The two copies under `src/scan/` are sibling `_tests.rs` files of the same module, which CLAUDE.md § Unit test layout says must share one helper file declared with `#[path]`. The copies have already drifted: `emit_tests.rs::declared` leaves `ColumnInfo.precision`/`scale` at `None` even for `ExaType::Numeric`, while `scan_fixture::output_columns` populates them, so the two fixtures model the same SDK type differently. `partial_agg_tests.rs` also parks its `use` items at line 600, mid-file, rather than at the top.
- Fix: Create `crates/lakehouse-engine/src/scan/declared_columns_test_support_tests.rs` holding one `pub(super) fn declared(types: &[(&str, ExaType)]) -> Vec<ColumnInfo>` (populating `size`/`precision`/`scale` from the `ExaType` payload, as `scan_fixture::output_columns` does), plus `numeric(precision, scale)` and `varchar()`. Declare it once with `#[cfg(test)] #[path = "declared_columns_test_support_tests.rs"] mod declared_columns_test_support;` alongside the existing `mod tests;` declarations in `crates/lakehouse-engine/src/scan/emit.rs`. Delete the local copies from `emit_tests.rs` and `partial_agg_tests.rs`, import the shared ones, and move `partial_agg_tests.rs`'s `use` items from line 600 to the top of the file. Leave `tests/scan_fixture/mod.rs` and `tests/micro_bench.rs` alone — they are separate test binaries.

### crates/lakehouse-engine/tests/micro_bench.rs

#### [SHRINKABLE] Helper functions are wedged between the file's `use` items
- Location: lines 44-69
- Issue: `declared` and `numeric` were inserted between `use lakehouse_engine::scan::emit::coerce_batch_to_exa_types;` (line 44) and `use lakehouse_engine::scan::spec::{…};` (line 69), splitting the import block in two.
- Fix: In `crates/lakehouse-engine/tests/micro_bench.rs`, move the `declared` and `numeric` function definitions below the last `use` statement so the import block stays contiguous.

### crates/lakehouse-engine/tests/scan_batch_loop.rs

#### [MISSING_BOUNDARY_TEST] The planned integration test for the coercion scenario does not exist
- Location: file-wide
- Issue: `plan.md` § Scenario Coverage maps "Output columns are coerced to the Arrow type the declared EMITS ExaType requires before emit_batch" to an Integration test named `raw_scan_coerces_every_column_to_its_declared_output_type` in this file. A repo-wide search finds no such test. The existing tests here assert row counts and byte-identity; none asserts that the batch that reaches `emit_batch` carries the Arrow type the declared `ExaType` requires. The scenario therefore has unit coverage only, and the coercion is never exercised through the real `run_raw_scan_with_session` path.
- Fix: In `crates/lakehouse-engine/tests/scan_batch_loop.rs`, add `#[test] fn raw_scan_coerces_every_column_to_its_declared_output_type()`. Drive `run_one_row` over `spec_for_file` with a declared list that forces a cast on both columns (`scan_fixture::decimal(20, 0)` for the Int64 `ID`, `scan_fixture::varchar()` for the string `NAME`), then assert the captured batch's schema reports `DataType::Decimal128(20, 0)` for `ID` and `DataType::Utf8` for `NAME`, and that the `ID` values round-trip unchanged.

### crates/lakehouse-engine/tests/e2e_scan_test.rs

#### [OUTDATED_COMMENT] The harness comments still name SLC 0.21.0
- Location: lines 11 and 79
- Issue: `//! 2. Install SLC 0.21.0 (LHRUST alias) …` and `// 3. Install SLC 0.21.0 (download + upload + ALTER SYSTEM).` both name a version this plan just moved off. `install_slc()` derives the version from the workspace `exasol-udf-sdk` pin (now 0.26.0), so the literal is both wrong and structurally wrong — naming any version here goes stale on the next bump.
- Fix: In `crates/lakehouse-engine/tests/e2e_scan_test.rs`, change both comments at lines 11 and 79 to name no version, e.g. "Install the Rust SLC pinned by the workspace `exasol-udf-sdk` version (LHRUST alias) …".

### bench/run.sh

#### [SWALLOWED_ERROR] A failed pin lookup yields an empty SLC version instead of an error
- Location: line 340
- Issue: `SLC_VERSION="${BENCH_SLC_VERSION:-$(sed -n 's/…/\1/p' Cargo.toml)}"` produces the empty string when the `sed` matches nothing (renamed dependency, reformatted manifest, changed layout). `set -euo pipefail` does not catch it: the command substitution exits 0 with no output. The run then continues and builds `lc-rust-.tar.gz` / the release URL `…/download/v/lc-rust-.tar.gz`, failing far downstream with an unrelated 404 instead of naming the real cause.
- Fix: In `bench/run.sh`, immediately after the `SLC_VERSION=` assignment at line 340, add a guard: `if [ -z "$SLC_VERSION" ]; then echo "bench/run.sh: could not read the exasol-udf-sdk version pin from Cargo.toml; set BENCH_SLC_VERSION explicitly" >&2; exit 1; fi`.

#### [INFORMATION_LEAKAGE] The Cargo.toml pin-parsing expression now lives in two files
- Location: `bench/run.sh` line 340 and `Makefile` line 129
- Issue: The `sed -n 's/^exasol-udf-sdk[[:space:]]*=.*version[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p' Cargo.toml` expression is copied verbatim from `Makefile:129` into `bench/run.sh:340`. How the SLC version is read out of the workspace manifest is one decision that now has to be edited in both files if the manifest layout changes — the same duplicate-transmission defect this plan removed from the scan spec. (`deploy/scripts/install.sh`'s `resolve_engine_pinned_slc_version` is a third encoding, but it reads a remote release tag and is out of scope here.)
- Fix: In `Makefile`, add a target `print-slc-version:` whose recipe is `@echo $(SLC_VERSION)`. In `bench/run.sh` line 340, replace the inline `sed` with `$(make -s print-slc-version)` so the Makefile stays the single owner of the derivation.

### deploy/scripts/secrets.sh

#### [DEAD_FLEXIBILITY] A hardcoded `BENCH_SLC_VERSION` re-creates the staleness just removed
- Location: line 49
- Issue: `secrets.sh` writes `BENCH_SLC_VERSION=0.26.0` into the generated `bench/.env`. `bench/run.sh` now derives that value from the workspace pin, and the generated override defeats that derivation — the literal was `0.21.0` and two releases stale before this plan corrected it, and it will go stale again at the next SDK bump. CLAUDE.md § Bench harness gotchas already records that a leftover `bench/.env` silently redirects a bench run; this line makes it silently pin a stale SLC too. The override knob itself stays available: `BENCH_SLC_VERSION` can still be exported per run.
- Fix: In `deploy/scripts/secrets.sh`, delete the `BENCH_SLC_VERSION=0.26.0` line at line 49 from the generated env file so `bench/run.sh`'s workspace-pin derivation applies, and leave `BENCH_SLC_VERSION` as an explicit per-run export only.

## Expert fixes

### crates/lakehouse-engine/src/scan/emit.rs

#### [SWALLOWED_ERROR] An unrepresentable value is silently turned into NULL by the coercion cast
- Location: line 193
- Issue: `coerce_column` calls `arrow::compute::cast`, which is `cast_with_options(array, to_type, &CastOptions::default())`, and `CastOptions::default()` is `safe: true` (verified in `arrow-cast-58.3.0/src/cast/mod.rs:87-94, 338-340`). Under `safe: true` a value that does not fit the target becomes NULL rather than an error. This change routes the partial-aggregate `Value` path through the same cast for the first time, so failures that previously surfaced as a wrong-typed value now surface as a silently wrong answer: a `SUM` over a wide decimal whose result exceeds the declared `DECIMAL(36,s)`, a `Decimal128`/`Int64` partial that exceeds an `Int64`-binned column, or an `AvgSum` above `Float64` range each emit NULL and the Exasol outer wrapper merges that NULL as if the shard had no data. The new test `partial_cells_conform_to_declared_output_columns` only exercises in-range values (`123_456_789_012_345` into `DECIMAL(36,2)`), so the overflow path is untested on both emit paths. A silent NULL contradicts § Design § Patterns' "Fail loudly on a missing declaration" and mission § Constraints' "returns a clean `ResourcesExhausted` error rather than OOM-crashing" posture.
- Fix: In `crates/lakehouse-engine/src/scan/emit.rs`, change `coerce_column` to call `arrow::compute::kernels::cast::cast_with_options(column.as_ref(), &target, &arrow::compute::CastOptions { safe: false, ..Default::default() })`, keeping the existing `map_err` that names the column, its source type and the target. Then add a unit test `coerce_fails_when_a_value_does_not_fit_its_declared_target` in `crates/lakehouse-engine/src/scan/emit_tests.rs` feeding a `Decimal128(38, 2)` value larger than `DECIMAL(36,2)` can hold into a `numeric(36, 2)` declaration, asserting the call fails naming the column and that no batch is emitted, and an equivalent test `partial_agg_fails_when_a_value_does_not_fit_its_declared_target` in `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` driving `partial_row_from_batch` with the same overflowing value. Run the full `cargo test -p lakehouse-engine` afterwards: `safe: false` also tightens the pre-existing raw-scan path, so any test that relied on a NULL-on-overflow cast must be reconciled rather than silenced.

### crates/lakehouse-engine/src/scan/partial_agg.rs

#### [UNTESTED_ERROR_PATH] The null-partial-row branch bypasses the declared-column conformance it now contradicts
- Location: lines 100-108 and 280-293
- Issue: `run_partial_aggregate` reads the declared columns and coerces the batch only inside the `Some(batch) if batch.num_rows() > 0` arm. The fallback arm calls `emit_null_partial_row`, which emits `Value::Int64(0)` for every counter column and `Value::Null` otherwise, with no `declared_output_columns` read, no `check_declared_arity`, and no coercion. The adapter declares counter columns `DECIMAL(20,0)` (`adapter/pushdown/grouped_agg.rs:686-689`), which Exasol reports as `ExaType::Numeric { precision: 20, scale: 0 }`, so after this change the populated arm emits `Value::Numeric` for exactly the column the empty arm emits `Value::Int64` for. The two arms of one function now disagree on the `Value` variant for the same declared column, and the SDK bump that motivates this plan is precisely the release that started validating that variant per column. No test covers the empty arm against a declared list, so which arm the SDK accepts is unverified. `run_grouped_partial_aggregate` by contrast reads the declared columns unconditionally before its loop, so the inconsistency is within the single-group path only.
- Fix: In `crates/lakehouse-engine/src/scan/partial_agg.rs`, hoist `let declared = declared_output_columns(ctx)?;` above the `match batches.first()` in `run_partial_aggregate` and pass `&declared` into `emit_null_partial_row`, changing its signature to `fn emit_null_partial_row(aggregates: &[AggregatePlan], declared: &[ColumnInfo]) -> Result<Vec<Value>, UdfError>`: call `check_declared_arity(declared, <total partial column count>)?` first, then for each counter column build the zero at the column's declared target (`Value::Numeric(Decimal { unscaled: 0, scale })` for `ExaType::Numeric`, `Value::Int64(0)` for `ExaType::Int64`, `Value::Int32(0)` for `ExaType::Int32`, `Value::Double(0.0)` for `ExaType::Double`, and a named `UdfError::User` for any other declaration) and `Value::Null` for the rest. Add a unit test `null_partial_row_conforms_to_declared_output_columns` in `crates/lakehouse-engine/src/scan/partial_agg_tests.rs` asserting a `COUNT` plus `SUM` plan under `[numeric(20, 0), ExaType::Double]` yields `[Value::Numeric(0, scale 0), Value::Null]`, and a second asserting a short declared list fails the call.

### crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs

#### [TOO_MANY_ARGUMENTS] The fallback builder reaches eight parameters and silences the lint
- Location: lines 1000-1010
- Issue: Adding `proj_types: &[String]` pushed `build_qualified_single_table_fallback_sql` to eight parameters, and the change suppresses the resulting warning with a newly added `#[allow(clippy::too_many_arguments)]` rather than resolving it — `cargo clippy --all-targets` is a 0-warning gate in this plan's own § Checklist, so the `allow` is the only thing keeping it green. The two new neighbours `fan_out_spec` and `proj_types` also carry an unenforceable positional-alignment contract between them (see the `[TACTICAL_SHORTCUT]` finding on the same function), which a single parameter would make unrepresentable.
- Fix: In `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`, introduce `pub(in super::super) struct FanOutProjection<'a> { pub spec: &'a ScanSpec, pub proj_types: &'a [String] }` with a constructor `fn new(spec: &'a ScanSpec, proj_types: &'a [String]) -> Result<Self, UdfError>` that rejects a length mismatch between `spec.common.projection` and `proj_types` with a `UdfError::User` naming both lengths. Replace the `fan_out_spec` and `proj_types` parameters of `build_qualified_single_table_fallback_sql` with one `fan_out: &FanOutProjection<'_>`, which brings the signature back to seven parameters — then delete the `#[allow(clippy::too_many_arguments)]` attribute and the `debug_assert_eq!` inside the body. Update all four call sites: `qualified_single_table_fallback_pushdown` in this file, `grouped_agg_tests.rs:2060`, and `sql_builders_tests.rs:2449`, `:2722`, `:2763`, `:3095`. Confirm `cargo clippy --all-targets` reports 0 warnings without the attribute.
