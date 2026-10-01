# Tasks: fix-string-fn-type-coercion-udf

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [x] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 1: Reproduce (Group R: Reproduce #227)
- [x] 1.1 Add the #227 E2E tests to `tests/e2e_capability_test.rs` (plan task 1.1)
- [x] 1.2 Run each new test against Docker Exasol before any fix; record failures here as reproduction evidence
  Reproduced against Exasol 2025.1.16 (Docker) with the pre-fix `.so` from `make cross-udf-build`. All 10 tests fail pre-fix; every native oracle assertion passed first.
  - `e2e_group_by_upper_integer_key_matches_native`: `GROUP BY UPPER(ID)` -> SQL state `22002`, `F-UDF-CL-RUST-9001: ... grouped partial aggregate SQL error: Error during planning: Function 'upper' requires String, but received Int64 ... No function matches the given name and argument types 'upper(Int64)'`.
  - `e2e_aggregate_over_upper_integer_matches_native`: `SELECT MAX(UPPER(ID)), MIN(UPPER(ID)), SUM(LENGTH(UPPER(ID)))` -> `22002`, `F-UDF-CL-RUST-9001: ... partial aggregate SQL error: ... Function 'upper' requires String, but received Int64`.
  - `e2e_grouped_having_and_order_by_over_upper_integer_push_down`: `SELECT UPPER(ID), MAX(UPPER(ID)) ... GROUP BY UPPER(ID) HAVING MAX(UPPER(ID)) < '3' ORDER BY UPPER(ID)` -> `22002`, `F-UDF-CL-RUST-9001: ... grouped partial aggregate SQL error: ... Function 'upper' requires String, but received Int64`. Exasol's pushdown request carried `groupBy`, `having`, `orderBy`. Shape note: `HAVING MIN(UPPER(ID)) < '3'` with only `COUNT(*)` selected does NOT reproduce; the adapter routes it to the GroupByWrapper (scan projects only `ID`, Exasol evaluates UPPER natively) and it passes.
  - `e2e_scalar_over_aggregate_of_upper_integer_matches_native`: `SELECT C_BOOL, MAX(UPPER(ID)) || '-' || MIN(UPPER(ID)) ... GROUP BY C_BOOL` -> `22002`, `F-UDF-CL-RUST-9001: ... grouped partial aggregate SQL error: ... Function 'upper' requires String, but received Int64`.
  - `e2e_group_by_decimal_cast_key_trims_like_native`: wrong value. VS keys `NULL, "10.50", "20.25", "30.00", "40.99", "50.00", "60.00"`; native `NULL, "10.5", "20.25", "30", "40.99", "50", "60"`.
  - `e2e_max_over_decimal_cast_trims_like_native`: wrong value. VS `MAX="60.00", MIN="10.50"`; native `"60"`, `"10.5"`.
  - `e2e_group_by_upper_double_key_matches_native`: `GROUP BY UPPER(C_DOUBLE)` -> `22002`, `F-UDF-CL-RUST-9001: ... grouped partial aggregate SQL error: ... Function 'upper' requires String, but received Float64`.
  - `e2e_instr_three_args_on_grouped_paths_matches_native`: `SELECT INSTR(C_NAME,'c',2), SUM(C_CUSTKEY) FROM dim_customer GROUP BY INSTR(C_NAME,'c',2)` -> wrong value. VS group key `1`, native `0` (start position dropped; first failing variant, the four-argument and `'e', 8` variants follow in the same test).
  - `e2e_max_instr_with_start_position_matches_native`: wrong value. VS `MAX(INSTR(C_NAME,'c',2)) = 1`, native `0`.
  - `e2e_ungrouped_aggregate_all_files_pruned_returns_one_row`: wrong shape. VS returns zero rows, native returns one row `(NULL, 0)`.
- [ ] 1.3 Issue creation SKIPPED: CLAUDE.md forbids agents filing issues. `#TBD` stays in the spec deltas; human follow-up (Float64 value range issue, pruned-aggregate issue)

## Phase 2: Renderer string conversion (Group A)
- [x] 2.1 Add `EXA_TO_VARCHAR_FN` to `crates/vs-expression/src/lib.rs`
- [x] 2.2 Add helper owning the string-converted argument table and string CAST rule, with tests
- [x] 2.3 DataFusion dialect: wrap string-converted args in `exa_to_varchar` (string-fn, CONCAT, INSTR/LOCATE arms)
- [x] 2.4 `render_cast`: `CAST(exa_to_varchar(x) AS VARCHAR)` in DataFusion dialect; boolean CASE branch Exasol-only
- [x] 2.5 INSTR/LOCATE beyond two args is a DataFusion render error
- [x] 2.6 Update `lib_tests.rs` expectations; gate `cargo test -p vs-expression`

## Phase 3: exa_to_varchar session function (Group B)
- [x] 3.1 Create `scan/to_varchar.rs`, register in `build_session_context`
- [x] 3.2 `return_field_from_args` and `simplify`
- [x] 3.3 Dispatch: string, Null, integers, Decimal128, Boolean, Date32
- [x] 3.4 Timestamp conversion
- [x] 3.5 Float32/Float64 to Exasol DOUBLE text [expert]
- [x] 3.6 JSON-fallback conversion
- [x] 3.7 Move `boolean_to_string_casing_test.rs` into `to_varchar_tests.rs`
- [x] 3.8 Registration test and Float64 arithmetic test

## Phase 4: Adapter rewrite removal (Group C)
- [x] 4.1 Rewire `apply_type_rewrites` to `like_subject_type_guard` alone; delete dead functions and update doc comments
- [x] 4.2 Remove `decimal_to_varchar_exasol` arm and `format_decimal_exasol_style` from vs-expression
- [x] 4.3 Update pinned DataFusion-rendering expectations in lakehouse-engine tests
- [x] 4.4 Add adapter tests per Scenario Coverage; confirm `dispatch_golden` unedited

## Phase 5: Grouped and aggregate paths (Group D)
- [x] 5.1 Prove text-match sites agree under wrapping renderer; add surface tests [expert]
- [x] 5.2 Assert merge-wrapper SQL contains no `exa_to_varchar`
- [x] 5.3 INSTR beyond two args routes to GroupByWrapper / RowScan in `request_shape_tests.rs`
- [x] 5.4 `empty_result_sql`: one row for fully pruned ungrouped aggregate on RowScan

## Phase 6: Live verification (Group E)
- [x] 6.1 Add remaining E2E tests to `tests/e2e_capability_test.rs`
- [x] 6.2 Run `make test-e2e`; record `EXPLAIN VIRTUAL` evidence
  BLOCKED, not green. Stack: Docker Exasol 2025.1.16 (`lh444-*`), fresh `make cross-udf-build` .so. `make test-e2e` exit 2 (stops at the first failing binary). Re-run of every listed binary with `--no-fail-fast`:
  - Pass: complex_type 11, count_distinct 21, credential_exposure 12, direct_storage 33, emit_declaration 9, harness_row_cap 10, join 38, non_ascii_identifier 9, refresh 14, timestamp_precision 13, version_udf 9. `e2e_capability_test`: 95 passed, 2 failed (below); all 6 new group E tests and all 10 group 1 tests pass.
  - Spark-fixture tests (positional_deletes 19, int96 10, type_relaxation 10) failed first only because the `lh444` stack never ran `spark-iceberg-fixtures`; after running `scripts/spark-fixtures/run_fixtures.sh` in the `lakehouse-444` docker network they all pass.
  - `e2e_scan_test`: 82 passed, 1 failed, `adapter_detects_container_cpuset`: environmental, the host has 4 cores and the container cpuset `0-3` names 4 (its own precondition message). Not related to this change.
  - FAIL `e2e_scalar_over_aggregate_of_upper_integer_matches_native` (group 1 test): VS returns `NULL group -> "10-3"`, native `"3-10"`. Cause: the merge wrapper `EMITS` declares `"PARTIAL_max_0" DOUBLE PRECISION, "PARTIAL_min_1" DOUBLE PRECISION` for `MAX(UPPER(ID))`/`MIN(UPPER(ID))` nested in `CONCAT` (`scalar_over_agg.rs` `NESTED_AGGREGATE_PLAN_TYPE`), so the per-shard string partials are merged numerically (ids 3 and 10 across two shards). Group D/C gap, not fixed here.
  - FAIL `e2e_widened_projection_with_declined_order_by_routes_to_wrapper` (existing #234 test, unedited): asserts `LHS_T0` in the pushed SQL for `LENGTH(c_double)` / `LENGTH(score)`; those are now pushed into the scan (`_LH_PROJ_9` = `character_length(exa_to_varchar("C_DOUBLE"))`), so the qualified wrapper is not used. Row values still match the native oracle. The plan's "no assertion edit" premise conflicts with the new pushdown; needs a decision.
  EXPLAIN VIRTUAL evidence (test `e2e_grouped_string_conversion_repros_carry_exa_to_varchar_in_the_scan_spec`, passes): the scan spec carries `exa_to_varchar(` for `GROUP BY UPPER(ID)`; `MAX/MIN(UPPER(ID))` + `SUM(LENGTH(UPPER(ID)))`; `GROUP BY UPPER(ID) HAVING ... ORDER BY`; `MAX(UPPER(ID)) || '-' || MIN(UPPER(ID)) GROUP BY C_BOOL` (`"arg_expr":"upper(exa_to_varchar(\"ID\"))"`); `GROUP BY CAST(C_DECIMAL_A AS VARCHAR(20))`; `MAX/MIN(CAST(C_DECIMAL_A AS VARCHAR(20)))`; `GROUP BY UPPER(C_DOUBLE)`.
- [x] 6.3 Type a nested-only MIN/MAX partial by its argument, not the `DOUBLE PRECISION` default (`e2e_scalar_over_aggregate_of_upper_integer_matches_native`) [expert]
  Root cause confirmed live: a MIN/MAX reached only inside a scalar folded with no declared type, so `NESTED_AGGREGATE_PLAN_TYPE` typed its `EMITS` partial `DOUBLE PRECISION`; the per-shard text maxima `"3"`/`"10"` merged numerically (grouped `NULL -> "10-3"`). The same default broke other non-numeric arguments pre-fix: `MAX(UPPER(C_VARCHAR)) || '!'`, a CASE or GREATEST over `C_VARCHAR` (04000 type mismatch), `YEAR(MAX(CAST(C_TS AS DATE)))`, `DATE_TRUNC`, `TO_DATE` (0A000), a CASE over `C_DATE` (UDF emit error), `MAX(C_QTY > 3)` (`1!` vs `TRUE!`). Fix: `classify_typed_scalar_over_aggregate` types a nested MIN/MAX partial from its argument (character, temporal, boolean; numeric keeps the default) using `col_types`, threaded into `detect_group_by_aggregates` and `single_group_plan_types`; `fold_nested_aggregate_plan` never overrides a top-level declared type. Post-fix every variant above matches native on the live stack (the GREATEST case differs only because native Exasol returns garbage for `GREATEST(<VARCHAR(2000000) column>, 'b')`; the VS values are the correct maxima).
  Residual, not fixed: the all-files-pruned single-group path (`empty_result.rs::nested_absent_agg_type`) still types an absent expression-argument aggregate `DOUBLE PRECISION`, so `SELECT YEAR(MAX(CAST(C_TS AS DATE))) ... WHERE C_VARCHAR = 'zz'` fails with 0A000 (native returns one NULL row). Fixing it changes the pinned expected string of the existing test `empty_path_resolves_same_shape_for_string_fn_aggregates` (`CAST(NULL AS DOUBLE PRECISION)` -> `CAST(NULL AS VARCHAR(2000000))` for `LENGTH(MAX(UPPER(C_CUSTKEY)))`), which this task's brief forbids; left for a decision.
- [x] 6.4 Replace the declining expression in `e2e_widened_projection_with_declined_order_by_routes_to_wrapper` (#234), since `LENGTH(c_double)` now pushes down
  Assertion edit, disclosed: block 1 select item `LENGTH(c_double)` -> `INSTR(c_double, '5', 2)`, oracle `SELECT LENGTH(CAST({d} AS DOUBLE))` -> `SELECT INSTR(CAST({d} AS DOUBLE), '5', 2)` (NULL row likewise); block 2 `LENGTH(score)` -> `INSTR(score, '5', 2)`, oracle `SELECT LENGTH(CAST({score} AS DOUBLE))` -> `SELECT INSTR(CAST({score} AS DOUBLE), '5', 2)`. Messages renamed to match. Unchanged: both `LHS_T0` assertions, the 10-column/3-row checks, `assert_typed_probe_prefix`, the id check, and the per-row native-oracle comparison. EXPLAIN VIRTUAL on the live stack: `LENGTH(c_double)`/`LENGTH(score)` push `character_length(exa_to_varchar(...))` as `_LH_PROJ_9` with no `LHS_T0`; `INSTR(..., '5', 2)` routes to `LHS_T0` in both blocks. Live values match native (`3, 3, NULL` and `0, 0, 2`).

## Phase 7: Verification
- [x] 7.1 Build, test, lint, format, `speq plan validate`
  Final run on the lh444 stack with the fix `.so`: `cargo fmt --check`, `cargo clippy --all-targets` clean; `cargo test --workspace --lib` 186 + 1379 + 155 pass; `speq plan validate` has only AND-step warnings. Full E2E, 15 of 16 binaries all green incl. `e2e_capability_test` 97/97; the lone failure is `e2e_scan_test::adapter_detects_container_cpuset`, environmental (host core count vs cpuset), unrelated.
- [x] 7.2 Close the pruned single-group residual from 6.2/6.3: `empty_result.rs` types an absent nested aggregate from `classify_typed_scalar_over_aggregate` (column type first, else the argument-derived partial type). Pinned `empty_path_resolves_same_shape_for_string_fn_aggregates` expectation changed `CAST(NULL AS DOUBLE PRECISION)` -> `CAST(NULL AS VARCHAR(2000000))` for `LENGTH(MAX(UPPER(C_CUSTKEY)))`. Live: `e2e_pruned_scalar_over_temporal_aggregate_returns_one_null_row` passes (pre-fix `0A000`).
- [x] 7.3 #234 assertion edit (6.4) kept: `LENGTH(c_double)` now pushes down, so the declining expression became `INSTR(..., '5', 2)`; `LHS_T0` assertions unchanged.
