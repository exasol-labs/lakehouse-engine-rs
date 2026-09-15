# Tasks: remove-emit-exa-types

## PR Lifecycle
- [x] resolved
- [x] implemented
- [x] version-bumped
- [ ] tested-green
- [ ] recorded
- [ ] pr-ready

## Phase 2: Implementation (Group A: SDK and SLC lockstep)
- [x] 1.1 Verify gh release v0.26.0 assets and cargo-exasol-udf 0.26.0
- [x] 1.2 Set exasol-udf-sdk / exasol-udf-macros to 0.26.0 in workspace deps, update Cargo.lock
- [x] 1.3 Confirm Makefile SLC_VERSION and install.sh resolve 0.26.0; install.test.sh still passes
- [x] 1.4 Replace hardcoded 0.21.0 in bench/run.sh; set BENCH_SLC_VERSION=0.26.0 in secrets.sh
- [x] 1.5 Add input_column/output_column_count/output_column to the three hand-written UdfContext impls

## Phase 2: Implementation (Group B: Declared column as emit authority)
- [x] 1.6 Live E2E gate: prove dynamic-EMITS output_column accessors populate before removal [expert]
- [x] 2.1 Add scan_fixture helper building Vec<ColumnInfo> + BatchCapturingCtx constructor
- [x] 2.2 Rewrite target_arrow_type to map ExaType to Arrow target [expert]
- [x] 2.3 Change coerce_batch_to_exa_types to take declared column metadata [expert]
- [x] 2.4 Change emit_stream/emit_one_batch to resolve declared columns from ctx, drop exa_types param [expert]
- [x] 2.5 Drop &spec.common.emit_exa_types arg at raw_scan.rs:62 and join_scan.rs:57
- [x] 2.6 Coerce partial-aggregate columns to declared ExaType before arrow_value_at (both paths) [expert]
- [x] 2.7 Rework emit_tests.rs coercion tests to sweep ExaType variants + error paths
- [x] 2.8 Add partial-aggregate unit tests for the four mismatches [expert]
- [x] 2.9 Update tests/micro_bench.rs consumer of coerce_batch_to_exa_types
- [x] 2.10 Set output columns on the 22 TestContext constructions under tests/

## Phase 2: Implementation (Group C: Field removal and adapter plumbing)
- [x] 3.1 Delete CommonScanSpec::emit_exa_types + Default init; replace spec_tests.rs test
- [x] 3.2 Delete field from five production construction sites; remove join_fan_out_scan_spec param
- [x] 3.3 Give build_qualified_single_table_fallback_sql explicit proj_types param, drop read-backs [expert]
- [x] 3.4 Update adapter unit tests constructing/reading the field
- [x] 3.5 Regenerate 11 golden dispatch fixtures + four inline expected-SQL literals

## Phase 2: Implementation (Group D: End-to-end proof)
- [x] 4.1 Add e2e_emit_declaration_test.rs covering value-correctness scenario
- [x] 4.2 Replace emit_exa_types e2e assertions with EMITS clause assertion; delete vacuous one
- [x] 4.3 Add e2e partial-aggregate case for AVG/STDDEV over non-double columns

## Phase 4: Code Review
- [x] 4.4 Review all changed files

## Phase 4: Review Fixes
- [x] 4.5 Pass the resolved target into coerce_column instead of re-deriving it per column
- [x] 4.6 Cast with `safe: false` in coerce_column and cover the overflow path on both emit paths [expert]
- [x] 4.7 Replace the debug_assert_eq! alignment guard in build_qualified_single_table_fallback_sql with a returned error
- [x] 4.8 Replace fan_out_spec/proj_types with a FanOutProjection struct and drop the too_many_arguments allow [expert]
- [x] 4.9 Hoist declared columns above the match in run_partial_aggregate and build the null row at its declared targets [expert]
- [x] 4.10 Extract the declared-column fixture builders into one shared scan test-support file
- [x] 4.11 Move micro_bench.rs helper definitions below the last use statement
- [x] 4.12 Add raw_scan_coerces_every_column_to_its_declared_output_type to scan_batch_loop.rs
- [x] 4.13 Drop the SLC version literal from the e2e_scan_test.rs harness comments
- [x] 4.14 Fail bench/run.sh when the SLC version pin lookup yields nothing
- [x] 4.15 Add a Makefile print-slc-version target and consume it from bench/run.sh
- [x] 4.16 Delete the hardcoded BENCH_SLC_VERSION from the generated bench/.env

## Phase 5: Verification
- [x] 5.1 Run automated checks (build, test, e2e, lint, format, specs) — build/test/e2e/lint/fmt green; `speq feature validate` fails only on pre-existing baseline errors in features this plan does not touch (0 errors on all 5 of this plan's own features)
- [x] 5.2 Scenario coverage audit — all 11 scenarios have a passing test; 2 tests carry a different name than the plan's table (noted in verification report), intent unchanged
- [x] 5.3 Manual verification — all 5 steps executed against the live Docker Exasol stack, all pass
