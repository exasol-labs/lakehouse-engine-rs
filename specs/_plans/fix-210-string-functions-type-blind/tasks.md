# Tasks: fix-210-string-functions-type-blind

## Phase 2: Implementation (Group A — pure argument table)
- [x] 1.1 Add `StringPositionArgs` enum + `string_position_args` table fn
- [x] 1.2 Unit-test the table (governed names, NotGoverned, LPAD/RPAD arity)
- [x] 1.3 Unit-test arity decline (INSTR/LOCATE > 2 args)

## Phase 2: Implementation (Group B — dispatch + recursive guard)
- [x] 2.1 Extract `wrap_cast_to_varchar` helper from `guard_like_subject`
- [x] 2.2 Add `coerce_string_position_arg`
- [x] 2.3 Add `string_function_arg_type_guard` (recursive, Option-returning) [expert]
- [x] 2.4 Extend `decimal_rewrite_col_types()` fixture (DOUBLE/BOOLEAN/TIMESTAMP)
- [x] 2.5 Unit-test the guard (coerce/decline/index-table/non-column/nesting)

## Phase 2: Implementation (Group C — WHERE-clause wiring)
- [x] 3.1 Wire guard into `handle_pushdown`'s filter chain (mod.rs)
- [x] 3.2 Update existing chain-reproducing tests for the new stage
- [x] 3.3 Add mod.rs tests (predicate_equal reach, decline, LIKE composition)

## Phase 2: Implementation (Group D — select-list + join projection wiring)
- [x] 4.1 Wire guard into `project_columns` (support.rs)
- [x] 4.2 Confirm existing wired `project_columns`/join tests stay byte-identical
- [x] 4.3 Add `project_columns` tests (coerce/decline/INSTR arity)
- [x] 4.4 Add `extract_join_projection` test (joins/rendering.rs)

## Phase 2: Implementation (Group E — E2E repro coverage)
- [x] 5.1 Add E2E repro tests (UPPER/LTRIM/LOWER/INSTR push through)
- [x] 5.2 Add type-decline E2E (UPPER(c_double)/c_ts/c_bool native oracle)
- [x] 5.3 Add arity-decline E2E (INSTR 3-arg, both surfaces)

## Phase 3: Verification (Group F)
- [x] 6.1 Run checklist, capture five repro rows — all 5 E2E repro/decline rows (5.1-5.3) verified
      live against the docker Exasol+MinIO+Iceberg-REST stack; additionally found and fixed two
      pre-existing bugs the live run exposed (not caught by unit tests, which only assert the
      returned Rust value shape, not live Exasol protocol validity):
      1. `project_columns`' decline fallback returned a full base-row projection whose column count
         mismatched the pushed selectList's item count, hard-failing with Exasol SQL state 04000.
         Fixed by routing the single-table row-scan decline through the existing
         `qualified_single_table_fallback_pushdown` wrapper (mod.rs), the same route already used
         for the GroupByWrapper/multi-DISTINCT decline cases.
      2. That wrapper's Exasol-dialect renderer (`vs-expression`) was dialect-blind for the whole
         string-function name-mapping table (INSTR/LOCATE→strpos, LENGTH→character_length,
         UNICODE→ascii, ...) — it only special-cased CAST length for Exasol. Fixed by rendering
         every governed string function verbatim (original name/arg order/count) under
         `Dialect::Exasol`, since that SQL is parsed by Exasol's own engine, not DataFusion.
      Full regression sweep (e2e_scan_test 52/52, e2e_join_test 15/15, e2e_count_distinct_test
      16/16, e2e_refresh_test 11/11) confirmed no regressions; see STATUS.md for the complete log.
