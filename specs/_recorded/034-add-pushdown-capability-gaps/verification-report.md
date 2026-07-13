# Verification Report: add-pushdown-capability-gaps

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Closes #104, #105, #106, #107. Advertises `FN_CAST`, `FN_NEG`, `FN_WEEK` (each backed by a verified `crates/vs-expression` translator arm, confirmed end-to-end against a live Exasol stack); every other candidate function (`FN_DIV`, `FN_TO_CHAR`, `FN_TO_NUMBER`, the four regexp scalars, and 15 of the 16 #107 date functions) is deliberately left unadvertised with documented rationale. Live E2E testing surfaced and fixed a real CAST dispatch-shape bug (see Notes) before it could ship. |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace`) | 74 (`vs-expression`) + relevant `lakehouse-engine` unit tests (incl. 9 capability tests) | all | 0 |
| Integration/E2E (`make test-e2e`, full 5-file suite) | 83 | 83 | 0 |

E2E breakdown (live Exasol + MinIO + Iceberg REST stack, `--test-threads=1`):

| Test file | Passed | Failed |
|-----------|--------|--------|
| `e2e_capability_test.rs` | 11 | 0 |
| `e2e_count_distinct_test.rs` | 6 | 0 |
| `e2e_join_test.rs` | 10 | 0 |
| `e2e_positional_deletes_test.rs` | 11 | 0 |
| `e2e_scan_test.rs` | 45 | 0 |
| **Total** | **83** | **0** |

### Manual Tests

| Test | Result |
|------|--------|
| `cargo test -p vs-expression cast` (faithful-target and fall-through CAST tests) | ✓ |
| `cargo test -p lakehouse-engine capabilities` (FN_CAST/FN_NEG/FN_WEEK present, excluded names absent) | ✓ |
| `make cross-musl-udf-build && make test-e2e` — `SELECT WEEK(event_date), CAST(id AS VARCHAR(2000000)), -score FROM MY_LAKEHOUSE.EVENTS` pushes down via the DataFusion scan path | ✓ (covered by `e2e_cast_in_filter`, `e2e_unary_minus_in_filter`, `e2e_week_in_filter`) |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --workspace
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 3.67s
(no warnings or errors)
```

### Formatter

```
cargo fmt --check
(no output — clean)
```

## Scenario Coverage

| Domain / Feature | Scenario | Test Location | Test Name | Passes |
|---|---|---|---|---|
| sql-comprehension/vs-expression-translator-scalar-ops | Arithmetic operators translate to binary SQL expressions (CHANGED) | `crates/vs-expression/src/lib.rs` | `renders_arithmetic_div` (op set), `renders_arithmetic_neg`, `neg_composes_with_aggregate_decomposition` | Pass |
| sql-comprehension/vs-expression-translator-scalar-ops | CAST translates to DataFusion CAST syntax (CHANGED) | `crates/vs-expression/src/lib.rs` | `renders_cast_varchar`, `renders_cast_decimal`, `renders_cast_double`, `renders_cast_date`, `renders_cast_char_as_varchar`, `renders_cast_boolean`, `renders_cast_timestamp_without_local_time_zone`, `cast_to_unsupported_target_falls_back`, `renders_cast_nested_function_scalar_defensive` | Pass |
| sql-comprehension/vs-expression-translator-scalar-ops | Integer division DIV is deliberately not translated (NEW) | `crates/vs-expression/src/lib.rs` | `div_falls_through_as_unsupported` | Pass |
| sql-comprehension/vs-expression-translator-scalar-ops | Conversion format functions TO_CHAR and TO_NUMBER are deliberately not translated (NEW) | `crates/vs-expression/src/lib.rs` | `to_char_and_to_number_fall_through_as_unsupported` | Pass |
| sql-comprehension/vs-expression-translator-scalar-fns | Regexp scalar functions are deliberately not translated (NEW) | `crates/vs-expression/src/lib.rs` | `regexp_scalar_functions_fall_through`, `regexp_scalar_exclusion_leaves_regexp_like_untouched` | Pass |
| sql-comprehension/vs-expression-translator-date-fns | WEEK translates to the DataFusion date_part('week') ISO-8601 call (NEW) | `crates/vs-expression/src/lib.rs` | `renders_week_as_iso_date_part`, `renders_week_at_year_boundary_dates`, `week_with_wrong_arity_falls_back` | Pass |
| sql-comprehension/vs-expression-translator-date-fns | Unsupported date functions fall through as unsupported nodes (CHANGED) | `crates/vs-expression/src/lib.rs` | `unsupported_date_fn_falls_through` | Pass |
| vs-adapter/pushdown-planning-capability-extensions | Conversion and unary-negation capabilities are advertised (NEW) | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` | Pass |
| vs-adapter/pushdown-planning-capability-extensions | ISO week capability is advertised (NEW) | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` | Pass |
| vs-adapter/pushdown-planning-capability-extensions | Regexp scalar function capabilities remain absent (NEW) | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` | Pass |
| vs-adapter/pushdown-planning-capability-extensions | No new join/cross-join capability introduced (NEW) | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `cast_neg_week_introduce_no_join_capability` | Pass |
| vs-adapter/pushdown-planning-capability-extensions | Advertised CAST/NEG/WEEK execute end-to-end (NEW) | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_cast_in_filter`, `e2e_unary_minus_in_filter`, `e2e_week_in_filter` | Pass |

## Notes

- **Real bug found and fixed during implementation (task 2.10):** the original CAST translator arm (task 2.1) matched Exasol's engine-source dispatch by assumption rather than verification, nesting CAST inside the generic `function_scalar` name-match arm. Live E2E testing (task 2.9) proved Exasol actually sends CAST as its own top-level node type, `function_scalar_cast` (confirmed against the Exasol engine source, `Compiler/src/querygraph/scalar/qecast.cpp`), matching the same family pattern as `function_scalar_extract`/`function_scalar_case`. The nested arm silently declined every real CAST predicate (swallowed by `render_df_filter_safe`'s `.ok()??`, so it degraded to a correctness-safe fallback rather than an error) — this never surfaced before because `FN_CAST` was never advertised until this plan. Fixed by adding a `function_scalar_cast` top-level dispatch arm (the real path) and keeping the nested arm defensively, mirroring the existing `REGEXP_LIKE` dual-encoding precedent — both paths share one `render_cast` helper, so the target-type exclusion list cannot drift between them. Recorded as decision-log entry [8], including the audit-gap lesson for future capability work: verify dispatch **node-type shape** against the engine serializer, not just type-mapping.
- **Code review** (`speq:code-reviewer`) found one real defect (swapped `#104`/`#105` issue references in `capabilities.rs` comments) and three minor comment/test-duplication nits; all four fixed and re-verified green (task 4.2). The reviewer separately confirmed, on request, that the CAST exclusion list cannot drift between the two dispatch arms (shared helper) and that the defensive nested arm is not dead code (exercised by its own test, same precedent as `REGEXP_LIKE`).
- Per the plan's decision-log entry [7], the project's Iceberg-spec compliance gate (CLAUDE.md) does not apply to this plan — these are Exasol SQL-expression-pushdown capabilities (VS-layer function translation), not Iceberg file-format or schema/type handling.
- No dead code removed (plan adds capability advertisements, translator coverage, and tests only).
