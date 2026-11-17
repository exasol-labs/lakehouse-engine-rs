# Verification Report: add-clock-function-pushdown

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The five clock functions and compared `TIMESTAMP WITH LOCAL TIME ZONE` literals fold into plain literals and prune files; every check passes on Exasol 8.29.13 and 2025.1.16 |
| Code review | 6 findings — 6 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Unit | not measured |
| Integration | not measured |

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit | 2034 | 2034 | 0 |
| Integration (local E2E, Exasol 2025.1.16, final build) | 407 | 407 | 0 |
| Integration (local E2E, Exasol 8.29.13, build before the review fixes) | 407 | 407 | 0 |
| Integration (local E2E, Exasol 8.29.13, final build, `clock::`, `e2e_clock_projection`, `order_by_constant::`, `topn`) | 17 | 17 | 0 |

### Manual Tests

Run against the local stack on Exasol 2025.1.16.

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.EVENTS WHERE EVENT_DATE = CURRENT_DATE` | ✓ pushed SQL `SELECT CAST(NULL AS DECIMAL(20,0)) AS "ID" FROM DUAL WHERE 1=0`, no `CURRENT_DATE` |
| `EXPLAIN VIRTUAL ... WHERE EVENT_TS > ADD_HOURS(CURRENT_TIMESTAMP, -1)` | ✓ pushed SQL prunes to `WHERE 1=0`, no `SESSIONTIMEZONE` |
| `SET SESSION_TIME_ZONE = 'ASIA/TOKYO'`, then `SELECT CURRENT_DATE, LOCALTIMESTAMP FROM MY_LAKEHOUSE.EVENTS LIMIT 1` | ✓ `2026-11-18`, `2026-11-18 05:24:52.578`, Tokyo's date and wall clock (native `CONVERT_TZ(SYSTIMESTAMP, DBTIMEZONE, 'ASIA/TOKYO')` was `05:24:53.237`), no `REFRESH` |
| `SET SESSION_TIME_ZONE = 'MARS/OLYMPUS'` | ✓ fails with `invalid value 'MARS/OLYMPUS' for 'SESSION_TIME_ZONE': expected a time zone name from EXA_TIME_ZONES` |
| `SELECT CURRENT_TIMESTAMP FROM MY_LAKEHOUSE.EVENTS WHERE id = 1` | ✓ `21:24:41.622`, native `SELECT CURRENT_TIMESTAMP` `21:24:41.846` |
| `SET SESSION_TIME_ZONE = 'europe/berlin'` | ✓ succeeds; `SET SESSION_TIME_ZONE = null` then removes the property |

## Tool Evidence

### Linter

```
make lint: exit 0
    Checking lakehouse-engine v0.58.5 (/home/crusty/code/lakehouse-engine-rs-2/crates/lakehouse-engine)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 41.35s
```

### Formatter

```
make fmt-check: exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown | pushdown-planning-clock-functions | The five clock functions are advertised | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `clock_functions_are_advertised_and_withheld_by_exclusion` | Pass |
| pushdown | pushdown-planning-clock-functions | A clock function reaches the pushed SQL as a literal | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `each_clock_function_reaches_the_pushed_sql_as_a_literal` | Pass |
| pushdown | pushdown-planning-clock-functions | A TIMESTAMP WITH LOCAL TIME ZONE literal in a comparison runs in the scan | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_tstz_literal_comparison_runs_in_the_scan` | Pass |
| pushdown | pushdown-planning-clock-functions | The clock functions fold in the zone that Exasol uses when the session zone matches | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `clock_functions_match_native_when_the_session_zone_matches` | Pass |
| pushdown | pushdown-planning-clock-functions | SESSION_TIME_ZONE sets the zone of the session-zone functions | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `session_time_zone_sets_the_zone_of_the_session_zone_functions` | Pass |
| pushdown | pushdown-planning-clock-functions | A session zone that differs from the folding zone gets the folding zone's values | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_differing_session_zone_gets_the_folding_zone_values` | Pass |
| pushdown | pushdown-planning-clock-functions | A projected clock function keeps the aggregate split | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_projected_clock_function_keeps_the_aggregate_split` | Pass |
| pushdown | pushdown-planning-clock-functions | Every clock function of a statement folds from one reading | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `every_clock_function_of_a_statement_folds_from_one_reading` | Pass |
| pushdown | pushdown-planning-clock-functions | A filter on a clock function or a time zone literal prunes files at plan time | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `clock_and_tstz_filters_prune_files` | Pass |
| pushdown | pushdown-planning-clock-functions | A numeric database time zone folds at its offset | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_numeric_tz_folds_at_its_offset` | Pass |
| pushdown | pushdown-planning-clock-functions | An unreadable TZ fails only a query that needs it | `crates/lakehouse-e2e/tests/e2e_local/clock.rs`; `crates/lakehouse-engine/src/adapter/time_zone_tests.rs` | `an_unreadable_tz_fails_only_a_query_that_needs_it`; `unreadable_tz_is_refused` (unset case) | Pass |
| pushdown | pushdown-planning-clock-functions | SESSION_TIME_ZONE accepts the zone names Exasol lists and rejects other values | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `session_time_zone_accepts_exasol_zone_names_and_rejects_others` | Pass |
| pushdown-capabilities | pushdown-planning-capability-extensions | Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_projected_tstz_literal_matches_native_and_pruned_scan_succeeds` | Pass |
| pushdown-capabilities | pushdown-planning-capability-extensions | Routed session-dependent projection returns the value Exasol computes natively | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_clock_projection_matches_native_session_local_value` | Pass |
| pushdown-aggregates | pushdown-planning-single-group-agg | A single-group request that does not decompose routes to the qualified wrapper | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `iceberg_single_group_aggregates_match_the_oracle` | Pass |
| sql-comprehension | vs-expression-translator-literals | Literal nodes translate to SQL literal forms | `crates/vs-expression/src/lib_tests.rs` | existing literal rendering tests | Pass |
| vs-adapter | create-virtual-schema | Adapter reports its pushdown capabilities | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `reports_audited_capability_set` | Pass |
| vs-properties | property-name-validation | Every property the adapter reads is known under every catalog kind | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` | Pass |

## Notes

- The property is `SESSION_TIME_ZONE`, not the planned `TIME_ZONE`: Exasol parses `ALTER VIRTUAL SCHEMA ... SET TIME_ZONE` as its own syntax and refuses a quoted property name (verified on 8.29.13). The user chose the name.
- Captured request shapes differ by version: Exasol 2025.1.16 sends each timestamp function a precision argument and types `LOCALTIMESTAMP` `TIMESTAMP`; Exasol 8.29.13 sends no argument and types `LOCALTIMESTAMP` `TIMESTAMP WITH LOCAL TIME ZONE`. The fold decides by the declared type and the precision argument, and the spec deltas state both versions.
- A constant beside an aggregate in a single-group select list now keeps the partial/merge split (`SELECT 'X', COUNT(*)`), which the fold needs for `SELECT SYSDATE, COUNT(*)`. The pruning shape table's expectation for that query changed from wrapper to split.
- The `EXA_TIME_ZONES` names of both versions together equal the `chrono-tz` 0.10.4 zone names, so no embedded list is shipped; `every_time_zone_value_is_an_exa_time_zones_name` fails on a `chrono-tz` upgrade that adds a zone Exasol lacks.
- The remote suites were not run: the change targets no Lakekeeper, Unity Catalog, Glue, or Azure behavior.
- The local stack of this clone was left on Exasol 2025.1.16; it ran 8.29.13 before.
