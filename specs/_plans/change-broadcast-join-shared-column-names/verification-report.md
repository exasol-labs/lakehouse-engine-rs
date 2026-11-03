# Verification Report: change-broadcast-join-shared-column-names

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A two-leg inner equi-join whose legs share column names, or that joins a table to itself, takes the broadcast path. All host tests, the local E2E suite, lint, format, and the manual checks pass. |
| Code review | 6 findings, 6 fixed |

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
| Unit and integration (`cargo test --workspace`) | 2097 | 2097 | 2 |
| E2E (`make test-e2e`, local Exasol Docker) | 343 | 343 | 0 |

The 2 ignored tests are the existing micro-benchmarks in `tests/it/micro_bench.rs`. No remote suite ran: the plan targets no Lakekeeper, Unity Catalog, Glue, or Azure behavior.

The first E2E run during implementation had 13 failures (12 `refresh::*` and `scan::e2e_added_columns_initial_default_fill_all_types`). The Iceberg REST fixture's SQLite catalog returned `SQLITE_BUSY` on catalog writes, before any adapter or scan code ran. After `make e2e-down e2e-up`, the 13 `refresh::` tests passed alone, and the full suite then passed 343 of 343 on the final code.

### Manual Tests

All run against the Docker Exasol on `MY_LAKEHOUSE_JOIN` and `MY_LAKEHOUSE_JOIN_LOW`.

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL` of `EVENTS e JOIN LABELS l ON e.ID = l.ID`: scan-spec join block carries `fact_alias` `LHS_T0` and `dimension_alias` `LHS_T1`, the condition compares `"LHS_T0"."ID"` with `"LHS_T1"."ID"`, and `EMITS` starts with `"LHS_T0_ID" DECIMAL(20,0)` | ✓ |
| Self-join `FACT_ORDERS a JOIN FACT_ORDERS b ON a.O_CUSTKEY = b.O_CUSTKEY` on the default schema | ✓ 20 rows |
| The same self-join with `EXPLAIN VIRTUAL` on the low-threshold schema | ✓ no scan-spec join block (`fact_alias` count 0), pushed SQL carries `LHS_T0` and `LHS_T1` |
| `EXPLAIN VIRTUAL` of the `DIM_CUSTOMER`/`FACT_ORDERS` join with `WHERE o.O_CUSTKEY LIKE '1%'` | ✓ no scan-spec join block, N-scan wrapper with `LHS_T0` and `LHS_T1` |
| The same `LIKE` join, rows on the default and the low-threshold schema | ✓ both return `customer-01` with order keys 1 and 6 |

### Positional binding, live proof

`EXPLAIN VIRTUAL` of query (a) from `e2e_broadcast_join_over_shared_column_names_matches_native`, abbreviated:

```
SELECT "LHVS".LAKEHOUSE_SCAN('{"table_root":"s3://warehouse/e2e_lakehouse/events",
 "projection":[{"relation":"LHS_T0","name":"ID"},{"relation":"LHS_T0","name":"NAME"},
               {"relation":"LHS_T1","name":"ID"},{"relation":"LHS_T1","name":"LABEL"}],
 "join":{"fact_alias":"LHS_T0","dimension_alias":"LHS_T1",
   "condition":"(\"LHS_T0\".\"ID\" = \"LHS_T1\".\"ID\")", ...}, ...}', files)
 EMITS ("LHS_T0_ID" DECIMAL(20,0), "LHS_T0_NAME" VARCHAR(2000000),
        "LHS_T1_ID" DECIMAL(20,0), "LHS_T1_LABEL" VARCHAR(2000000))
```

Exasol returns the correct `e.ID, l.ID, e.NAME, l.LABEL` values although the `EMITS` names differ from the user's names, so Exasol binds the pushdown result by position.

## Tool Evidence

### Linter

```
make lint (cargo clippy --workspace --all-targets -D warnings): exit 0
```

### Formatter

```
make fmt-check: exit 0, no changes
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown-joins | pushdown-planning-join | Legs that share column names are planned as a broadcast fan-out | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_over_shared_column_names_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | Broadcast-eligible inner equi-join is planned as a broadcast fan-out | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_pushdown_shape` | Pass |
| pushdown-joins | pushdown-planning-join | Small-side selection uses table-format metadata and the broadcast threshold | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_above_threshold_unaccelerated_fallback_shape` | Pass |
| pushdown-joins | pushdown-planning-join | Broadcast join projection and filter are rendered per involved table | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_over_shared_column_names_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | Join condition is rendered via the vs-expression translator | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_over_shared_column_names_matches_native` | Pass |
| pushdown-joins | pushdown-planning-join | A bare LIMIT over a broadcast-eligible join is served by the broadcast path with a per-shard post-join cap | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_bare_limit_stays_broadcast_and_truncates` | Pass |
| scan-aggregation | scan-execution-join | Scan registers both tables and executes the inner equi-join | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_join_over_shared_column_names_matches_native` | Pass |
| scan-aggregation | scan-execution-join | Scan registers both tables and executes the inner equi-join | `crates/lakehouse-engine/tests/it/scan_join_test.rs` | `join_registers_each_side_against_its_own_backend` | Pass |
| pushdown-joins | pushdown-planning-join-fallback-self-join | A table joined to itself renders each occurrence as its own leg | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_self_join_on_primitive_column_matches_single_node` | Pass |
| pushdown-joins | pushdown-planning-join-fallback-self-join | A shared-column-name join on the fallback renders each column qualified by its leg | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_self_join_with_one_unaliased_occurrence_matches_single_node` | Pass |
| pushdown-joins | pushdown-planning-join-filter-type-coercion | A broadcast-join filter over a non-string LIKE subject declines the broadcast plan | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_like_on_decimal_column_falls_back_and_filters` | Pass |
| pushdown-joins | pushdown-planning-join-filter-type-coercion | A broadcast-join filter over a DATE LIKE subject keeps the broadcast plan | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_like_on_date_column_stays_broadcast_and_filters` | Pass |
| pushdown | pushdown-declined-filter-self-apply | A broadcast-eligible join whose filter declines takes the N-scan fallback | `crates/lakehouse-e2e/tests/e2e_local/join.rs` | `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` | Pass |
| pushdown-joins | pushdown-planning-join-fallback | A join outside the broadcast contract is declined safely | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders_tests.rs` | `join_outside_contract_declined_safely` | Pass |
| pushdown-types | pushdown-planning-like-type-coercion | A select-list LIKE over a non-string column widens the projection to the full base row | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `selectlist_like_over_non_string_subject_falls_back_to_full_row` | Pass |
| pushdown-types | pushdown-planning-string-fn-type-coercion | A non-coercible resolvable column type in a select-list string function falls back to the full base row | `crates/lakehouse-engine/src/adapter/pushdown/support_tests.rs` | `selectlist_string_fn_over_double_falls_back_to_full_row` | Pass |

Each class of column comparison has its own unit test with the same column name on both legs: `order_by_on_other_legs_same_named_column_declines_broadcast`, `shared_column_name_with_different_types_declares_each_legs_type`, `binary_detection_resolves_a_shared_column_name_per_leg` (all in `joins/sql_builders_tests.rs`), `output_names_of_distinct_pairs_differ` (`scan/spec_tests.rs`), and `build_join_sql_resolves_a_shared_column_name_per_relation` (`scan/join_scan_tests.rs`). The implementer mutated the code (bare-name binding, name-only sort-key binding, byte-counted identifier length, hard-coded join-block aliases, lenient `NoLeg` check) and confirmed each mutation fails its test.

## Notes

- **Bare-name comparison inventory, scan (task 1.2).** `combined_type`, `combined_upper_fields`, `render_join_sort_target`, and the column-type closure of `build_join_sql` are converted to (relation alias, column). `render_projection` and `render_column` are shared with the single-table `raw_scan.rs::build_scan_sql`, whose lowercase bare-name lookup never sees a join column, because a single-table spec carries no relation alias and the function does not read the join block.
- **Bare-name comparison inventory, adapter (tasks 2.1, 2.2).** Converted: `column_exa_type` and its callers (`guard_like_subject`, `is_bare_decimal_column`, `coerce_string_position_arg`, `timestamp_literal_floor`), `apply_type_rewrites`, `type_accepted_rewrite`, `classify_where_filter`, `project_columns`, `BinaryColumns::resolve` and `raw_bytes`, `bound_sort_key`, `parse_sort_key_element`, `ParsedSortKey::render_order_by_element`, `wrap_declined_order_by`. Never on the broadcast path: `type_screened_leg_filter` (N-scan legs only), `collect_all_column_names` and `referenced_column_projection` (single-table and N-scan wrappers), `detect_topn` (requires one involved table), `extend_projection_with_sort_keys` and `referenced_columns` (single-table declined wrapper), and the empty-side path of `plan_join` (wrapped unqualified, out of scope, #TBD). `ensure_no_side_refuses_a_referenced_column` stays keyed by `tableName`: a refused column is a property of the table, which a self-join's two occurrences share, and over-charging is the safe direction.
- **Identifier limit.** The live Exasol rejects a 129-character identifier (`maximum length of identifier exceeded`, SQL state 42000) and accepts 128. 128 and 129 two-byte characters (`Ä`) pass and fail the same way, so the limit counts characters. A projected column whose output name exceeds 128 characters declines the broadcast plan.
- **Added scan check.** `build_join_sql` also rejects an unqualified projection item or post-join sort key with a `UdfError::User`, a check the plan did not list. Guessing a bare name would pick the wrong relation of a shared column.
- **Not on the broadcast path.** `JoinLegs::qualify` is not idempotent on a self-join, so `render_broadcast_join` qualifies each clause once from the raw request, and the N-scan fallback receives the raw request. The doc comments of `qualify` and `qualify_every_column` state this. A `NoLeg` column is detected through `JoinLegs::qualify_every_column`, which resolves every node before `qualify` runs, because a `NoLeg` node can still carry Exasol's own `tableAlias`.
- **Spec delta wording.** The Backgrounds of the two type-coercion deltas named `extract_join_projection`, which `render_broadcast_join` replaced by calling `project_columns` directly. Both deltas now name `render_broadcast_join`.
- **Out of scope, unchanged.** The empty-side join result declares a type by bare name across both tables (#TBD).
- **No version bump.** The project hook skips the version bump.
