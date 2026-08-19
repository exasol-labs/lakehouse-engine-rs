# Verification Report: fix-join-fallback-self-join-attribution

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Self-join column attribution now resolves per join leg via `JoinLegs`, keyed on `(tableName, tableAlias)`, instead of colliding on bare `tableName`. Both issue #361 repro shapes (2-leg → 100 rows, 3-leg → 1000 rows over a 10-row table) were confirmed failing live before the fix and pass live after it. All unit and e2e suites are green. |
| Code review | 12 findings — standard: 9 fixed, expert: 3 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (subsumed by e2e — see Notes) |

## Test Evidence

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit + non-e2e integration (`cargo test --workspace`) | 1533 | 1533 | 0 |
| E2E (`make test-e2e`, Docker Exasol/MinIO/iceberg-rest) | 279 | 279 | 0 |

Both runs: 0 failed.

Key new tests, all passing:
- Unit: `attribution_tests.rs` (19 tests incl. `absent_alias_is_a_distinct_leg_key`, `attachment_leg_*` ×4), `sql_builders_tests.rs` (`self_join_renders_each_occurrence_as_its_own_leg`, `three_leg_self_join_attaches_each_condition_at_its_own_join_point`, `unattributable_column_reference_is_a_hard_error_naming_the_column`, `n_scan_wrapper_qualifies_every_clause_by_leg`, `conditions_attach_by_leg_set_and_leg_local_filters_partition_exactly`, `a_leg_count_disagreeing_with_the_resolved_sides_declines_naming_both_counts`), `rendering_tests.rs` (`leg_local_conjunct_reaches_only_its_own_occurrence_leg`), `planning_tests.rs` (`self_join_is_never_broadcast_eligible`).
- E2E (`e2e_join_test.rs`): `e2e_self_join_on_primitive_column_matches_single_node`, `e2e_self_join_with_one_unaliased_occurrence_matches_single_node`, `e2e_three_leg_self_join_matches_single_node`, `e2e_self_join_with_one_sided_filter_matches_single_node` — each asserts the exact expected row multiset, not a count, plus the wrapper-shape it took.
- E2E (`e2e_complex_type_test.rs`): `e2e_self_join_on_nested_json_column_matches_single_node` — self-join on a JSON-rendered `List` column; confirms the NULL-`TAGS` row matches nothing.
- Regression guard: `e2e_three_table_join_result_correct` and `e2e_above_threshold_result_matches_broadcast` (pre-existing) stay green.

## Tool Evidence

### Linter

```
cargo clippy --all-targets
    Checking lakehouse-catalog v0.2.0
    Checking vs-expression v0.2.0
    Checking lakehouse-engine v0.40.1
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 19.53s
```
0 warnings, 0 errors. (Also verified with `-D warnings` during implementation.)

### Formatter

```
cargo fmt --all -- --check
```
Exit 0, no diff.

### Build

```
cargo build --workspace
```
Exit 0.

## Scenario Coverage

| Scenario | Test Type | Test Location | Test Name | Passes |
|----------|-----------|----------------|-----------|--------|
| A table joined to itself renders each occurrence as its own leg | Unit | `joins/sql_builders_tests.rs` | `self_join_renders_each_occurrence_as_its_own_leg` | Pass |
| A table joined to itself renders each occurrence as its own leg | Integration | `tests/e2e_join_test.rs` | `e2e_self_join_on_primitive_column_matches_single_node` | Pass |
| A table joined to itself renders each occurrence as its own leg | Integration | `tests/e2e_complex_type_test.rs` | `e2e_self_join_on_nested_json_column_matches_single_node` | Pass |
| One occurrence of a self-joined table carries no alias | Unit | `joins/attribution_tests.rs` | `absent_alias_is_a_distinct_leg_key` | Pass |
| One occurrence of a self-joined table carries no alias | Integration | `tests/e2e_join_test.rs` | `e2e_self_join_with_one_unaliased_occurrence_matches_single_node` | Pass |
| A three-leg self-join attaches each condition to its own leg pair | Unit | `joins/sql_builders_tests.rs` | `three_leg_self_join_attaches_each_condition_at_its_own_join_point` | Pass |
| A three-leg self-join attaches each condition to its own leg pair | Integration | `tests/e2e_join_test.rs` | `e2e_three_leg_self_join_matches_single_node` | Pass |
| A WHERE conjunct local to one occurrence is pushed into only that occurrence's leg | Unit | `joins/rendering_tests.rs` | `leg_local_conjunct_reaches_only_its_own_occurrence_leg` | Pass |
| A WHERE conjunct local to one occurrence is pushed into only that occurrence's leg | Integration | `tests/e2e_join_test.rs` | `e2e_self_join_with_one_sided_filter_matches_single_node` | Pass |
| A column reference no leg key matches fails loudly | Unit | `joins/sql_builders_tests.rs` | `unattributable_column_reference_is_a_hard_error_naming_the_column` | Pass |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | Unit | `joins/sql_builders_tests.rs` | `n_scan_wrapper_qualifies_every_clause_by_leg` | Pass |
| A three-or-more-table inner join falls back to an N-scan unaccelerated wrapper | Integration | `tests/e2e_join_test.rs` | `e2e_three_table_join_result_correct` (pre-existing, stayed green) | Pass |
| Join conditions attach greedily by leg set and leg-local filters push into each leg | Unit | `joins/sql_builders_tests.rs` | `conditions_attach_by_leg_set_and_leg_local_filters_partition_exactly` | Pass |
| No-table-twice requests emit byte-identical SQL | Unit | `joins/sql_builders_tests.rs` | `golden_n_scan_join_sql_unchanged` (renamed target per review finding #9 — see Notes) | Pass |
| Self-join is never broadcast-eligible | Unit | `joins/planning_tests.rs` | `self_join_is_never_broadcast_eligible` | Pass |
| Self-join is never broadcast-eligible | Integration | `tests/e2e_join_test.rs` | `e2e_above_threshold_result_matches_broadcast` (pre-existing, stayed green) | Pass |
| A leg-count/resolved-side mismatch declines instead of panicking (added by expert review fix) | Unit | `joins/sql_builders_tests.rs` | `a_leg_count_disagreeing_with_the_resolved_sides_declines_naming_both_counts` | Pass |

## Notes

- **Live evidence before/after the fix.** Task 1.1 reproduced both #361 shapes against the running Docker Exasol container: the 2-leg self-join returned 100 rows and the 3-leg shape 1000 rows over a 10-row `FACT_ORDERS`, with the generated `ON` matching the plan's documented tautology byte-for-byte. During implementation (task 3.1) a temporary probe on the same query, after the fix, showed a real `ON` condition and correct per-leg filter/projection; the probe code was removed and the behavior is now covered by the permanent unit/e2e tests listed above.
- **Manual Testing table**: the plan's `exapump sql -p docker` commands against the persistent `MY_LAKEHOUSE` virtual schema were not re-run separately in this pass. That schema's virtual-schema/UDF `.so` would need a fresh BucketFS deploy to reflect this change, which the e2e suite's harness already does per-test (it rebuilds and deploys its own `.so` via `make test-e2e`'s `cross-musl-udf-build` dependency). The five new e2e tests assert byte-for-byte the same queries and expected row counts the Manual Testing table specifies, so that table's intent is covered by the automated e2e run rather than a separate manual step; no additional deploy to the shared Docker environment was performed.
- **Review-driven scope changes**: finding #9 (duplicate golden test) replaced `no_table_twice_request_emits_byte_identical_sql` with reusing `golden_n_scan_join_sql_unchanged`'s existing literal; the plan's Verification table was updated accordingly. Finding #3 (missing wrapper-shape assertions) used `!has_broadcast_join_block` instead of the literally-specified `!has_two_scan_wrapper` for the three 2-leg e2e tests, because `has_two_scan_wrapper(s) ≡ has_n_scan_wrapper(s, 2)` makes the literal pairing an unsatisfiable tautology (`X && !X`) — verified directly against the helper's source before accepting the change.
- **Version**: `crates/lakehouse-engine` bumped 0.40.0 → 0.40.1 (PATCH, per the plan's Impact section).
