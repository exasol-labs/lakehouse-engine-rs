# Verification Report: add-type-relaxation

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All 19 implementation tasks and 8 code-review fixes complete. Host unit suite, both live E2E suites (Iceberg and Unity/Delta), lint, and format all green. |
| Code review | 14 findings — 14 fixed |

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
| Unit (host `cargo test -p lakehouse-engine --lib`) | 1058 | 1058 | 0 |
| Integration/E2E (Iceberg, `make test-e2e`) | 264 | 264 | 0 |
| Integration/E2E (Unity/Delta, `make test-e2e-unity`) | 24 | 24 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `vs-adapter/delta-reader-feature-gating`: `SELECT COUNT(*) FROM DELTA_E2E.TYPE_WIDENING` returns 2 | ✓ (covered by `unity_delta_type_widening_returns_the_widened_types_across_both_files`, run live) |
| `datafusion-scan/type-relaxation`: widened-column projection returns real pre/post-widening values | ✓ (same live test, asserts all eleven protocol-supported columns) |
| `vs-adapter/delta-type-mapping`: `DECIMAL_DECIMAL_GREATER_SCALE` returns rescaled `decimal(20,5)` values, no refusal | ✓ (same live test) |
| `vs-adapter/iceberg-type-promotion`: `SELECT * FROM E2E_LAKEHOUSE.ICEBERG_TYPE_PROMOTION` returns both layouts at promoted types | ✓ (covered by `iceberg_type_promotion_returns_both_layouts_at_the_promoted_types`, run live against the real Spark-authored fixture) |
| `vs-adapter/iceberg-type-promotion`: a `date`→`timestamp` promotion refuses naming table/column/types/issue | ✓ (unit-only, per decision [14] — no live fixture exists; covered by `date_to_timestamp_promotion_is_refused_naming_table_column_both_types_and_the_issue` and the `resolve_scan`-level integration tests in `iceberg_tests.rs`) |
| `packaging/iceberg-type-promotion-fixture`: Spark job authors the table, exits 0 | ✓ (`spark-iceberg-fixtures` job run live, exit 0, `=== spark-iceberg-fixtures: type-promotion fixture ===` section clean) |
| `e2e-harness/unity-catalog-e2e-harness-delta-queries`: `make test-e2e-unity` exits 0, all Delta scenarios pass | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --all-targets --all-features
Finished — exit 0, no warnings
```

### Formatter

```
cargo fmt --check
Finished — exit 0, no diff
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | type-relaxation | A narrow physical column binds to the current wider logical type and is cast per file | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `a_narrow_physical_column_is_cast_to_the_current_logical_type_per_file` | Pass |
| datafusion-scan | type-relaxation | Every supported relaxation pair is proven castable rather than assumed | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `arrow_castability_pins_every_supported_relaxation_pair` | Pass |
| datafusion-scan | type-relaxation | Every supported relaxation pair is proven castable rather than assumed | `crates/lakehouse-engine/src/scan/type_relaxation_tests.rs` | `every_supported_relaxation_pair_reads_its_real_values_from_a_narrow_parquet_file` | Pass |
| datafusion-scan | type-relaxation | A relaxed column crosses the emit boundary at its declared Exasol type | `crates/lakehouse-engine/src/scan/emit_tests.rs` | `a_relaxed_column_coerces_to_its_declared_exatype_without_a_relaxation_branch` | Pass |
| vs-adapter | iceberg-type-promotion | A promotion this engine reads resolves through the shared relaxation cast | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `a_readable_iceberg_promotion_plans_normally_and_carries_the_current_type` | Pass |
| vs-adapter | iceberg-type-promotion | A promotion this engine reads resolves through the shared relaxation cast | `crates/lakehouse-engine/tests/e2e_type_relaxation_test.rs` | `iceberg_type_promotion_returns_both_layouts_at_the_promoted_types` | Pass (live) |
| vs-adapter | iceberg-type-promotion | A date-to-timestamp promotion is refused at plan time by name | `crates/lakehouse-engine/src/adapter/pushdown/format/iceberg_tests.rs` | `date_to_timestamp_promotion_is_refused_naming_table_column_both_types_and_the_issue` | Pass |
| vs-adapter | iceberg-type-promotion | The unknown primitive type is unrepresentable, and the mapping is the tripwire | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `iceberg_primitive_mappings_are_exhaustive_so_a_new_variant_breaks_the_build` | Pass |
| vs-adapter | delta-reader-feature-gating | A reader feature outside the allow-list refuses the table before any log replay | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `a_reader_feature_outside_the_allow_list_is_refused_with_no_per_feature_special_case` | Pass |
| vs-adapter | delta-reader-feature-gating | Every allow-listed reader feature keeps its table queryable | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_protocol_tests.rs` | `all_seven_allow_listed_reader_features_pass_including_both_type_widening_names` | Pass |
| vs-adapter | delta-reader-feature-gating | Every allow-listed reader feature keeps its table queryable | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `every_shipped_fixture_whose_reader_features_are_allow_listed_still_resolves` | Pass |
| vs-adapter | delta-type-mapping | Every recorded Delta type change is validated, and an unsupported one refuses its column | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_schema_tests.rs` | `every_pair_the_protocol_lists_is_supported`, `a_field_carrying_an_unsupported_recorded_type_change_is_refused_naming_both_types`, `a_field_whose_recorded_type_changes_are_all_supported_plans_normally` | Pass |
| vs-adapter | delta-type-mapping | Every recorded Delta type change is validated, and an unsupported one refuses its column | `crates/lakehouse-engine/src/adapter/pushdown/format/delta_replay_tests.rs` | `an_unsupported_recorded_type_change_refuses_only_its_own_column` | Pass |
| packaging | iceberg-type-promotion-fixture | Spark produces an Iceberg table whose readable promotions span the schema change | `crates/lakehouse-engine/tests/e2e_type_relaxation_test.rs` | `e2e_type_promotion_pre_promotion_data_file_is_physically_narrow` | Pass (live) |
| packaging | iceberg-type-promotion-fixture | The new fixture and its suite are wired into the paths that actually run | `crates/lakehouse-engine/tests/build_convention.rs` | `the_type_relaxation_suite_and_fixture_are_wired_into_run_fixtures_and_make_test_e2e` | Pass |
| e2e-harness | unity-catalog-e2e-harness-delta-queries | A Delta table using an unsupported reader feature fails the query loud (CHANGED) | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_unsupported_reader_feature_fails_the_query_loud` | Pass (live) |
| e2e-harness | unity-catalog-e2e-harness-delta-queries | A type-widened Delta table returns its current wider types across the widening boundary | `crates/lakehouse-engine/tests/e2e_unity_test.rs` | `unity_delta_type_widening_returns_the_widened_types_across_both_files` | Pass (live) |

## Notes

**Scope correction during implementation (decision [14]).** Task 6.1 found that Apache Iceberg
Java never implements the `date` → `timestamp` promotion at any version this stack can run —
`TypeUtil.isPromotionAllowed` has no `date` case, confirmed identical across `apache-iceberg-1.10.1`,
`apache-iceberg-1.11.0`, and `main`. No conforming Spark writer can author the second fixture table
the plan originally scoped. This is the exact contingency the plan's interview pre-authorized
("fall back to unit-test coverage… rather than blocking the whole plan"); the fallback was applied
directly rather than re-planning, and is recorded in decision [14]. The `date`/`timestamp_ns`
refusal is proven by unit tests (`refuse_date_promotion` over synthetic `TableMetadata`) plus
integration tests driving the real `resolve_scan` entry point — not by a live fixture.

**Second scope correction (decision [15]).** Wiring the Delta `delta.typeChanges` validation
(task 3.2) found that two of the vendored `type-widening` fixture's thirteen recorded changes —
`byte_decimal` (`byte`→`decimal(4,1)`) and `short_decimal` (`short`→`decimal(6,1)`) — derive a
NEGATIVE `k1` against the Delta protocol's `Decimal(10+k1,k2)` base and are outside the current
protocol's supported list. These two columns are refused per column (the same mechanism already
used for `binary`/`map`/`struct`/`variant`); the other eleven of thirteen are queryable at their
widened types. `e2e-harness/unity-catalog-e2e-harness-delta-queries/spec.md` and plan task 5.3 were
updated to assert eleven columns queryable and two refused, not all thirteen queryable.

**A real bug surfaced during live E2E verification, fixed in this pass.** The new
`iceberg_type_promotion_returns_both_layouts_at_the_promoted_types` test failed on its first live
run: Exasol's WebSocket rendering trims the trailing zero on a decimal value (`12345678.90` →
`12345678.9`), consistent with this project's already-recorded `e2e_decimal_cast_trims_trailing_zeros`
behavior. The four `decimal_decimal` ground-truth constants in `tests/common/type_promotion_fixtures.rs`
were corrected to match the live rendering; rerun confirmed green.

**Code review.** 14 findings (10 standard, 4 expert) were raised and all fixed: two outdated doc
comments, a dead struct field, over-broad `pub(super)` visibility, a mixed-abstraction-level loop,
six unused constants, a silenced clippy lint (replaced with non-pi/e literals rather than suppressed),
and five missing-boundary-test gaps — including one (`ASSERTION_FREE_TEST`) where the original
exhaustiveness "tripwire" test asserted nothing that could ever fail. Each missing-test fix was
verified with a genuine RED-then-GREEN cycle (mutation-testing the production code to confirm the
new test actually fails when it should).

**Version bump not yet applied.** Per the plan's Impact section, this is `feat` — a MINOR bump on
`crates/lakehouse-engine` (0.38.0 → 0.39.0) — applied as the next step in the implement-pr pipeline,
before commit.
