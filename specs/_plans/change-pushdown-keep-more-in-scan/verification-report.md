# Verification Report: change-pushdown-keep-more-in-scan

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | The scan keeps the rendered conjuncts of a WHERE with a declined conjunct, and unprojected date-part group keys keep the split aggregate. Every check passes on Exasol 2025.1.16 and 8.29.13. |
| Code review | 8 findings (standard: 7, expert: 1), 8 fixed |

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
| Unit (`cargo test --workspace`) | 2144 | 2144 | 0 |
| E2E on Exasol 2025.1.16 (`make e2e-up test-e2e`) | 397 | 397 | 0 |
| E2E on Exasol 8.29.13 (`EXASOL_IMAGE=exasol/docker-db:8.29.13 make e2e-up test-e2e`) | 397 | 397 | 0 |

The unit run includes `datafusion_truncates_an_overprecise_timestamp_literal`, which passes on DataFusion 55.2.0.

### Manual Tests

All four ran on Exasol 2025.1.16 through `exapump sql`.

| Test | Result |
|------|--------|
| `EXPLAIN VIRTUAL SELECT ID … WHERE C_DECIMAL_A LIKE '1%' AND ID > 1`: the `LHS_T0` wrapper's `WHERE` holds `("LHS_T0"."C_DECIMAL_A" LIKE '1%')`, and the scan spec holds `"filter":"(1 < \"ID\")"` | ✓ |
| `SELECT ID … WHERE C_DECIMAL_A LIKE '1%' AND ID > 1 ORDER BY ID` returns ids 5 and 7 | ✓ |
| `SELECT COUNT(*) … WHERE C_DECIMAL_A LIKE '1%' AND ID > 1` returns 2 | ✓ |
| `EXPLAIN VIRTUAL SELECT COUNT(*) … GROUP BY YEAR(C_DATE)`: the scan spec holds `"group_keys"`, the `EMITS` clause declares `"GK_0" DECIMAL(4,0)`, and the plan has no `LHS_T0` wrapper | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.75s
```

### Formatter

```
cargo fmt --all -- --check
(no changes)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| pushdown | pushdown-declined-filter-self-apply | A declined single-table WHERE filter is applied in the adapter's own outer WHERE | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_second_arity_returns_filtered_rows` | Pass |
| pushdown | pushdown-declined-filter-self-apply | A declined filter does not widen the wrapper's projection | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_filter_select_star_returns_full_row_shape`, `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` | Pass |
| pushdown | pushdown-declined-filter-self-apply | A single-table WHERE filter splits at AND between the scan and the wrapper | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` | Pass |
| pushdown | pushdown-planning | Filter predicate is pushed into the scan spec | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` | Pass |
| pushdown | pushdown-planning | A declined WHERE filter routes the single-table request to the qualified wrapper | `crates/lakehouse-e2e/tests/e2e_local/capability.rs`, `crates/lakehouse-e2e/tests/common/pruning_cases.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan`, the `Placement::Split` case through `iceberg_pruning_keeps_every_file_with_a_matching_row` | Pass |
| pushdown-aggregates | pushdown-planning-aggregate-extensions | A declined WHERE filter under an aggregate request is applied ahead of the aggregate | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` | Pass |
| pushdown-aggregates | pushdown-planning-split-aggregate-types | A date-part function takes its fixed native type | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_split_aggregate_columns_match_native_oracle` | Pass |

## Notes

- Task 1.1 measured the date-part result types on both Exasol versions with `CREATE TABLE … AS SELECT` and `EXA_ALL_COLUMNS`. `YEAR` and `EXTRACT(YEAR …)` are `DECIMAL(4,0)`. `MONTH`, `DAY`, `HOUR`, `MINUTE`, `WEEK`, and the `EXTRACT` forms of `MONTH`, `DAY`, `HOUR`, and `MINUTE` are `DECIMAL(2,0)`. The types match over a DATE, a `TIMESTAMP(3)`, and, on 2025.1.16 only, a `TIMESTAMP(9)`. The spec delta needed no change.
- Exasol rejects `EXTRACT(HOUR FROM <date>)` and `EXTRACT(MINUTE FROM <date>)` with "Feature not supported" on both versions. The function forms `HOUR(<date>)` and `MINUTE(<date>)` work. The type rule is unaffected, because Exasol rejects the query before the adapter sees it.
- The expert review fix (4.8) closes a gap the per-conjunct split exposed. A text column used as an `AND` operand now stays in the wrapper's `WHERE`, so `WHERE C_VARCHAR AND C_DECIMAL_A LIKE '1%' AND ID > 1` raises the native 42000 error. The new error-oracle case in `e2e_declined_conjunct_keeps_the_other_conjuncts_in_the_scan` covers it.
- `WHERE C_VARCHAR AND ID > 1` now splits, and Exasol rejects it with 42000 "where clause must be a boolean expression" where native Exasol says "operands in AND must be a boolean expression". Both reject the query with the same SQL state. Mission capability 12 does not count error wording as a deviation.
- A numeric column used as an `AND` operand, as in `WHERE C_DECIMAL_A AND ID > 1`, still renders into the scan filter. The scan UDF then fails with a DataFusion type-coercion error, where native Exasol raises the 42000 error. Both reject the query. This gap predates the plan and is a candidate follow-up.
- The issue reference stays open per the plan: `Closes #615` only after a human records on #615 that the typed-literal rewrite resolves its DATE text-literal bullet, and `Refs #615` otherwise.
