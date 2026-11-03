# Verification Report: refactor-exasol-column-type

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Every Exasol type in `lakehouse-engine` is an owned `ExasolType`, and `ColumnTypes` is the one per-column map. Declared column types, pushdown SQL, and golden SQL are unchanged against `main`. |
| Code review | 10 findings — 10 fixed |

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
| Unit and in-crate integration (`cargo test --workspace`) | 2106 | 2104 | 2 |
| E2E, local Exasol 2025.x (`make e2e-up test-e2e`) | 343 | 343 | 0 |

The 2 ignored tests are the existing `micro_bench` benchmarks. The local E2E suite passed twice on the final code.

### Manual Tests

| Test | Result |
|------|--------|
| `SYS.EXA_ALL_COLUMNS` after `make test-e2e`, `main` (`a4a2ca6d`) against the branch, both suites run back to back on one stack: 3,804 columns each, 0 type differences | ✓ |
| `scripts/capture-pushdown-payload.sh 'SELECT C_TS, C_DECIMAL_A, C_VARCHAR FROM {table}'`: identical on `main` and the branch, `EMITS ("C_DECIMAL_A" DECIMAL(9,2), "C_VARCHAR" VARCHAR(2000000), "C_TS" TIMESTAMP(6))` | ✓ |
| `scripts/capture-pushdown-payload.sh "SELECT CAST(C_VARCHAR AS CHAR(20)) FROM {table}"`: identical on `main` and the branch, `EMITS ("_LH_PROJ_0" CHAR(20))` | ✓ |
| Golden SQL: `git diff --exit-code main -- crates/lakehouse-engine/src/adapter/pushdown/testdata/dispatch_golden/` exits 0 | ✓ |
| Done checks 1 to 3 (task 5.1): the dead-name search prints nothing; the type-literal search hits only `types/exasol_type.rs`, the two `CAST` `dataType` sites in `support.rs`, and the `DATE` messages in `scan/convert.rs`; the tuple-and-`String` search prints only `InvolvedColumns.types` and `first_refused_member` | ✓ |

## Tool Evidence

### Linter

```
cargo clippy --workspace --all-targets --all-features -- -D warnings
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 4.06s
```

### Formatter

```
cargo fmt --all -- --check
(no output, exit 0)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| scan-types | type-mapping | A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `catalog_decimal_guard_is_shared_by_both_source_kinds` | Pass |
| scan-types | type-mapping | The Iceberg-to-Arrow logical mapping reads the same catalog-decimal guard | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `catalog_decimal_guard_is_shared_by_both_source_kinds` | Pass |
| scan-types | type-mapping-timestamp-precision | A declared TIMESTAMP(p) EMITS column maps back to the Arrow unit of that precision | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `cast_to_timestamp9_emits_nanoseconds_and_keeps_every_seeded_digit` | Pass |
| vs-adapter | create-virtual-schema-declaration-details | A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision | `crates/lakehouse-e2e/tests/e2e_local/timestamp_precision.rs` | `iceberg_microsecond_timestamps_round_trip_at_the_declared_precision` | Pass |
| vs-adapter | create-virtual-schema-declaration-details | A TIMESTAMP(p) column declaration serializes fractionalSecondsPrecision (CHAR and ASCII step) | `crates/lakehouse-engine/src/types/mapping_tests.rs` | `exasol_type_to_json_round_trips_every_variant` | Pass |

## Notes

- Not run locally, per `.speq/implement-pr-hook.md`: the Exasol 8.x local suite, Lakekeeper, Unity Catalog, and Glue. CI runs every suite on the PR.
- `cargo build` for the `.so` ran through `make cross-udf-build`, which `make test-e2e` invokes.
- The first `SYS.EXA_ALL_COLUMNS` comparison differed in table count, not in type. Tests create and drop fixture tables, such as `INITDEF`, in the shared catalog, so the table set depends on when a schema was created. Running both suites back to back on one stack removed the difference.
- Review fix 7.4: a request `fractionalSecondsPrecision` above 255 saturates to 255, so it stays an explicit `TIMESTAMP(n)` that Exasol rejects. Exasol sends at most 9.
- Review fix 7.10: `FanOutProjection::new` refuses a projected column that the type map lacks. Production builds the projection from the same map, so no request reaches the error.
- `ColumnTypes` and `ProjectedColumn` are public, because the public functions `build_scan_driving_sql` and `validate_agg_col_types` take them. Both surface probes list them.
