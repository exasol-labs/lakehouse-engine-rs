# Verification Report: fix-iceberg-initial-default-rendered-types

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | Pre-add rows of `time`, `decimal(38,2)`, `binary`, `fixed(4)`, and `uuid` columns read their `initial-default` exactly as a stored value reads, and the REQUIRED `time` column no longer fails with 22002 (#367) |
| Code review | 4 findings, 4 fixed |

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
| Unit (`cargo test --workspace`) | 2135 | 2135 | 0 |
| Integration (E2E, `e2e_local` against Docker Exasol) | 388 | 388 | 0 |

### Manual Tests

| Test | Result |
|------|--------|
| `exapump sql 'SELECT "ID", "C_TIME", "C_TIME_REQ", "C_DEC38", "C_BINARY", "C_FIXED", "C_UUID" FROM INITDEF_VS.INITDEF ORDER BY "ID"'` | ✓ rows 1 to 6 all read `22:31:08.123456, 22:31:08.123456, 123.45, deadbeef, 01020304, 0123456789abcdef0123456789abcdef` |

## Tool Evidence

### Linter

```
make lint (cargo clippy --workspace --all-targets --all-features): exit 0, 0 warnings
```

### Formatter

```
make fmt-check (cargo fmt --all -- --check): exit 0, no changes
```

### Build

```
make cross-udf-build: exit 0
✓ 4 UDF(s) validated in 'target/release/liblakehouse_engine.so'
```

### Reproduction (before the fix)

```
VM error: F-UDF-CL-RUST-9001: UDF error: scan failed: assigned data could not be read:
Execution error: Non-nullable column 'c_time_req' is missing from the physical schema (sqlCode 22002)
```

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| scan-read-path | scan-execution-field-id-projection-absent-fields | The VS encodes each field's Iceberg initial-default once per query into the scan spec | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `declaration_encodes_every_primitive_initial_default` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | Every supported primitive initial-default survives the scan-spec serialization round-trip | `crates/lakehouse-engine/src/scan/field_id_projection_tests.rs` | `initial_default_round_trips_across_full_type_vocabulary` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | An initial-default of a type without a native Arrow tag reads as a stored value of its column reads | `crates/lakehouse-e2e/tests/e2e_local/scan.rs` | `e2e_added_columns_initial_default_fill_all_types` | Pass |
| scan-read-path | scan-execution-field-id-projection-absent-fields | A primitive initial-default that cannot be read at its column's declared type refuses the column | `crates/lakehouse-engine/src/adapter/pushdown/pushdown_tests.rs` | `declaration_refuses_a_column_whose_initial_default_cannot_be_rendered` | Pass |
| scan-types | type-mapping-iceberg-logical | A catalog-declared DECIMAL outside Exasol's DECIMAL domain falls back to VARCHAR | existing tests of the scenario | unchanged (cross-reference reworded only) | Pass |

## Notes

- The encode and refusal unit tests live in `adapter/pushdown/pushdown_tests.rs` next to the existing declaration tests, not in `format/iceberg_tests.rs` as the plan listed.
- The refusal scenario's "a query that reads only other columns succeeds" relies on the existing refused-column handling of `vs-adapter/column-source-notes`; the unit test proves the note carries `refused`, and no new E2E test covers a refused default.
- Existing virtual schemas pick up the fix after their next `CREATE`, `REFRESH`, or `SET PROPERTIES` (D4).
