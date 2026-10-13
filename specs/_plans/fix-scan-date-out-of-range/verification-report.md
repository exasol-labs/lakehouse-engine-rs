# Verification Report: fix-scan-date-out-of-range

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | A Date32 value outside Exasol's DATE range now fails the query with an error naming the column and the day count, on the raw-row, aggregate, and value-conversion paths. |
| Code review | 7 findings (standard: 7, expert: 0), all fixed (tasks 4.5 to 4.11) |

| Check | Status |
|-------|--------|
| Build | ✓ |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ (covered by the E2E test, see Notes) |

## Test Evidence

### Test Results

| Type | Result |
|------|--------|
| Unit (`cargo test`) | 0 failures; 1431 tests in the main engine suite |
| E2E (`make test-e2e`, local Docker Exasol) | 0 failures across all suites; `out_of_range_date_fails_the_query_on_every_emit_path` passes |

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT ID, STORED_DATE ... WHERE ID = 4` errors naming `STORED_DATE` and `100000000` | ✓ asserted by the E2E test |
| `GROUP BY STORED_DATE` over IDs 1 and 4 errors, no 1970-01-01 group | ✓ asserted by the E2E test |
| IDs 1, 2, 3 return 2024-01-01, 0001-01-01, 9999-12-31 | ✓ asserted by the E2E test |

## Tool Evidence

### Linter

`cargo clippy --all-targets`: exit 0, 0 warnings.

### Formatter

`cargo fmt --all -- --check`: exit 0, no changes.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| datafusion-scan | scan-execution-value-conversion | A DATE value outside Exasol's DATE range fails the query | `crates/lakehouse-engine/tests/e2e_direct_storage_test.rs` | `out_of_range_date_fails_the_query_on_every_emit_path` | Pass |
| datafusion-scan | scan-execution-value-conversion | guard bounds and message (supporting unit tests) | `crates/lakehouse-engine/src/scan/convert_tests.rs` | `date32_guard_*` | Pass |

## Notes

- The `exapump` binary is not on PATH in this session, so the three manual queries were not re-run by hand. The E2E test runs the same three cases against the Docker Exasol container.
- The workspace version bumps `lakehouse-engine` from 0.52.1 to 0.52.2 (patch, fix plan). The plan has no version spec delta.
