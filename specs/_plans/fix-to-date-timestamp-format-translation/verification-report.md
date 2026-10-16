# Verification Report: fix-to-date-timestamp-format-translation

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | All five plan scenarios have passing tests. One E2E test outside this change fails on the 2-CPU Docker host. |
| Code review | 8 findings (8 standard, 0 expert), 8 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ `make cross-udf-build` exit 0 |
| Tests | ✓ `cargo test --workspace`: 1925 passed, 0 failed |
| E2E | ✓ with one unrelated failure, see Notes |
| Lint | ✓ `cargo clippy --all-targets`: 0 warnings |
| Format | ✓ `cargo fmt --all -- --check`: no changes |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ via the E2E equivalents, see Notes |

## Test Evidence

### Coverage

Changed-line coverage of production lines is 96/96 = 100.0% (threshold 90%): `crates/vs-expression/src/lib.rs` 49/49, `crates/vs-expression/src/datetime_format.rs` 47/47.

### Test Results

| Type | Run | Passed | Failed |
|------|-----|--------|--------|
| Unit (workspace) | 1925 | 1925 | 0 |
| E2E, 10 binaries in `make test-e2e` before the failure, plus 8 binaries rerun with `--no-fail-fast` | 601 | 600 | 1 (unrelated) |

### Manual Tests

| Test | Result |
|------|--------|
| `TO_TIMESTAMP(... 'YYYY-MM-DD HH24:MI:SS')` returns the native value | ✓ `e2e_to_date_to_timestamp_translated_format_matches_native` |
| Declined `DD-MON-YYYY` format stays in the wrapper, no `to_date(` in the scan | ✓ `e2e_to_date_declined_format_matches_native` |
| `TO_DATE(... 'YYYY-MM-DD')` returns `2024-01-01` | ✓ `e2e_to_date_to_timestamp_translated_format_matches_native` |

## Tool Evidence

### Linter

`cargo clippy --all-targets` exit 0, 0 warnings.

### Formatter

`cargo fmt --all -- --check` exit 0.

## Scenario Coverage

| Domain | Feature | Scenario | Test Location | Test Name | Passes |
|--------|---------|----------|---------------|-----------|--------|
| sql-functions | datetime-format / date-fns | TO_DATE and TO_TIMESTAMP translate to DataFusion conversion calls | `e2e_capability_test.rs`, `lib_tests.rs` | `e2e_to_date_to_timestamp_translated_format_matches_native`, `renders_to_date_to_timestamp` | Pass |
| sql-functions | datetime-format | A string-literal format translates to a strftime pattern | `e2e_capability_test.rs`, `datetime_format_tests.rs` | `e2e_to_date_to_timestamp_translated_format_matches_native`, `translates_supported_formats` | Pass |
| sql-functions | datetime-format | A format the translation cannot express declines in the DataFusion dialect | `lib_tests.rs`, `datetime_format_tests.rs` | `to_date_to_timestamp_decline_untranslatable_format`, `declines_unsupported_formats` | Pass |
| sql-functions | datetime-format | A declined format returns the native result on every pushdown path | `e2e_capability_test.rs` | `e2e_to_date_declined_format_matches_native` | Pass |
| sql-functions | datetime-format | A string Exasol rejects for a translated format fails the query | `e2e_capability_test.rs` | `e2e_to_date_translated_format_fails_on_rejected_input` | Pass |

## Notes

- `make test-e2e` stops at `e2e_scan_test::adapter_detects_container_cpuset`, which panics with `PRECONDITION UNMET: the Exasol container's CPU set "0-1" must name at least 3 CPUs`. The Docker host gives the container 2 CPUs. The change does not touch that code. Cargo skipped the remaining 8 binaries after that failure, so they were rerun with `--no-fail-fast` and all passed.
- A fifth E2E test, `e2e_to_date_translated_format_empty_string_is_null`, was added beyond the plan and passes. It covers the `nullif` empty-string rule of decision-log [1].
- `exapump` is not installed on the host. The manual-testing queries are the ones the E2E tests above run against the Docker Exasol, so they were executed through the tests.
- `changed_line_coverage.py` diffs only committed changes and reported 0/0 on this uncommitted work. The figure above comes from the same logic applied to the working tree.
- Nothing is committed.
