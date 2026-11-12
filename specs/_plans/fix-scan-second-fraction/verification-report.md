# Verification Report: fix-scan-second-fraction

## Verdict

| Result | Details |
|--------|---------|
| **PASS** | `SECOND(ts)` and `EXTRACT(SECOND FROM ts)` keep the fraction of the argument's declared precision in the scan, typed `DECIMAL(2+p, p)` (#603). |
| Code review | 4 findings (4 standard, 0 expert) — 4 fixed |

| Check | Status |
|-------|--------|
| Build | ✓ (`make cross-udf-build` exit 0, 4 UDFs validated; `cargo check --workspace` after the version bump exit 0) |
| Tests | ✓ |
| Lint | ✓ |
| Format | ✓ |
| Scenario Coverage | ✓ |
| Manual Tests | ✓ |

## Test Evidence

### Coverage

| Type | Coverage % |
|------|------------|
| Changed lines | 96.5% (82 of 85) |
| Workspace lines | 91.80% |

The 3 uncovered lines are the guard in `render_seconds` for more than 9 fractional digits. The classifier declines that case first, so the guard stays unreachable by design (the reviewer agreed it stays).

### Test Results

| Type | Run | Passed | Ignored |
|------|-----|--------|---------|
| Unit (`cargo test --workspace --lib`, after the review fixes) | all binaries | all, 0 failed | none new |
| Integration (E2E, `--test-threads=1 timestamp_precision split_aggregate`, before the review fixes) | 9 | 9 | 0 |

The review fixes touched a decline message and tests only. Phase B re-runs the full unit and E2E suites on the committed tree.

### Manual Tests

| Test | Result |
|------|--------|
| `SELECT ID, SECOND(TS), SECOND(TS_NS) FROM TS_PRECISION_VS.TS_PRECISION_PROBE` returns `0.000001`, `0.000002`, `0.123456`, `0.123457` and `0.000000001`... | ✓ |
| `EXPLAIN VIRTUAL SELECT SECOND(TS)` shows `date_part('microsecond', "TS")` and `EMITS DECIMAL(8,6)` | ✓ |
| `EXPLAIN VIRTUAL ... GROUP BY SECOND(TS_NS)` is a partial aggregate with group key `DECIMAL(11,9)` and no raw `"TS_NS"` projection | ✓ |
| `WHERE SECOND(TS) > 0.1` counts 2 rows (0 before) | ✓ |

## Tool Evidence

### Linter

`make fmt-check lint` exits 0 with no warnings.

### Formatter

`cargo fmt --all` leaves no changes.

## Scenario Coverage

See plan.md § Verification › Scenario Coverage. Every scenario maps to `second_renders_the_fraction_of_its_argument_precision`, `second_of_a_date_is_zero_without_an_exact_type`, `second_over_any_other_argument_declines` (`scan_typing_tests.rs`), the `lib_tests.rs` render tests, or the E2E `second_and_extract_second_match_a_native_copy` (`timestamp_precision.rs`), and all of them pass.

## Notes

- The E2E marker strings use doubled quotes (`date_part(''microsecond''`) because the scan spec sits inside a SQL string literal. The plan spelled them with single quotes.
- The implementer also edited the `group_key_exasol_types` doc comment in `grouped_agg.rs`, which the new typing rule made incomplete, and widened `exasol_type::MAX_FRACTIONAL_DIGITS` to `pub(crate)`.
- The E2E run reused the Exasol stack started from the main checkout, so it uploaded this worktree's `.so` to that stack's BucketFS.
- Versions bumped: `lakehouse-engine` 0.58.2 to 0.58.3, `vs-expression` 0.3.0 to 0.3.1.
