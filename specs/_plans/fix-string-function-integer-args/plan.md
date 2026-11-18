# Plan: fix-string-function-integer-args

## Summary

The scan converts the numeric argument of `LPAD`, `RPAD`, `REPEAT`, `LEFT`, `RIGHT`, `SUBSTR`, `CHR`, and `UNICODECHR` to `Int64` by its Arrow type, rounding a fraction as Exasol does, so these calls no longer fail with a DataFusion planning error (#604).

## Context

- Exasol runs `RPAD(C_VARCHAR, 2 + MOD(ID, 2))`, `RPAD(C_VARCHAR, C_DECIMAL_A)`, and `RPAD(C_VARCHAR, C_DOUBLE)`. The virtual schema fails each with `Failed to coerce arguments to satisfy a call to 'rpad' function`.
- DataFusion 55.2's `lpad`, `rpad`, `left`, and `right` accept only an exact `Int64` argument, `chr` a uniform `Int64`, and `substr` and `repeat` coerce only other integers. None accepts `Decimal128` or `Float64`.
- The scan's `ExasolNumericTyping` analyzer rule (`crates/lakehouse-engine/src/scan/exasol_numeric.rs`) types `+`, `-`, `*`, and `MOD` as Exasol does, so `2 + MOD(ID, 2)` is `Decimal128(p, 0)` even over an `Int64` column (#475).
- That rule runs before DataFusion's type coercion on every scan plan, knows each argument's exact Arrow type, and already rewrites `greatest`, `least`, and `coalesce` arguments by type. A rewrite there covers the select list, filters, GROUP BY keys, aggregate arguments, and join conditions at once, and leaves the scan SQL unchanged.
- The `vs-expression` typed pass (#587, PR #596) knows only the Exasol type. `DECIMAL(20,0)` there can be `Int64` or `Decimal128(20,0)` in the scan, so a fix there would cast every integer argument, including a bare `Int64` column.
- Exasol rounds a non-integer argument to the nearest integer, a half upward: `RPAD('ab', 3.5)` is `'ab  '`, `SUBSTR('abcd', 1.5)` is `'bcd'`, `CHR(65.5)` is `'B'` (live, 2025.1.16, per #604).
- Follow-up plans:
  - `fix-substr-negative-start`: a negative `SUBSTR` start counts from the string's end in Exasol (`SUBSTR('abcd', -2)` is `'cd'`); the scan clamps it to the first character. Present before this change for an `Int64` start.
  - `fix-round-trunc-scale-integer-arg`: the scale argument of `ROUND(x, n)` and `TRUNC(x, n)` with a DECIMAL or DOUBLE `n`, after a live check that it fails the same way.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| scan-numeric-typing | CHANGED | `scan-types/scan-numeric-typing/spec.md` |

## Impact

Queries that call one of the eight functions with an arithmetic, DECIMAL, DOUBLE, or non-`Int64` integer argument now return native Exasol's result instead of failing. An argument outside ±999,999,999 now fails with Exasol's `numeric value out of range` message, where an `Int64` argument previously computed the function. The scan SQL in `EXPLAIN VIRTUAL` is unchanged for every query. No capability, property, or CONNECTION field changes, so `docs/` needs no update. No breaking change.

## Requirements

| Requirement | Details |
|-------------|---------|
| Argument positions | `lpad`/`rpad`: argument 1. `repeat`, `left`, `right`: argument 1. `substr`: arguments 1 and 2. `chr`: argument 0 (`UNICODECHR` renders as `chr` too). |
| Negative arguments | After rounding, a negative `lpad`/`rpad`/`left`/`right` count or `substr` length becomes NULL; a negative `repeat` count fails with `data exception - negative numbers not allowed in function REPEAT`. A negative `substr` start and a `chr` code keep DataFusion's handling (follow-up). |
| Conversion | `scan/string_argument.rs` wraps DataFusion's `lpad`, `rpad`, `repeat`, `left`, `right`, `substr`, and `chr` in `ExasolStringFunction`, registered on the scan session builder after the defaults so it replaces them, plus an `ExprPlanner` placed first for `SUBSTR`, which the SQL parser reads as `SUBSTRING` (D7). The wrapper accepts any integer, Decimal, Float, dictionary, or NULL argument at those positions, rounds a fraction, range-checks, and passes `Int64` to the wrapped function. `Int64` arguments are range-checked too (D3). |
| Rounding | A Decimal rounds exactly, a Float by `f64::round`, both a half away from zero. Live, native Exasol rounds `-1.5` to `-2` (D2). |
| Out of range | Live, native Exasol fails every one of the eight functions for an argument outside `[-999999999, 999999999]` with `data exception - numeric value out of range: value <v> is not in [ -999999999 .. 999999999 ]` (SQL state 22003). The UDF raises `numeric_overflow(...)` with the same detail and records it in the session's `RaisedFailure` slot, so the message survives the Parquet row filter, as the exact-arithmetic UDFs do. |
| Typing rule interaction | DataFusion checks a function's signature while it plans the SQL, before `ExasolNumericTyping` runs, so the conversion cannot be an analyzer rewrite. The wrapper's `coerce_types` keeps a numeric argument's own type, and `ExasolNumericTyping` leaves the call alone. |

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | | The fix adds one module. `string_function_arg_type_guard` no longer exists on `main` (removed by #596). |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A string function's numeric argument rounds to an integer as Exasol does | Integration | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `string_function_numeric_arguments_match_native` |
| A string function's numeric argument rounds to an integer as Exasol does | Unit | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `string_function_numeric_argument_rounds_to_int64` |
| A string function's numeric argument outside Exasol's range fails the query | Integration | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `string_function_numeric_arguments_match_native` |
| A string function's numeric argument outside Exasol's range fails the query | Unit | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `string_function_numeric_argument_outside_exasols_range_fails` |
| A negative count or length makes a string function return NULL as Exasol does | Integration | `crates/lakehouse-e2e/tests/e2e_local/text_conversion.rs` | `string_function_numeric_arguments_match_native` |
| A negative count or length makes a string function return NULL as Exasol does | Unit | `crates/lakehouse-engine/src/scan/string_argument_tests.rs` | `negative_count_or_length_returns_null_or_refuses_repeat` |

- `string_function_numeric_arguments_match_native` reuses `assert_cases_match_native` over `TYPED_DISTINCT_PROBE` and its native copy. It runs each of the eight functions with an arithmetic argument (`2 + MOD(ID, 2)`), a DECIMAL argument (`C_DECIMAL_A`, holding `10.50` and `40.99`), and a DOUBLE argument (`C_DOUBLE`, holding the halves `0.5` to `5.5`), scaled into a sensible range where needed (for example `CHR(60 + C_DOUBLE)`, `SUBSTR(C_VARCHAR, C_DOUBLE + 0.5, C_DOUBLE)`, every start at or above 1). Each shape runs in the select list (`Has(SCAN_ITEM)`), a WHERE filter (`Has(SCAN_FILTER)`), a GROUP BY key (`Split`), and one aggregate argument (`MAX(RPAD(C_VARCHAR, C_DOUBLE))`, `Split`). The path check proves the scan computed it, and the native comparison proves the values and rows, halves included. Negative counts and lengths, and an out-of-range argument as an error case, run the same way.
- The unit tests evaluate SQL through `scan_session_context` over a `MemTable` with `Int32`, `Int64`, `Decimal128(20,0)`, `Decimal128(9,2)`, and `Float64` columns. The first asserts each function's result per argument type, halves included. The second asserts that `1e9` and `-1e9` as `Int64`, `Float64`, and `Decimal128(36,0)` fail the query with a message starting `data exception - numeric value out of range` that is recorded on the session, and that `999999999` succeeds for `SUBSTR` and `CHR` (whose results stay small).

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| scan-numeric-typing | After `make cross-udf-build` and `make e2e-up`, run through `exapump` against the virtual schema over `e2e_lakehouse`: `SELECT ID, RPAD(C_VARCHAR, 2 + MOD(ID, 2)), SUBSTR('abcd', C_DOUBLE), CHR(64 + C_DOUBLE) FROM <vs>.TYPED_DISTINCT_PROBE ORDER BY ID` | Rows, no `Failed to coerce arguments` error; row `ID = 1` (`C_DOUBLE = 0.5`) shows `'abcd'` and `'A'`; the same query over the native copy returns identical rows |
| scan-numeric-typing | `EXPLAIN VIRTUAL SELECT RPAD(C_VARCHAR, ID) FROM <vs>.TYPED_DISTINCT_PROBE` | Scan spec expression `rpad("C_VARCHAR", "ID")`, as before the change |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace --lib exasol_numeric` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Coverage | `cargo llvm-cov --workspace --lcov --output-path <scratchpad>/lcov.info` and `python3 .claude/skills/test-strategy-review/scripts/changed_line_coverage.py <scratchpad>/lcov.info` | Changed-line coverage at or above 90% |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |
