# Plan: fix-instant-column-tstz-comparisons

## Summary

A `TIMESTAMP WITH LOCAL TIME ZONE` value compared with an instant column (Iceberg `timestamptz`, Delta or Unity `timestamp`, Parquet UTC-adjusted or INT96) compares the instants, so `ts_tz > CURRENT_TIMESTAMP - INTERVAL '1' HOUR` returns native rows in every session. The column stays a plain `TIMESTAMP` holding UTC, and `docs/semantic-differences.md` documents how it reads and compares with plain values (#658).

## Context

- #658: an instant column reads as its UTC date and time, and today a compared TSTZ value folds to its wall-clock time in the folding zone, so the scan compares a UTC value with a zone-local one and every such filter is off by the folding zone's offset.
- Exasol sends every TSTZ value, including the ones it folds itself such as `CURRENT_TIMESTAMP - INTERVAL '1' HOUR`, as a `literal_timestamputc` holding UTC. It sends a plain `TIMESTAMP` or `DATE` literal unchanged (probed on 2025.1.16).
- A `TIMESTAMP(p) WITH LOCAL TIME ZONE` virtual column with an exact pushdown-side conversion works on 2025.1.16 and 8.29.13 (probed with a throwaway Lua adapter). Exasol refuses that type as an `EXPORT ... INTO SCRIPT` source column (`export/export-iceberg-table`), so the declaration would break the round trip `export/export-append-iceberg-table` specifies. The user kept the plain `TIMESTAMP` declaration.
- Native Exasol compares a TSTZ column with a plain `TIMESTAMP` on the session-local wall clock, and computes `ADD_DAYS`, `TRUNC`, and `DATE_TRUNC` over it in the session zone, resolving the result by the session's `TIME_ZONE_BEHAVIOR`. The adapter sees neither setting, which is why plain values keep comparing as UTC.
- `scan-types/type-mapping-timestamp-precision` "Iceberg timestamptz maps to plain Exasol TIMESTAMP" stays true: no declared type and no emitted value changes.
- Architecture: the fold constraint changes, see `architecture.md`.

## Features

| Feature | Status | Spec |
|---------|--------|------|
| pushdown-planning-clock-functions | CHANGED | `pushdown/pushdown-planning-clock-functions/spec.md` |
| session-time-zone | CHANGED | `vs-properties/session-time-zone/spec.md` |

## Impact

- A comparison between an instant column and `CURRENT_TIMESTAMP`, an Exasol 8 `LOCALTIMESTAMP`, or a `TIMESTAMP WITH LOCAL TIME ZONE` literal returns different rows, and its scan prunes different files, whenever the folding zone (`SESSION_TIME_ZONE`, else `DBTIMEZONE`) is not UTC. The new rows are the native ones in every session. The old ones were off by the folding zone's offset.
- Such a comparison no longer reads `TZ`, so it succeeds under an unreadable `TZ`.
- No declared type, displayed value, property, or capability changes, and no `REFRESH` is needed.
- Docs: `docs/semantic-differences.md` gains an "Instant columns" section, and its clock section, `docs/capabilities.md`, `docs/tuning.md` (`SESSION_TIME_ZONE`), and `docs/capability-matrix.md` (`TIMESTAMP_UTC`) state the UTC fold for instant columns. A comparison Exasol evaluates itself (a join with a native TSTZ column, a non-pushed subquery, or one under `EXCLUDED_CAPABILITIES`) still reads the plain UTC column as session-local, so the "Instant columns" section names it with the `CONVERT_TZ(tz, 'UTC', SESSIONTIMEZONE)` workaround, and the `EXCLUDED_CAPABILITIES` workaround no longer applies to instant columns.

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test case | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` `a_tstz_literal_comparison_runs_in_the_scan`, the `TZ` arm of its column loop | The instant column now folds to UTC; the new instant-column test proves it |
| Test case | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` `clock_functions_match_native_when_the_session_zone_matches`, the `TZ` column | Same reason; the oracle's plain `TZ` column reads session-local |

No production code becomes redundant.

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| A TIMESTAMP WITH LOCAL TIME ZONE value compared with an instant column compares the instants | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_tstz_value_compared_with_an_instant_column_compares_the_instants` (oracle gains a `TIMESTAMP WITH LOCAL TIME ZONE` copy of `TZ`, inserted in a UTC session) |
| (same, pruning step) | Integration | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `clock_and_tstz_filters_prune_files`: the `TZ` case of `CLOCK_CASES` (`tests/common/pruning_cases.rs`) runs in a `PACIFIC/KIRITIMATI` session, with the files and the oracle bound of the instant's UTC date and time |
| (same, every instant source) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/clock_tests.rs` | `tstz_values_compared_with_an_instant_column_fold_to_utc`: one case row per source tag (`timestamptz_ms/us/ns`, and `timestamp_us` folding to the wall clock), per comparison shape, and per `LOCALTIMESTAMP` version |
| A plain value compared with an instant column compares with its UTC date and time | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_plain_value_compared_with_an_instant_column_compares_its_utc_date_and_time` |
| A clock function reaches the pushed SQL as a literal | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `each_clock_function_reaches_the_pushed_sql_as_a_literal` (unchanged; it compares `TS`) |
| A filter on a clock function or a time zone literal prunes files at plan time | Integration | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `clock_and_tstz_filters_prune_files`: `CLOCK_CASES` gains the TSTZ-literal case on the fixture's zone-less timestamp column, since its `TZ` case moves to the instant-column scenario |
| A TIMESTAMP WITH LOCAL TIME ZONE literal in a comparison runs in the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_tstz_literal_comparison_runs_in_the_scan` (`TS` only) |
| The clock functions fold in the zone that Exasol uses when the session zone matches | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `clock_functions_match_native_when_the_session_zone_matches` (`D` and `TS` only) |
| A session zone that differs from the folding zone gets the folding zone's values | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_differing_session_zone_gets_the_folding_zone_values` (unchanged; it compares `TS`) |
| An unreadable TZ fails only a query that needs it | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `an_unreadable_tz_fails_only_a_query_that_needs_it` (adds the instant-column query that succeeds) |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-clock-functions | `exapump sql -d "$DSN" "ALTER SESSION SET TIME_ZONE = 'PACIFIC/KIRITIMATI'; EXPLAIN VIRTUAL SELECT ID FROM CLOCK_VS.<clock table> WHERE TZ > CAST(TIMESTAMP '2024-03-01 10:00:00' AS TIMESTAMP WITH LOCAL TIME ZONE)"` | The scan filter holds `2024-02-29 20:00:00.000` and no `SESSIONTIMEZONE` |
| session-time-zone | `exapump sql -d "$DSN" "ALTER VIRTUAL SCHEMA CLOCK_VS SET SESSION_TIME_ZONE = 'ASIA/TOKYO'; ALTER SESSION SET TIME_ZONE = 'PACIFIC/KIRITIMATI'; SELECT COUNT(*) FROM CLOCK_VS.<clock table> WHERE TZ > CURRENT_TIMESTAMP - INTERVAL '1' HOUR"` | The same count as without `SESSION_TIME_ZONE` and as a native TSTZ copy of the rows |

`$DSN` is `exasol://sys:exasol@<host>:<port>?validateservercertificate=0`.

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Test | `cargo test --workspace --lib` | 0 failures |
| E2E | `make e2e-up test-e2e` | 0 failures |
| Lint | `make lint` | 0 errors/warnings |
| Format | `make fmt-check` | No changes |

## Out of scope

- Declaring instant columns `TIMESTAMP WITH LOCAL TIME ZONE`, rejected in `intent.md` [D4].
- An expression over an instant column compared with a TSTZ value, such as `ADD_HOURS(tz, 1) > CURRENT_TIMESTAMP`, keeps the folding-zone rule ([D2]); `docs/semantic-differences.md` names it.
- The Exasol-dialect rendering of a projected TSTZ literal, `CAST(CONVERT_TZ(<utc>, 'UTC', SESSIONTIMEZONE) AS TIMESTAMP WITH LOCAL TIME ZONE)`, shows an instant in the repeated hour of a DST fall-back one hour off under `AMBIGUOUS ST`; `ADD_SECONDS(<UTC epoch as TSTZ>, SECONDS_BETWEEN(...))` is exact (#TBD, open question: needs an issue).
- A benchmark of instant-column conversion cost, moot with the plain declaration.
