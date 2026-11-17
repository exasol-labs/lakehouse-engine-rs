# Plan: add-clock-function-pushdown

## Summary

The adapter advertises `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `SYSDATE`, and `SYSTIMESTAMP`, folds each call into a date or timestamp literal from one clock reading per statement, and replaces a compared `TIMESTAMP WITH LOCAL TIME ZONE` literal with the plain timestamp it stands for, so filters over either prune files and run in the scan (#641, #242). A new `SESSION_TIME_ZONE` virtual schema property names the zone that stands in for the session zone, and the `TZ` zone (`DBTIMEZONE`) does when it is unset.

## Context

- A bare clock call is never pushed: `WHERE d = CURRENT_DATE` and `WHERE ts > SYSTIMESTAMP` read every file (verified live on 8.29.13).
- Exasol folds any expression over a clock function before the request, in its own clock and the session zone: `CURRENT_DATE + 1`, `CURRENT_DATE - 7`, `ADD_DAYS(CURRENT_DATE, -7)`, and `ADD_HOURS(SYSTIMESTAMP, -1)` arrive as plain literals and already prune (verified live).
- Exasol sends a `TIMESTAMP WITH LOCAL TIME ZONE` value as `literal_timestamputc` holding the UTC instant: `CAST(TIMESTAMP '…' AS TIMESTAMP WITH LOCAL TIME ZONE)`, `ADD_HOURS(CURRENT_TIMESTAMP, -1)`, `CURRENT_TIMESTAMP - INTERVAL '1' HOUR`, and `LOCALTIMESTAMP - INTERVAL '1' HOUR`. The scan declines it, because comparing it with a plain `TIMESTAMP` reads the plain side in the session zone, so such a filter runs in Exasol and prunes nothing (#242, verified live).
- Exasol's parser reads `SET TIME_ZONE` in `ALTER VIRTUAL SCHEMA ... SET TIME_ZONE = ...` as its own token and refuses a quoted property name, so a property named `TIME_ZONE` could be set at CREATE only. The property is `SESSION_TIME_ZONE` (verified live on 8.29.13).
- A single-group select list with a constant beside an aggregate (`SELECT SYSDATE, COUNT(*)`) declines the partial/merge split to the wrapper today.
- The adapter VM sees `DBTIMEZONE` through `TZ`, but no request carries `SESSIONTIMEZONE` (measurements in #641).
- The adapter already rewrites request nodes before planning (`adapter/pushdown/binary_filter.rs` synthesizes `literal_binary`), and every planning path, pruning included, handles `literal_date` and `literal_timestamp`.
- `chrono-tz` 0.10.4 is in the dependency graph through `arrow-array` and embeds the tz database. The 596 `EXA_TIME_ZONES` names of Exasol 8.29.13 and the 597 of 2025.1.16 together are exactly its 597 zone names.
- Exasol sends each clock function as a `function_scalar` node. Exasol 2025.1.16 gives the three timestamp functions one `literal_exactnumeric` precision argument (3 unless the call names one) and declares that precision; Exasol 8.29.13 sends no argument. Exasol 8.29.13 declares a projected `LOCALTIMESTAMP` `TIMESTAMP WITH LOCAL TIME ZONE`, equal to `CURRENT_TIMESTAMP`, and Exasol 2025.1.16 declares it `TIMESTAMP` (captured live).
- `capabilities.rs`, `scalar_functions.rs`, and `docs/capabilities.md` state that the adapter sees neither `SESSIONTIMEZONE` nor `DBTIMEZONE`. The `DBTIMEZONE` half is wrong.
- Architecture: the pushdown context and the per-statement state gain the clock (`architecture.md` in this plan).

## Features

| Feature | Status | Spec |
|---------|--------|------|
| pushdown/pushdown-planning-clock-functions | NEW | `pushdown/pushdown-planning-clock-functions/spec.md` |
| pushdown-capabilities/pushdown-planning-capability-extensions | CHANGED | `pushdown-capabilities/pushdown-planning-capability-extensions/spec.md` |
| sql-comprehension/vs-expression-translator-literals | CHANGED | `sql-comprehension/vs-expression-translator-literals/spec.md` |
| sql-functions/vs-expression-translator-date-fns | CHANGED | `sql-functions/vs-expression-translator-date-fns/spec.md` |
| sql-functions/vs-expression-translator-date-diff-fns | CHANGED | `sql-functions/vs-expression-translator-date-diff-fns/spec.md` |
| sql-functions/vs-expression-translator-greatest-least | CHANGED | `sql-functions/vs-expression-translator-greatest-least/spec.md` |
| pushdown-aggregates/pushdown-planning-single-group-agg | CHANGED | `pushdown-aggregates/pushdown-planning-single-group-agg/spec.md` |
| vs-adapter/create-virtual-schema | CHANGED | `vs-adapter/create-virtual-schema/spec.md` |
| vs-properties/property-name-validation | CHANGED | `vs-properties/property-name-validation/spec.md` |
| vs-properties/property-value-validation | CHANGED | `vs-properties/property-value-validation/spec.md` |

## Impact

- A filter on a bare clock function, or a comparison with a `TIMESTAMP WITH LOCAL TIME ZONE` literal (including `ADD_HOURS(CURRENT_TIMESTAMP, -1)` and `CURRENT_TIMESTAMP - INTERVAL …`), now prunes files and filters in the scan.
- Behavior change: in a session whose `SESSIONTIMEZONE` differs from the folding zone (`SESSION_TIME_ZONE`, or `DBTIMEZONE` when it is unset), `CURRENT_DATE`, and on Exasol 2025 `LOCALTIMESTAMP`, over a virtual table return the folding zone's values, and a comparison with `CURRENT_TIMESTAMP` or a `TIMESTAMP WITH LOCAL TIME ZONE` literal compares in the folding zone. Before, both followed the session. A projected `CURRENT_TIMESTAMP` or TSTZ literal still shows the session's native value. `docs/semantic-differences.md` documents it, and `EXCLUDED_CAPABILITIES` restores native evaluation for the clock functions (`FN_CURRENT_DATE,FN_CURRENT_TIMESTAMP,FN_LOCALTIMESTAMP`) and for TSTZ literals (`LITERAL_TIMESTAMP_UTC`).
- In one query, an expression over a clock function (`CURRENT_DATE - 7`) is folded by Exasol in the session zone, and a bare call by the adapter in the folding zone. They differ only when the two zones differ.
- A clock value now comes from the adapter node's OS clock when it plans the statement, not from Exasol's statement clock.
- A query that needs the `TZ` zone fails when `TZ` is empty, unset, or unreadable. Exasol always sets `TZ`, so this needs a broken `%env TZ` line.
- New property `SESSION_TIME_ZONE`. An existing virtual schema needs no migration.

## Dependencies

- `chrono-tz` as a direct dependency of `lakehouse-engine`, pinned in the root `[workspace.dependencies]` at the version `arrow-array` already resolves. Its `case-insensitive` feature is not enabled, because it generates the zone table at build time through `chrono-tz-build`; a case-insensitive scan of the 597 zones does the same.

## Implementation

1. Capture the request nodes first. Advertise the five capabilities on the local stack, then capture with `scripts/capture-pushdown-payload.sh` how Exasol 8 and 2025 send each function (name, arguments, any precision argument), and the requests of `WHERE d = CURRENT_DATE`, `WHERE ts > CURRENT_TIMESTAMP`, `SELECT CURRENT_TIMESTAMP`, and `SELECT CURRENT_TIMESTAMP, region, COUNT(*) … GROUP BY region`. Confirm that Exasol still folds `CURRENT_DATE - 7` and `ADD_HOURS(CURRENT_TIMESTAMP, -1)` into literals once the functions are advertised. Adjust D2 and D4 of `intent.md` if a shape differs.
2. `adapter/time_zone.rs`: one type for a resolved zone (a `chrono_tz::Tz` or a fixed offset). A `SESSION_TIME_ZONE` value resolves through `chrono-tz` without regard to case (D5). `TZ` parses as any tz database name, or as `UTC±H[:MM]` or `Etc/GMT±N` with the sign inverted (D11). It owns the wall-clock date and time of an instant in a zone.
3. `adapter/properties.rs`: add `SESSION_TIME_ZONE` to `PROPERTY_NAMES` and its value rule, reusing the existing invalid-value entry and the check of step 2.
4. `adapter/pushdown/clock.rs`: the statement clock (one `OnceLock` instant per adapter VM, D3) and the fold. The fold walks the pushdown request once. It replaces `CURRENT_DATE` and `SYSDATE` with `literal_date`, and `SYSTIMESTAMP` with `literal_timestamp`. Inside a select-list item that `selectListDataTypes` declares `TIMESTAMP WITH LOCAL TIME ZONE`, it replaces `CURRENT_TIMESTAMP` and `LOCALTIMESTAMP` with `literal_timestamputc` of the UTC instant and keeps a `literal_timestamputc`. Everywhere else it replaces `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, `literal_timestamputc`, and `literal_timestamp_utc` with the `literal_timestamp` of the instant's wall-clock time in the folding zone (D2). A folded timestamp has the digits of the precision argument, or three without one (D4). It resolves a zone only when a node needs it, and fails with the error of the "unreadable TZ" scenario.
5. `adapter/mod.rs` reads the statement clock and `TZ` in the `pushdown` arm and carries them in `PushdownContext`; `handle_pushdown` folds the request right after `PushdownRequest::parse`, the one reader of `selectListDataTypes`.
6. `adapter/pushdown/single_group_agg.rs`: a constant select-list item beside an aggregate decomposes with no nested aggregate, and the merge renders it.
7. `adapter/capabilities.rs`: advertise `FN_CURRENT_DATE`, `FN_CURRENT_TIMESTAMP`, `FN_LOCALTIMESTAMP`, `FN_SYSDATE`, and `FN_SYSTIMESTAMP`, and replace the withdrawal comment. Replace the comments in `vs-expression/src/scalar_functions.rs` and at the `literal_timestamputc` arm of `vs-expression/src/lib.rs`.
8. Docs: `tuning.md` (`SESSION_TIME_ZONE`), `semantic-differences.md` (the folding zone for clock functions and TSTZ comparisons, the clock source, the two zones in one query, and two readings when one statement spans two virtual schemas), `capabilities.md` (the clock row and the TSTZ literal row, with the corrected `DBTIMEZONE` claim), `capability-matrix.md` (the five rows, `LITERAL_TIMESTAMP_UTC`, and the `DBTIMEZONE` and `SESSIONTIMEZONE` notes), and `install.md` (a `%env TZ` line in the adapter script changes the zone of `SYSDATE` and `SYSTIMESTAMP`).

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| Test | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` `e2e_now_family_matches_native_oracle` | Superseded by the clock module's tests of statement constancy and native match |
| Test data | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` `reports_audited_capability_set` | Drop the four clock names from its not-advertised list, and add `FN_DBTIMEZONE` and `FN_SESSIONTIMEZONE` |
| Test | `crates/vs-expression/src/lib_tests.rs` `now_family_falls_through` | Keep it as the guard that a clock call the fold misses is refused, not computed: add `LOCALTIMESTAMP` to its names |
| Comment | `crates/lakehouse-engine/src/adapter/capabilities.rs:93`, `crates/vs-expression/src/scalar_functions.rs:266`, the `literal_timestamputc` arm of `crates/vs-expression/src/lib.rs` | State the withdrawal and decline reasons this plan changes |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| The five clock functions are advertised | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `clock_functions_are_advertised_and_withheld_by_exclusion` |
| A clock function reaches the pushed SQL as a literal | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `each_clock_function_reaches_the_pushed_sql_as_a_literal` |
| A TIMESTAMP WITH LOCAL TIME ZONE literal in a comparison runs in the scan | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_tstz_literal_comparison_runs_in_the_scan` |
| The clock functions fold in the zone that Exasol uses when the session zone matches | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `clock_functions_match_native_when_the_session_zone_matches` |
| SESSION_TIME_ZONE sets the zone of the session-zone functions | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `session_time_zone_sets_the_zone_of_the_session_zone_functions` |
| A session zone that differs from the folding zone gets the folding zone's values | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_differing_session_zone_gets_the_folding_zone_values` |
| A projected clock function keeps the aggregate split | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_projected_clock_function_keeps_the_aggregate_split` |
| Every clock function of a statement folds from one reading | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `every_clock_function_of_a_statement_folds_from_one_reading` |
| A filter on a clock function or a time zone literal prunes files at plan time | Integration | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `clock_and_tstz_filters_prune_files` |
| A numeric database time zone folds at its offset | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `a_numeric_tz_folds_at_its_offset` |
| An unreadable TZ fails only a query that needs it | Integration | `crates/lakehouse-e2e/tests/e2e_local/clock.rs` | `an_unreadable_tz_fails_only_a_query_that_needs_it` (the `%env TZ=;` empty case); the unset case, which no Exasol script can produce, is proved by the unit test `unreadable_tz_is_refused` |
| SESSION_TIME_ZONE accepts the zone names Exasol lists and rejects other values | Integration | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `session_time_zone_accepts_exasol_zone_names_and_rejects_others` |
| Projected constant whose declared EMITS type Exasol rejects routes to the qualified wrapper | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_projected_tstz_literal_matches_native_and_pruned_scan_succeeds` (unchanged) |
| Routed session-dependent projection returns the value Exasol computes natively | Integration | `crates/lakehouse-e2e/tests/e2e_local/capability.rs` | `e2e_clock_projection_matches_native_session_local_value` (renamed from `e2e_now_family_projection_matches_native_session_local_value`; sets the session zone to `DBTIMEZONE` and adds the two `EXPLAIN VIRTUAL` checks) |
| Literal nodes translate to SQL literal forms | Unit | `crates/vs-expression/src/lib_tests.rs` | the existing literal rendering tests (unchanged behavior) |
| Adapter reports its pushdown capabilities | Unit | `crates/lakehouse-engine/src/adapter/capabilities_tests.rs` | `reports_audited_capability_set` |
| A single-group request that does not decompose routes to the qualified wrapper | Integration | `crates/lakehouse-e2e/tests/e2e_local/pruning.rs` | `iceberg_single_group_aggregates_match_the_oracle` (the `SELECT 'X', COUNT(*)` shape now splits) |
| Every property the adapter reads is known under every catalog kind | Integration | `crates/lakehouse-e2e/tests/e2e_local/property_validation.rs` | `create_accepts_every_known_property_name` |

Unit tests of pure logic:

- `crates/lakehouse-engine/src/adapter/time_zone_tests.rs`: `tz_reads_zone_names_and_inverted_utc_offsets` (case table: `CET`, `Europe/Berlin`, `EUROPE/BERLIN`, `UTC-7` → +07:00, `UTC+5:30` → −05:30, `Etc/GMT-7` → +07:00, `UTC`), `unreadable_tz_is_refused` (`NOT/A_ZONE`, empty, unset, `UTC+x`), `every_exa_time_zones_name_is_a_time_zone_value`, and `every_time_zone_value_is_an_exa_time_zones_name` (over every `chrono-tz` zone, against the captured lists).
- `crates/lakehouse-engine/src/adapter/pushdown/clock_tests.rs`: `folds_each_clock_function_into_its_literal` (case table over the five names, with and without a precision argument), `tstz_values_fold_to_the_wall_clock_outside_tstz_select_items` (filter, `having`, join condition, aggregate argument, and a plain select-list item turn `CURRENT_TIMESTAMP` and `literal_timestamputc` into the folding-zone `literal_timestamp`; a TSTZ-declared select-list item keeps `literal_timestamputc`), and `a_request_without_a_clock_or_tstz_node_never_reads_tz`.

The `clock.rs` E2E module is declared in `crates/lakehouse-e2e/tests/e2e_local/main.rs`. The two `TZ` tests install their adapter scripts in their own schema through `create_schema_and_scripts_with_env`, so their `%env TZ` lines reach no other virtual schema. Value checks compare against the same session's native value or against `CONVERT_TZ` of native `SYSTIMESTAMP`, with a 60-second tolerance.

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| pushdown-planning-clock-functions | `EXAPUMP_DSN='exasol://sys:exasol@127.0.0.1:38563?validateservercertificate=0' exapump sql "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.EVENTS WHERE EVENT_DATE = CURRENT_DATE"` | The pushed SQL holds today's `DATE` literal (or prunes to `WHERE 1=0`) and no `CURRENT_DATE` |
| pushdown-planning-clock-functions | `exapump sql "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.EVENTS WHERE EVENT_TS > ADD_HOURS(CURRENT_TIMESTAMP, -1)"` | The scan filter holds a plain `TIMESTAMP` literal, no `SESSIONTIMEZONE`, and the plan prunes to `WHERE 1=0` |
| pushdown-planning-clock-functions | `exapump sql "ALTER VIRTUAL SCHEMA MY_LAKEHOUSE SET SESSION_TIME_ZONE = 'ASIA/TOKYO'"`, then `exapump sql "SELECT CURRENT_DATE, LOCALTIMESTAMP FROM MY_LAKEHOUSE.EVENTS LIMIT 1"` | Tokyo's date, and on Exasol 2025 Tokyo's wall clock (on Exasol 8 `LOCALTIMESTAMP` shows the session's native value), without a `REFRESH` |
| pushdown-planning-clock-functions | `exapump sql "ALTER VIRTUAL SCHEMA MY_LAKEHOUSE SET SESSION_TIME_ZONE = 'MARS/OLYMPUS'"` | Fails with `invalid value 'MARS/OLYMPUS' for 'SESSION_TIME_ZONE': expected a time zone name from EXA_TIME_ZONES` |
| pushdown-planning-capability-extensions | `exapump sql "SELECT CURRENT_TIMESTAMP FROM MY_LAKEHOUSE.EVENTS WHERE id = 1"` | One `TIMESTAMP WITH LOCAL TIME ZONE` value, equal to native `SELECT CURRENT_TIMESTAMP` within a second |
| property-name-validation, property-value-validation | `exapump sql "ALTER VIRTUAL SCHEMA MY_LAKEHOUSE SET SESSION_TIME_ZONE = 'europe/berlin'"` | Succeeds |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-udf-build` | Exit 0 |
| Unit tests | `cargo test --workspace --lib` | 0 failures |
| E2E | `make e2e-up test-e2e` (with this clone's stack overrides) | 0 failures |
| Lint | `make lint` | 0 errors or warnings |
| Format | `make fmt-check` | No changes |

## Out of scope

- Following the session time zone: neither the adapter nor a UDF can observe it.
- `DBTIMEZONE` and `SESSIONTIMEZONE` stay unadvertised.
- Date arithmetic over a clock function: Exasol folds it into a literal itself before the request.
