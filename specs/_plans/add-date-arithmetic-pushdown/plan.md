# Plan: add-date-arithmetic-pushdown

## Summary

Advertise and translate the six Exasol date functions whose DataFusion 54.0.0 rendering both
executes and matches Exasol semantics, so date-heavy expressions push down instead of raw-scanning
(Closes #107). Nine functions stay deferred with a named execution, parity, or session-state
divergence; LAST_DAY is excluded because it is not an Exasol function.

## Design

### Context

Issue #107 flags fifteen-plus date functions the Exasol compiler can push but the adapter does not
advertise, forcing raw row scans for date-heavy queries. A capability is advertised only once it
has a verified `crates/vs-expression` translation AND its DataFusion result is confirmed to match
Exasol (the issue's "backing-path bar"). The mission encourages composing DataFusion expressions
to reproduce Exasol behavior rather than accepting "no native builtin" as final.

- **Goals** — Push down every date function whose DataFusion 54 rendering (native or composed) is
  verified to match Exasol, advertised per function, gated by an end-to-end parity test.
- **Non-Goals** — Advertise a function whose parity is unverified or session-state dependent;
  change the timestamp/timestamptz type mapping; touch `POSIX_TIME` (out of issue #107 scope).

### Decision

Split the issue #107 unadvertised functions into a supported subset and a deferred subset by
per-function research against the Exasol built-in function reference (verified on live Exasol
2025.1.3) and the DataFusion 54.0.0 (Arrow 58) function surface, verified against the pinned-tag
source and by executing each candidate rendering through DataFusion 54.0.0. Each supported function
gets a translator arm emitting a canonical DataFusion expression that is confirmed to execute and an
`FN_*` capability; each deferred function keeps falling through with a documented reason. One listed
name, `LAST_DAY`, is not an Exasol function at all (issue #107 listed it in error) and is excluded
from both subsets.

#### Architecture

```
Exasol compiler ──(function_scalar ADD_DAYS/…)──▶ vs-expression translator arm
                                                     │  renders canonical DataFusion SQL
                                                     ▼
                                          DataFusion scan (node-local)
capabilities.rs FN_* ──advertises──▶ Exasol pushes the node only when advertised
```

#### Patterns

| Pattern | Where | Why |
|---------|-------|-----|
| Capability gated on verified parity | `capabilities.rs` FN_* + E2E test | Matches WEEK/#115 precedent: advertise only while parity holds |
| Composed-expression rewrite | `vs-expression` `*_BETWEEN` | DataFusion 54 lacks a native between builtin; compose from `date_part('epoch', …)`/date subtraction |
| Integer-microsecond timestamp arithmetic | `ADD_HOURS`/`ADD_MINUTES` arms | DataFusion 54.0.0 rejects `Interval × Interval` at plan time (arrow-rs#9030); add the whole-second offset in the `Int64` microsecond domain via an `arrow_cast` round-trip, which executes and always yields a `TIMESTAMP` |
| Documented fall-through | deferred arms | Execution-broken, session-state, or input-type-dependent functions stay unsupported, named in Background |

### Consequences

| Decision | Alternatives Considered | Rationale |
|----------|------------------------|-----------|
| Support 6, defer 9, exclude LAST_DAY | Advertise all as a block | The bar forbids advertising unverified parity; block advertising risks silently wrong results; LAST_DAY has no Exasol function or capability to advertise |
| Render `ADD_HOURS`/`ADD_MINUTES` in the microsecond domain | `<x> + <n> * INTERVAL '<unit>'` | Interval scaling hard-errors at plan time in DataFusion 54.0.0 (`Interval(MonthDayNano) * Interval(MonthDayNano)`, arrow-rs#9030); the `arrow_cast` `Int64`-microsecond round-trip executes, always yields a `TIMESTAMP` (matching Exasol, which promotes a DATE argument), and preserves microsecond precision |
| Defer `ADD_DAYS`/`ADD_WEEKS` | Reuse the microsecond rendering (always `TIMESTAMP`) | Exasol returns the argument's type — DATE→DATE, TIMESTAMP→TIMESTAMP (verified on live Exasol 2025.1.3); the type-blind string translator has no argument-type information, so every execution-safe DataFusion 54.0.0 rendering collapses to one output type and would widen a DATE result to TIMESTAMP |
| Defer `ADD_YEARS` | Epoch/`365.25`-day arithmetic; year-interval add | Calendar-correct leap clamping (`2000-02-29`+1y → `2001-02-28`) needs interval-year addition, which requires the broken `Interval × Interval` multiply; epoch-second arithmetic is not calendar-correct; and its return type is input-type-dependent like `ADD_DAYS` |
| Defer `ADD_MONTHS` | Compose sticky-month-end via CASE | Exasol returns month-end when input is month-end; Arrow interval-month add does not — faithful rewrite is fragile |
| Defer `MONTHS_BETWEEN`/`YEARS_BETWEEN` | Compose Oracle fractional (day/31) | No native DF54 equivalent; fractional-over-31 + month-end special case is high-risk |
| Defer `ADD_SECONDS` | Epoch round-trip `to_timestamp` | Fractional count + precision truncation; `to_timestamp` normalizes to ns and attaches session TZ |
| Defer `DAYOFWEEK` | Render `date_part('dow')+1` | Correct only under default `NLS_FIRST_DAY_OF_WEEK`; the VS cannot observe the session parameter |
| Defer `CONVERT_TZ` | Compose `AT TIME ZONE`/`arrow_cast` | Session `TIME_ZONE_BEHAVIOR` dependency, Exasol-specific shift options, and `timestamptz`→plain `TIMESTAMP` mapping leave no per-value zone to convert |

## Features

| Feature | Status | Spec |
|---------|--------|------|
| sql-comprehension/vs-expression-translator-date-fns | CHANGED | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |

## Function disposition (issue #107)

Every function named in issue #107's not-advertised set (`capabilities.rs` lines 316-331) maps to
exactly one disposition. No function is silently dropped.

| Function | Disposition | Translator arm / reason |
|----------|-------------|-------------------------|
| ADD_HOURS | Supported | `arrow_cast(arrow_cast(arrow_cast(<x>, 'Timestamp(Microsecond, None)'), 'Int64') + CAST(ROUND(<n>) AS BIGINT) * 3600000000, 'Timestamp(Microsecond, None)')` |
| ADD_MINUTES | Supported | `arrow_cast(arrow_cast(arrow_cast(<x>, 'Timestamp(Microsecond, None)'), 'Int64') + CAST(ROUND(<n>) AS BIGINT) * 60000000, 'Timestamp(Microsecond, None)')` |
| DAYS_BETWEEN | Supported | `CAST(<a> AS DATE) - CAST(<b> AS DATE)` (→ `Int64`, confirmed) |
| HOURS_BETWEEN | Supported | `(date_part('epoch',<a>) - date_part('epoch',<b>)) / 3600` |
| MINUTES_BETWEEN | Supported | `(date_part('epoch',<a>) - date_part('epoch',<b>)) / 60` |
| SECONDS_BETWEEN | Supported | `date_part('epoch',<a>) - date_part('epoch',<b>)` |
| LAST_DAY | Not applicable | Not an Exasol function/capability (issue #107 listed it in error — verified against live Exasol 2025.1.3, `function or script LAST_DAY not found` SQL code 42000, and the `ScalarFunctionCapability` enum, which has no `LAST_DAY` member). No translator arm, no capability |
| ADD_DAYS | Deferred | Return type is input-type-dependent — Exasol returns DATE for a DATE argument, TIMESTAMP for a TIMESTAMP argument (live Exasol 2025.1.3); the type-blind string translator has no argument type, so no execution-safe DF54 rendering reproduces both |
| ADD_WEEKS | Deferred | Same input-type-dependent return type as `ADD_DAYS` |
| ADD_YEARS | Deferred | Leap-day calendar clamp needs interval-year addition (broken `Interval × Interval` multiply, arrow-rs#9030); epoch-second arithmetic is not calendar-correct; and the return type is input-type-dependent |
| ADD_SECONDS | Deferred | Fractional count + precision truncation; DF54 `Float × INTERVAL` unverified, epoch round-trip attaches session TZ |
| ADD_MONTHS | Deferred | Exasol month-end stickiness diverges from Arrow interval-month addition |
| MONTHS_BETWEEN | Deferred | Oracle-style fractional (day/31, month-end integer) has no DF54 equivalent |
| YEARS_BETWEEN | Deferred | Oracle-style fractional; no DF54 equivalent |
| DAYOFWEEK | Deferred | Depends on `NLS_FIRST_DAY_OF_WEEK` session parameter (VS cannot observe it); also not present on the target Exasol 2025.1.3 (`function or script DAYOFWEEK not found`, SQL code 42000) — see decision-log finding |
| CONVERT_TZ | Deferred | Session `TIME_ZONE_BEHAVIOR`/`SESSIONTIMEZONE` + Exasol shift options; `timestamptz`→`TIMESTAMP` drops per-value zone |

## Dependencies

- DataFusion 54.0.0 (Arrow 58.3.0) — already the workspace version. Verified against the pinned-tag
  source AND by executing each rendering through DataFusion 54.0.0 (see decision-log finding
  `[plan-review] ADD_* interval-multiply renderings are execution-broken`):
  - `DATE − DATE → Int64` day count — **confirmed** (`is_date_minus_date` in
    `datafusion/expr-common/src/type_coercion/binary.rs` returns `ret: Int64`; executed
    `CAST(ts AS DATE) - CAST(d AS DATE)` yields an `Int64`). This supersedes the earlier "assumed,
    not source-confirmed" note; the `DAYS_BETWEEN` E2E case (task 3.1) still guards the sign.
  - `date_part('epoch', …) → Float64` fractional seconds — confirmed
    (`datafusion/functions/src/datetime/date_part.rs`; executed result `Float64`).
  - `arrow_cast(<ts>, 'Int64')` ↔ `arrow_cast(<int>, 'Timestamp(Microsecond, None)')` round-trips
    exactly, and `arrow_cast(<Date32>, 'Timestamp(Microsecond, None)')` promotes a date to midnight
    — confirmed by execution; this is the mechanic behind the `ADD_HOURS`/`ADD_MINUTES` rendering.
  - **NOT usable:** `<integer> * INTERVAL '<unit>'` — DataFusion 54.0.0 rejects
    `Interval(MonthDayNano) * Interval(MonthDayNano)` at plan time (arrow-rs#9030, open, no
    milestone). This is why the ADD_* interval-scaling renderings were withdrawn.

## Implementation Tasks

1. Translate the supported date functions and pin renderings
   - [ ] 1.1 Add translator arms to `crates/vs-expression/src/lib.rs` for `ADD_HOURS`, `ADD_MINUTES`, `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, and `SECONDS_BETWEEN` emitting the canonical renderings from the disposition table, with arity checks that fall through (raise / `None`) on wrong argument counts; add rendering unit tests pinning each emitted string. `ADD_HOURS`/`ADD_MINUTES` MUST use the `arrow_cast` `Int64`-microsecond round-trip (`arrow_cast(arrow_cast(arrow_cast(<x>, 'Timestamp(Microsecond, None)'), 'Int64') + CAST(ROUND(<n>) AS BIGINT) * <unit_microseconds>, 'Timestamp(Microsecond, None)')`, `unit_microseconds` = `3600000000` for hours, `60000000` for minutes) and MUST NOT emit `<n> * INTERVAL '<unit>'` (rejected by DataFusion 54.0.0 at plan time, arrow-rs#9030). Do NOT add arms for `ADD_DAYS`, `ADD_WEEKS`, or `ADD_YEARS` — they are deferred (see disposition table) [expert]
2. Align advertised capabilities with the translated set
   - [ ] 2.1 In `crates/lakehouse-engine/src/adapter/capabilities.rs`, advertise `FN_ADD_HOURS`, `FN_ADD_MINUTES`, `FN_DAYS_BETWEEN`, `FN_HOURS_BETWEEN`, `FN_MINUTES_BETWEEN`, and `FN_SECONDS_BETWEEN`, moving these six names in `reports_audited_capability_set` from the must-NOT list to a must-be-advertised assertion, and keep `FN_ADD_DAYS`, `FN_ADD_WEEKS`, `FN_ADD_YEARS`, `FN_ADD_SECONDS`, `FN_ADD_MONTHS`, `FN_MONTHS_BETWEEN`, `FN_YEARS_BETWEEN`, `FN_DAYOFWEEK`, `FN_CONVERT_TZ` in the must-NOT-be-advertised assertion; in `crates/vs-expression/src/lib.rs` narrow the `unsupported_date_fn_falls_through` test to `ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`, `POSIX_TIME`, and `LAST_DAY`. `LAST_DAY` is a fall-through-only name (no translator arm, no `FN_LAST_DAY` — no such Exasol function or capability exists); it MUST NOT appear in any capabilities.rs must-be-advertised assertion. This single task owns all `reports_audited_capability_set` edits to avoid a concurrent-edit collision
3. Verify Exasol parity end to end
   - [ ] 3.1 Add end-to-end parity tests in `crates/lakehouse-engine/tests/e2e_capability_test.rs` executing each supported function through the VS against the seed table and asserting Exasol-matching values, with these cases pinned by literal arguments: (a) round-half-away-from-zero on the count — `ADD_HOURS(ts, 1.5)` → +2 hours and `ADD_HOURS(ts, 2.5)` → +3 hours (matching Exasol's "decimals are rounded before adding"); (b) `ADD_HOURS` and `ADD_MINUTES` on a DATE-typed input (not only TIMESTAMP), asserting the result is a TIMESTAMP at the promoted instant (e.g. `ADD_HOURS(DATE '2020-01-01', 5)` → `2020-01-01 05:00:00`), confirming the rendering's Date32→timestamp promotion matches Exasol; (c) `ADD_HOURS`/`ADD_MINUTES` on a TIMESTAMP input with a non-zero sub-second (microsecond) component, asserting the fractional seconds survive the microsecond round-trip; (d) one fractional `*_BETWEEN`, `HOURS_BETWEEN` between two timestamps 2.5 hours apart → `2.5`; (e) one negative-sign case, `DAYS_BETWEEN` with the first argument earlier than the second → a negative result. Withdraw the arm and its `FN_*` for any function whose result diverges from Exasol [expert]

## Parallelization

| Parallel Group | Tasks |
|----------------|-------|
| Group A | 1.1 |
| Group B | 2.1 |
| Group C | 3.1 |

Sequential dependencies:
- Group A → Group B (advertising requires the translator arms to exist)
- Group A, Group B → Group C (E2E requires the built `.so` with arms translated and capabilities advertised)

## Dead Code Removal

| Type | Location | Reason |
|------|----------|--------|
| None | — | The deferred functions were never implemented; they already fall through. No obsolete code is introduced or removed. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|----------|-----------|---------------|-----------|
| ADD_HOURS and ADD_MINUTES translate to microsecond-domain timestamp arithmetic | Unit | `crates/vs-expression/src/lib.rs` | `renders_add_hours_minutes_microsecond_domain` |
| ADD_HOURS and ADD_MINUTES translate to microsecond-domain timestamp arithmetic | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_add_hours_minutes_match_exasol` |
| DAYS_BETWEEN translates to a whole-day date difference | Unit | `crates/vs-expression/src/lib.rs` | `renders_days_between_as_date_difference` |
| DAYS_BETWEEN translates to a whole-day date difference | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_days_between_matches_exasol` |
| HOURS_BETWEEN, MINUTES_BETWEEN, and SECONDS_BETWEEN translate to epoch-second differences | Unit | `crates/vs-expression/src/lib.rs` | `renders_time_between_as_epoch_difference` |
| HOURS_BETWEEN, MINUTES_BETWEEN, and SECONDS_BETWEEN translate to epoch-second differences | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_time_between_matches_exasol` |
| Unsupported date functions fall through as unsupported nodes | Unit | `crates/vs-expression/src/lib.rs` | `unsupported_date_fn_falls_through` |
| Unsupported date functions fall through as unsupported nodes | Unit | `crates/lakehouse-engine/src/adapter/capabilities.rs` | `reports_audited_capability_set` |

### Manual Testing

| Feature | Command | Expected Output |
|---------|---------|-----------------|
| vs-expression-translator-date-fns | `cargo test -p vs-expression date` | Rendering unit tests for the `ADD_HOURS`/`ADD_MINUTES` and `*_BETWEEN` arms pass; `unsupported_date_fn_falls_through` passes for the nine deferred functions (`ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`), POSIX_TIME, and LAST_DAY |
| vs-expression-translator-date-fns | `make test-e2e` | E2E parity tests execute each supported function through the VS against the local Exasol container and match Exasol's values |

### Checklist

| Step | Command | Expected |
|------|---------|----------|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (E2E) | `make test-e2e` | 0 failures against the local Exasol container |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
