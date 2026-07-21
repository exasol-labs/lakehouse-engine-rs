# Decision Log: add-date-arithmetic-pushdown

## Interview

Headless (no live interview). The issue #107 body plus the "rewrite SQL to get matching behavior"
guidance is the interview record.

**Q:** Which date functions from issue #107 should push down, and which stay unsupported?
**A:** Advertise a function only once it has a verified `vs-expression` translation AND its
DataFusion result is confirmed to match Exasol (the issue's "backing-path bar"). Attempt composed
rewrites rather than accepting "no native builtin" as final. Split scope if parity holds only for a
subset. Per-function verification, not block advertisement.

**Q:** How should `CONVERT_TZ` interact with the timestamp mapping?
**A:** Factor in the project rule that Iceberg `timestamptz` maps to plain Exasol `TIMESTAMP`
(Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` type); decide whether that
leaves `CONVERT_TZ` supportable.

**Q:** Is `POSIX_TIME` in scope?
**A:** No. Issue #107 does not list it. Leave it unsupported, untouched.

**Q:** Is `WEEK`/`FN_WEEK` in scope?
**A:** No. Already advertised via PR #115. Out of scope here.

## Design Decisions

### [1] Split the issue #107 functions into a supported and a deferred subset by verified parity

- **Count SUPERSEDED twice:** first by `[plan-review] LAST_DAY is not an Exasol function` (removed
  `LAST_DAY`), then by `[plan-review] ADD_* interval-multiply renderings are execution-broken`, which
  moved `ADD_DAYS`, `ADD_WEEKS`, and `ADD_YEARS` to Deferred. The final split is **six supported**
  (`ADD_HOURS`, `ADD_MINUTES`, `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN`)
  and **nine deferred** (`ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`,
  `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`). The split-by-verified-parity method
  below is unchanged; only the membership shifted.
- **Original decision (kept for history):** Support `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`,
  `ADD_MINUTES`, `ADD_YEARS`, `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, and `SECONDS_BETWEEN`
  with translator arms and `FN_*` capabilities. Defer `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`,
  `YEARS_BETWEEN`, `DAYOFWEEK`, and `CONVERT_TZ`, each with a named reason. `POSIX_TIME` stays
  unsupported (out of scope). `LAST_DAY` is excluded entirely — it is not an Exasol function (see
  review finding `[plan-review] LAST_DAY is not an Exasol function`).
- **Alternatives:** Advertise all as a block (rejected: violates the backing-path bar, risks
  silently wrong results). Defer everything until a full calendar-semantics layer exists (rejected:
  leaves verified, high-value pushdowns on the table).
- **Rationale:** The issue's bar permits advertising only functions with confirmed parity. Nine
  have a DataFusion 54 rendering verified against the Exasol reference; six have a documented parity
  or session-state divergence. This is the "split if parity holds only for a subset" outcome the
  issue invites.
- **Promotes to ADR:** yes

### [2] Use integer interval scaling for the ADD_* family; round the count first

- **SUPERSEDED** by review finding `[plan-review] ADD_* interval-multiply renderings are
  execution-broken`. The `<x> + CAST(ROUND(<n>) AS BIGINT) * <unit interval>` rendering hard-errors
  at plan time in DataFusion 54.0.0 (`Interval(MonthDayNano) * Interval(MonthDayNano)`,
  arrow-rs#9030); the "`Interval × integer` coercion is verified" rationale was wrong — coercion
  succeeds at plan-typing but the multiply kernel is unimplemented. `ADD_HOURS`/`ADD_MINUTES` now
  render in the microsecond domain; `ADD_DAYS`/`ADD_WEEKS`/`ADD_YEARS` are deferred.
- **Original decision (kept for history):** Render
  `ADD_DAYS`/`ADD_WEEKS`/`ADD_HOURS`/`ADD_MINUTES`/`ADD_YEARS` as
  `<x> + CAST(ROUND(<n>) AS BIGINT) * <unit interval>`.
- **Promotes to ADR:** no

### [3] Defer ADD_SECONDS despite being a fixed-length unit

- **Decision:** Leave `ADD_SECONDS` unsupported.
- **Alternatives:** Integer interval scaling like the other ADD_* (rejected: would round away the
  fractional-second count that distinguishes `ADD_SECONDS`). Epoch round-trip via `to_timestamp`
  (rejected: `to_timestamp` normalizes to nanoseconds and attaches the session time zone, and
  DataFusion 54 `Float × INTERVAL` scaling is unverified).
- **Rationale:** `ADD_SECONDS` uniquely accepts a fractional count truncated to the first
  argument's precision; no DataFusion 54 rendering reproduces that with verified parity.
- **Promotes to ADR:** no

### [4] Defer the calendar-arithmetic functions ADD_MONTHS, MONTHS_BETWEEN, YEARS_BETWEEN

- **Decision:** Leave `ADD_MONTHS`, `MONTHS_BETWEEN`, and `YEARS_BETWEEN` unsupported.
- **Alternatives:** Compose the Oracle-style semantics (sticky month-end for `ADD_MONTHS`;
  day-fraction over 31 with a month-end integer special case for the `*_BETWEEN`) via
  `CASE`/`LAST_DAY`/`date_part` (rejected as high-risk for a first pass).
- **Rationale:** Exasol's month-end stickiness and the Oracle-style fractional month/year
  difference diverge from DataFusion 54 / Arrow's plain interval-month addition and have no native
  equivalent. These are the "calendar semantics are the risk" functions the issue flags; a faithful
  rewrite is fragile and warrants a focused follow-up rather than blocking this plan.
- **Promotes to ADR:** no

### [5] Defer DAYOFWEEK on session-parameter dependency

- **Decision:** Leave `DAYOFWEEK` unsupported.
- **Alternatives:** Render `date_part('dow', <x>) + 1` (rejected: correct only under the default
  `NLS_FIRST_DAY_OF_WEEK` = Sunday; a session set to Monday-first would make the pushdown silently
  wrong).
- **Rationale:** DataFusion 54 `date_part('dow')` is Sunday=0; `+1` matches Exasol's default
  numbering, but Exasol `DAYOFWEEK` depends on the `NLS_FIRST_DAY_OF_WEEK` session parameter the VS
  cannot observe. Parity is not guaranteed, so the function stays unsupported (same reasoning class
  as `CONVERT_TZ`).
- **Promotes to ADR:** no

### [6] Defer CONVERT_TZ on session state and the timestamptz mapping

- **Decision:** Leave `CONVERT_TZ` unsupported.
- **Alternatives:** Compose `AT TIME ZONE` / `arrow_cast` timezone attachment (rejected: no
  DataFusion 54 `(naive, from_tz, to_tz) → naive` function; casting a zone-aware timestamp back to
  naive strips the zone and cross-zone comparison errors).
- **Rationale:** Exasol `CONVERT_TZ` depends on the `TIME_ZONE_BEHAVIOR` session value (and
  `SESSIONTIMEZONE` for the local-time-zone input type) and Exasol-specific invalid/ambiguous shift
  options. The project maps Iceberg `timestamptz` — Iceberg spec: "a date and a time of day with a
  timezone", stored as UTC — to plain Exasol `TIMESTAMP`, so no per-value zone survives to convert.
- **Promotes to ADR:** yes

### [7] Gate each supported capability on an end-to-end parity test (WEEK precedent)

- **Decision:** Advertise each `FN_*` only while an E2E test confirms the rendered expression
  matches Exasol; withdraw the arm and its capability if a case diverges.
- **Alternatives:** Advertise on the strength of unit tests alone (rejected: unit tests pin the
  emitted string, not Exasol runtime parity).
- **Rationale:** Mirrors the `FN_WEEK` / PR #115 precedent, where ISO-8601 parity gates
  advertisement. The `ADD_HOURS`/`ADD_MINUTES` DATE-input promotion and the fractional `*_BETWEEN`
  cases are the ones most likely to diverge and are pinned by dedicated E2E assertions. (`ADD_YEARS`
  was superseded to Deferred by `[plan-review] ADD_* interval-multiply renderings are
  execution-broken` and no longer has an E2E case.)
- **Promotes to ADR:** no

## Review Findings

### [plan-review] LAST_DAY is not an Exasol function

- **Finding (BLOCKER):** The plan advertised `FN_LAST_DAY` and gave `LAST_DAY` a translator arm and
  an E2E parity case, but `LAST_DAY` is not an Exasol function or advertisable capability. Verified
  three ways: live Exasol 2025.1.3 returns `function or script LAST_DAY not found` (SQL code 42000);
  Exasol's built-in function docs do not list it (it is an Oracle function); and the authoritative
  `ScalarFunctionCapability` enum has no `LAST_DAY` member. Issue #107 listed `FN_LAST_DAY` in error.
  Consequences: the translator arm would be dead code (Exasol never emits `function_scalar LAST_DAY`),
  advertising `FN_LAST_DAY` would advertise a phantom capability, and task 3.1's `SELECT LAST_DAY(col)`
  parity test cannot compile against Exasol, so its own verification gate can never pass.
- **Direction change:** Removed `LAST_DAY` from the Supported set. Dropped its translator arm from
  task 1.1, dropped `FN_LAST_DAY` from the capability task, dropped its E2E case from task 3.1,
  deleted its NEW spec scenario and both Scenario-Coverage rows. Reclassified `LAST_DAY` in the
  disposition table as a third category, **Not applicable** (distinct from Supported and Deferred —
  the deferred functions are real Exasol functions with a genuine parity gap; `LAST_DAY` does
  not exist at all). `LAST_DAY` stays in the `unsupported_date_fn_falls_through` test (harmless
  generic fall-through) but appears in no capabilities.rs must-be-advertised assertion and gets no
  translator arm. Updated the supported count (nine at the time of this finding; later six — see
  `[plan-review] ADD_* interval-multiply renderings are execution-broken`) and superseded Design
  Decision [1].
- **Promotes to ADR:** no

### [plan-review] Soften the unverified DATE − DATE claim

- **Finding (ADVISORY):** The Dependencies section asserted `DATE − DATE → Int64` for DataFusion 54
  as a "verified surface" without citing evidence.
- **Direction change:** Moved `DATE − DATE` out of the verified list into an explicitly-labelled
  assumed surface, with a note that the `DAYS_BETWEEN` E2E case (task 3.1) confirms or refutes it and
  the arm is withdrawn per the parity gate if the subtraction yields an interval instead of an
  integer day count.
- **Promotes to ADR:** no

### [plan-review] Cover a DATE-typed input for ADD_HOURS/ADD_MINUTES

- **Finding (ADVISORY):** Exasol `ADD_HOURS`/`ADD_MINUTES` accept a DATE argument (returning a
  TIMESTAMP), but the E2E and seed table exercised only TIMESTAMP-typed columns.
- **Direction change:** Task 3.1 now requires an `ADD_HOURS`/`ADD_MINUTES` case on a DATE-typed
  input, confirming Date32→timestamp promotion in DataFusion.
- **Promotes to ADR:** no

### [plan-review] Pin fractional, negative-sign, and half-rounding E2E cases

- **Finding (ADVISORY):** Task 3.1's enumeration named only the `ADD_YEARS` leap-year case, thin
  given this plan's central risk is silent wrong results.
- **Direction change:** Task 3.1 now pins with literal arguments: a fractional `*_BETWEEN`
  (`HOURS_BETWEEN` 2.5 hours apart → `2.5`), a negative-sign case (`DAYS_BETWEEN` first arg earlier
  → negative), and round-half-away-from-zero rounding (`ADD_DAYS(d, 1.5)` → +2, `ADD_DAYS(d, 2.5)`
  → +3), alongside the retained leap-year clamp.
- **Promotes to ADR:** no

### [plan-review] Remove the capabilities.rs concurrent-edit collision

- **Finding (ADVISORY):** Tasks 2.1 and 2.2 both edited the `reports_audited_capability_set` test in
  capabilities.rs yet sat in the same parallel Group B — a collision risk for concurrent implementers.
- **Direction change:** Merged 2.1 and 2.2 into a single capability-surgery task (2.1) that owns all
  `reports_audited_capability_set` edits, removing the collision. Group B now holds one task.
- **Promotes to ADR:** no

### [plan-review] Keep the ADD_* / *_BETWEEN rendering families bundled in task 1.1

- **Finding (ADVISORY, optional):** Task 1.1 bundles the `ADD_*` and `*_BETWEEN` rendering families
  into one `[expert]` task; the reviewer noted splitting would improve reviewability.
- **Direction change:** Kept bundled. The arms are homogeneous (each emits one canonical string
  pinned by a rendering unit test — six after `[plan-review] ADD_* interval-multiply renderings are
  execution-broken`; nine when this finding was written), share the same arity-check and
  fall-through pattern, and the E2E parity gate (task 3.1) is the real correctness check. Splitting
  would add task-list overhead without materially improving review of near-identical arms.
- **Promotes to ADR:** no

### [plan-review] ADD_* interval-multiply renderings are execution-broken

- **Finding (BLOCKER):** All five "Supported" `ADD_*` renderings used
  `<x> + CAST(ROUND(<n>) AS BIGINT) * INTERVAL '<unit>'`. This does not diverge subtly — it HARD-ERRORS
  before execution in the workspace-pinned DataFusion 54.0.0. Integer × Interval coercion succeeds at
  plan-typing (both operands coerce to `Interval(MonthDayNano)`), but the `Interval(MonthDayNano) *
  Interval(MonthDayNano)` multiply kernel is unimplemented (arrow-rs#9030, open, no milestone).
  Verified three ways at tag 54.0.0: (a) the sqllogictest
  `datafusion/sqllogictest/test_files/datetime/arith_interval_double.slt` documents `SELECT interval
  '1 day' * 21` as `query error Invalid interval arithmetic operation: Interval(MonthDayNano) *
  Interval(MonthDayNano)`; (b) executing `ts + CAST(ROUND(1) AS BIGINT) * INTERVAL '1 hour'` through
  DataFusion 54.0.0 fails at planning with `Cannot get result type for temporal operation Int64 *
  Interval(MonthDayNano): Invalid interval arithmetic operation`; (c) both operand orders hit the
  same type-based path, so every unit (`day`/`hour`/`minute`/`year`) fails identically. Because
  capabilities (task 2.1) advertise BEFORE the E2E gate (task 3.1) runs, shipping as-is would let a
  real `ADD_DAYS`/etc. query push down and hard-error at the UDF. A secondary defect compounds it:
  `Date32 + INTERVAL '1 hour'` stays `Date32` in DataFusion 54.0.0 (confirmed via
  `arith_date_interval.slt` and by execution — `date + interval '1 hour'` → `2026-01-01`, `Date32`),
  so even with the multiply fixed, `ADD_HOURS`/`ADD_MINUTES` on a DATE argument would silently drop
  the sub-day offset.
- **Direction change:**
  - **`ADD_HOURS`, `ADD_MINUTES` → kept Supported, new rendering.** Render in the integer-microsecond
    domain: `arrow_cast(arrow_cast(arrow_cast(<x>, 'Timestamp(Microsecond, None)'), 'Int64') +
    CAST(ROUND(<n>) AS BIGINT) * <unit_microseconds>, 'Timestamp(Microsecond, None)')`
    (`3600000000` for hours, `60000000` for minutes). Verified by execution through DataFusion
    54.0.0: it runs (no interval multiply), preserves microsecond precision, and always yields a
    `TIMESTAMP` — including promoting a Date32 argument to a midnight timestamp
    (`ADD_HOURS(DATE '2020-01-01', 5)` → `2020-01-01 05:00:00`). This matches Exasol, which returns a
    TIMESTAMP for `ADD_HOURS`/`ADD_MINUTES` on both DATE and TIMESTAMP inputs (verified on live
    Exasol 2025.1.3), and it fixes the secondary Date32-stays-Date32 defect by construction. Named
    trade-off: normalization to microseconds truncates a nanosecond-precision Iceberg v3
    `timestamp_ns` argument's sub-microsecond part — consistent with the project's microsecond
    timestamp mapping and the #155 literal-precision fix.
  - **`ADD_DAYS`, `ADD_WEEKS` → moved to Deferred.** Exasol's return type is input-type-dependent:
    `ADD_DAYS(DATE '2020-01-01', 5)` → `2020-01-06` (DATE), `ADD_DAYS(TIMESTAMP '2020-01-01
    12:34:56', 5)` → `2020-01-06 12:34:56` (TIMESTAMP) — verified on live Exasol 2025.1.3. The
    `vs-expression` translator renders SQL from the pushdown expression tree with no argument type
    (column nodes carry only `name`/`tableAlias`), so a single rendering cannot return DATE for a
    DATE argument and TIMESTAMP for a TIMESTAMP argument. The only type-preserving primitive
    (`<x> + <interval>`) needs the broken runtime interval scale; every execution-safe rendering
    routes through `TIMESTAMP` and would widen a DATE result. Deferred rather than shipping a
    type-widening rendering (the user's bar: do not ship technically-executing-but-still-wrong). The
    unblock condition is a type-aware translator (the adapter already annotates column nodes with
    `tableAlias`, and `crates/lakehouse-engine/src/adapter/iceberg_predicate.rs`'s `resolve_column`
    already resolves per-column Iceberg types from the scan schema elsewhere in the pipeline — so
    type data exists today, but the shared `vs-expression` translator crate is not wired to receive
    it and cannot infer the type of an arbitrary, possibly non-column argument expression; an
    analogous `dataType` annotation threaded into the translator would let the arm branch).
  - **`ADD_YEARS` → moved to Deferred.** The initial reason (leap clamp needs interval-year addition,
    which hits arrow-rs#9030) was corrected — see the follow-up finding
    `[plan-review] ADD_YEARS defers on month-end stickiness, not the interval-multiply gap`. The
    accurate reason: Exasol applies month-end stickiness that no execution-safe DataFusion 54.0.0
    rendering reproduces, the same divergence class as `ADD_MONTHS`. Its return type is also
    input-type-dependent like `ADD_DAYS`. Deferred per the same defer-honestly precedent as
    `ADD_MONTHS`.
  - Supported count updated from nine to **six** (`ADD_HOURS`, `ADD_MINUTES`, `DAYS_BETWEEN`,
    `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN`); deferred count updated to **nine**
    (adds `ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`). Supersedes Design Decisions [1] (count) and [2]
    (ADD_* rendering). Updated the plan disposition table, task 1.1 rendering description, task 2.1
    capability lists, task 3.1 E2E cases (dropped the `ADD_YEARS` leap case and the `ADD_DAYS`
    half-rounding case; the rounding and DATE-input-promotion cases now use `ADD_HOURS`/`ADD_MINUTES`,
    plus a new sub-second-preservation case), the Scenario Coverage table, and the spec deltas.
- **Promotes to ADR:** yes

### [plan-review] Confirm the DATE − DATE and epoch renderings against DataFusion 54.0.0 source

- **Finding (ADVISORY → resolved):** The plan carried `DAYS_BETWEEN`'s `DATE − DATE → Int64` as
  "assumed, not yet source-confirmed" (per an earlier advisory) and `date_part('epoch', …) → Float64`
  as merely "verified surface" without a citation.
- **Direction change:** Both are now source- and execution-confirmed at tag 54.0.0.
  `DATE − DATE → Int64`: `is_date_minus_date` in
  `datafusion/expr-common/src/type_coercion/binary.rs` returns `ret: Int64`, and an executed
  `CAST(<ts> AS DATE) - CAST(<date> AS DATE)` returns an `Int64` (value `0` for equal dates).
  `date_part('epoch', …) → Float64`: `datafusion/functions/src/datetime/date_part.rs`, and an
  executed `date_part('epoch', ts)` returns a `Float64` fractional-second value. The Dependencies
  section is upgraded from "assumed" to "confirmed"; the `DAYS_BETWEEN` E2E case now guards only the
  sign convention, not the result type.
- **Promotes to ADR:** no

### [plan-review] DAYOFWEEK may not exist on the target Exasol version

- **Finding (ADVISORY):** `DAYOFWEEK` was deferred solely on the `NLS_FIRST_DAY_OF_WEEK`
  session-parameter dependency. Independent verification against the same Exasol version the E2E
  harness targets found a second, more basic reason: `SELECT DAYOFWEEK(DATE '2020-01-01')` on live
  Exasol 2025.1.3 returns `function or script DAYOFWEEK not found` (SQL code 42000). `DAYOFWEEK` is
  documented on Exasol's current docs site but is not present on this project's target Exasol
  version.
- **Direction change:** `DAYOFWEEK` stays deferred (disposition unchanged), but the recorded reason
  now names the version caveat alongside the session-parameter dependency, so a future revisit checks
  the function exists on the target Exasol version before treating it as only a session-parameter
  problem. (No spec/plan wording change beyond this log entry; the function was already fall-through.)
- **Promotes to ADR:** no

### [plan-review] ADD_YEARS defers on month-end stickiness, not the interval-multiply gap

- **Finding (BLOCKER):** An independent verification pass — every candidate rendering re-checked
  against pinned-tag Arrow 58.3.0 / DataFusion 54.0.0 source and against live Exasol 2025.1.3 —
  found the recorded `ADD_YEARS` deferral reason technically wrong and misleading. The prior reason
  claimed leap-day clamping "only interval-year addition reproduces, and runtime-scaled interval
  addition hits arrow-rs#9030." Two facts refute this: (a) a year-interval builds WITHOUT any runtime
  multiply via `arrow_cast(<months_int>, 'Interval(YearMonth)')` — Arrow 58 permits
  `Int32 → Interval(YearMonth)` (`arrow-cast/src/cast/mod.rs`, `can_cast_types`), so arrow-rs#9030
  never applies to this path; (b) `Date`/`Timestamp` + `Interval(YearMonth)` addition IS implemented
  and clamps the leap case correctly (`arrow-array/src/types.rs` `add_year_months` via chrono
  `add_months_datetime`; `arrow-arith/src/numeric.rs`), so `ADD_YEARS(DATE '2000-02-29', 1)` →
  `2001-02-28` is reproducible and execution-safe. The stated blocker (arrow-rs#9030) is therefore
  false. Left uncorrected, a future planner could re-attempt `ADD_YEARS` once arrow-rs#9030 closes
  and ship a rendering that is still wrong.
- **Direction change:** Corrected the `ADD_YEARS` reason across `plan.md` (Consequences and
  disposition table), `spec.md` (Background), and the prior interval-multiply finding to the verified
  root cause: Exasol applies **month-end stickiness**, the same divergence class as `ADD_MONTHS`.
  `ADD_YEARS(DATE '2001-02-28', 3)` returns `2004-02-29` on live Exasol 2025.1.3 (a last-day-of-month
  argument maps to the last day of the target month), whereas Arrow's `Interval(YearMonth)` add keeps
  the day-of-month and yields `2004-02-28`. No execution-safe DataFusion 54.0.0 rendering reproduces
  this stickiness; the return type is also input-type-dependent like `ADD_DAYS`. `ADD_YEARS` stays
  Deferred (disposition and count unchanged) on the same defer-honestly precedent as `ADD_MONTHS`.
- **Verification note:** The pass also re-confirmed, at tag 54.0.0 and against live Exasol 2025.1.3,
  every other disposition already recorded: the interval-multiply defect
  (`type_coercion/binary.rs` coerces `Integer × Interval` to `Interval(MonthDayNano) ×
  Interval(MonthDayNano)`); `DATE − DATE → Int64`; `date_part('epoch', …) → Float64`; the three
  `arrow_cast` casts the `ADD_HOURS`/`ADD_MINUTES` microsecond-domain rendering depends on
  (`Date32 → Timestamp(µs)`, `Timestamp → Int64` exact reinterpret, `Int64 → Timestamp`); ROUND as
  round-half-away-from-zero; `ADD_DAYS`/`ADD_WEEKS` input-type-dependent return types
  (`ADD_DAYS(DATE '2024-01-01', 5)` → DATE `2024-01-06`); the fractional/negative `*_BETWEEN` values;
  and `DAYOFWEEK` absent on the target version (SQL code 42000). No other disposition changed.
- **Promotes to ADR:** no
