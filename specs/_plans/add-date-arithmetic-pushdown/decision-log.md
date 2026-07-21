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

### [1] Split the issue #107 functions into 9 supported and 6 deferred by verified parity

- **Decision:** Support `ADD_DAYS`, `ADD_WEEKS`, `ADD_HOURS`, `ADD_MINUTES`, `ADD_YEARS`,
  `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, and `SECONDS_BETWEEN` with translator arms and
  `FN_*` capabilities. Defer `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`,
  `DAYOFWEEK`, and `CONVERT_TZ`, each with a named reason. `POSIX_TIME` stays unsupported (out of
  scope). `LAST_DAY` is excluded entirely — it is not an Exasol function (see review finding
  `[plan-review] LAST_DAY is not an Exasol function`).
- **Alternatives:** Advertise all as a block (rejected: violates the backing-path bar, risks
  silently wrong results). Defer everything until a full calendar-semantics layer exists (rejected:
  leaves verified, high-value pushdowns on the table).
- **Rationale:** The issue's bar permits advertising only functions with confirmed parity. Nine
  have a DataFusion 54 rendering verified against the Exasol reference; six have a documented parity
  or session-state divergence. This is the "split if parity holds only for a subset" outcome the
  issue invites.
- **Promotes to ADR:** yes

### [2] Use integer interval scaling for the ADD_* family; round the count first

- **Decision:** Render `ADD_DAYS`/`ADD_WEEKS`/`ADD_HOURS`/`ADD_MINUTES`/`ADD_YEARS` as
  `<x> + CAST(ROUND(<n>) AS BIGINT) * <unit interval>`.
- **Alternatives:** `Float × INTERVAL` scaling without rounding (rejected: DataFusion 54's float
  interval-scaling kernel is unverified; Exasol rounds the count for these functions anyway). Epoch
  arithmetic (rejected: unnecessary for whole-unit adds and loses the input's date/timestamp type).
- **Rationale:** DataFusion 54's `Interval × integer` coercion is verified against the v54 source;
  Exasol rounds decimals before adding for `ADD_DAYS/WEEKS/HOURS/MINUTES`. `ROUND` then `CAST … AS
  BIGINT` keeps the safe integer path and matches Exasol's rounding rule.
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
  advertisement. The `ADD_YEARS` leap-year clamp is the case most likely to diverge and is pinned
  by a dedicated E2E assertion.
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
  the six deferred functions are real Exasol functions with a genuine parity gap; `LAST_DAY` does
  not exist at all). `LAST_DAY` stays in the `unsupported_date_fn_falls_through` test (harmless
  generic fall-through) but appears in no capabilities.rs must-be-advertised assertion and gets no
  translator arm. Updated the supported count to nine and superseded Design Decision [1].
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
- **Direction change:** Kept bundled. The nine arms are homogeneous (each emits one canonical string
  pinned by a rendering unit test), share the same arity-check and fall-through pattern, and the
  E2E parity gate (task 3.1) is the real correctness check. Splitting would add task-list overhead
  without materially improving review of near-identical arms.
- **Promotes to ADR:** no
