# Decisions: add-date-arithmetic-pushdown

## ADR: Split issue #107 date functions into a supported and a deferred subset by verified parity

**ID:** split-issue-107-date-functions-supported-deferred-by-verified-parity
**Plan:** add-date-arithmetic-pushdown
**Status:** Accepted

### Context

Issue #107 asked which Exasol date/time functions the VS expression translator should push down to
DataFusion. The project's backing-path bar permits advertising a function only once it has a
verified `vs-expression` translation AND its DataFusion result is confirmed to match Exasol. Two
review passes and a live-Exasol E2E parity run each found renderings that executed but diverged
from Exasol, narrowing the initially proposed set twice.

### Decision

Advertise pushdown for exactly four functions confirmed by E2E parity against live Exasol
2025.1.3: `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, `SECONDS_BETWEEN`. Defer eleven
functions named in issue #107 (`ADD_HOURS`, `ADD_MINUTES`, `ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`,
`ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, `CONVERT_TZ`), each
with a distinct named divergence reason. Treat `LAST_DAY` as not applicable — it is not an Exasol
function. Leave `POSIX_TIME` out of scope, unchanged.

### Options Considered

| Option | Verdict |
|--------|---------|
| Split by verified parity, function by function | ✓ Chosen — matches the issue's own bar and the project's capability invariant of advertising only what the engine can back correctly |
| Advertise all functions as a block | ✗ Rejected — violates the backing-path bar and risks silently wrong results |
| Defer everything until a full calendar-semantics layer exists | ✗ Rejected — leaves verified, high-value pushdowns (`*_BETWEEN`) on the table |

### Consequences

Users get correct pushdown for date-difference queries immediately. `ADD_*` date-arithmetic
pushdown stays a follow-up, gated on a type-aware translator that can vary rendering by argument
type (DATE vs. TIMESTAMP) — the seam (`resolve_column`'s per-column Iceberg type resolution)
already exists elsewhere in the adapter but is not yet threaded into the shared translator crate.

## ADR: Defer CONVERT_TZ on session state and the timestamptz mapping

**ID:** defer-convert-tz-session-state-timestamptz-mapping
**Plan:** add-date-arithmetic-pushdown
**Status:** Accepted

### Context

Issue #107 asked whether `CONVERT_TZ` could push down. Exasol's `CONVERT_TZ` depends on the
`TIME_ZONE_BEHAVIOR` session value (and `SESSIONTIMEZONE` for its local-time-zone input form) and
Exasol-specific invalid/ambiguous-shift options the VS cannot observe. Separately, the project maps
Iceberg `timestamptz` — "a date and a time of day with a timezone" per the Iceberg spec, stored as
UTC — to plain Exasol `TIMESTAMP`, so no per-value zone survives into the VS to convert.

### Decision

Leave `CONVERT_TZ` unsupported; it falls through for Exasol to post-process.

### Options Considered

| Option | Verdict |
|--------|---------|
| Leave unsupported, fall through to Exasol | ✓ Chosen — no per-value timezone data exists post-mapping, and session state is unobservable |
| Compose `AT TIME ZONE` / `arrow_cast` timezone attachment | ✗ Rejected — no DataFusion 54 `(naive, from_tz, to_tz) → naive` function exists; casting a zone-aware timestamp back to naive strips the zone and breaks cross-zone comparison |

### Consequences

`CONVERT_TZ` queries continue to execute correctly via Exasol's own post-processing. A future
revisit needs either a timestamptz-preserving mapping (a larger, separately-scoped change) or a
translator path that receives session timezone state, neither of which this plan takes on.

## ADR: Render ADD_HOURS/ADD_MINUTES in the microsecond domain; defer ADD_DAYS/ADD_WEEKS/ADD_YEARS

**ID:** add-hours-minutes-microsecond-rendering-defer-add-days-weeks-years
**Plan:** add-date-arithmetic-pushdown
**Status:** Accepted

### Context

The initially planned `ADD_*` rendering, `<x> + CAST(ROUND(<n>) AS BIGINT) * INTERVAL '<unit>'`,
hard-errors at plan time in the workspace-pinned DataFusion 54.0.0: `Integer × Interval` coerces
both operands to `Interval(MonthDayNano)`, but that multiply kernel is unimplemented (arrow-rs#9030,
open). A later end-to-end parity run against live Exasol 2025.1.3 found a second divergence: even a
working `ADD_HOURS`/`ADD_MINUTES` rendering that always returns `Timestamp(Microsecond)` (mapped to
`TIMESTAMP(3)`) is rejected by Exasol on a DATE argument, which Exasol types as `TIMESTAMP(0)`.

### Decision

Do not advertise `ADD_HOURS`, `ADD_MINUTES`, `ADD_DAYS`, `ADD_WEEKS`, or `ADD_YEARS`. Withdraw the
translator arms for `ADD_HOURS`/`ADD_MINUTES` (their microsecond-domain rendering is correct for a
TIMESTAMP argument but fails Exasol's stricter DATE-argument precision check) and defer
`ADD_DAYS`/`ADD_WEEKS` (input-type-dependent return type the type-blind translator cannot
reproduce) and `ADD_YEARS` (Exasol's month-end stickiness, the same divergence class as
`ADD_MONTHS`, not the interval-multiply defect first suspected).

### Options Considered

| Option | Verdict |
|--------|---------|
| Withdraw all five `ADD_*` arms; fall through to Exasol | ✓ Chosen — the type-blind string translator cannot vary result type or precision by argument type, and every execution-safe DataFusion 54.0.0 rendering tried failed a live-Exasol parity check |
| Ship the microsecond-domain `ADD_HOURS`/`ADD_MINUTES` rendering despite the DATE-argument failure | ✗ Rejected — an advertised capability that hard-fails Exasol's own type check on DATE columns violates the parity gate |
| Wait for arrow-rs#9030 to close and re-attempt integer-interval scaling | ✗ Rejected — a follow-up review found the true `ADD_YEARS` blocker is month-end stickiness, not the interval-multiply defect, so closing #9030 would not fix it |

### Consequences

No `ADD_*` date-arithmetic function pushes down in this plan; all five fall through to Exasol's
own evaluation. Re-attempting any of them needs a type-aware translator that can vary rendering
(and result precision) by the argument's Iceberg-resolved type — tracked as unblock conditions in
the plan's decision log, not new work here.
