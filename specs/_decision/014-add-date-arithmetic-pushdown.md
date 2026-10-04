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

Leave `CONVERT_TZ` unsupported; it falls through for Exasol to post-process.

Do not advertise `ADD_HOURS`, `ADD_MINUTES`, `ADD_DAYS`, `ADD_WEEKS`, or `ADD_YEARS`.

