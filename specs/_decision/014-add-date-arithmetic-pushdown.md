# Decisions: add-date-arithmetic-pushdown

## ADR: Split issue #107 date functions into a supported and a deferred subset by verified parity

**ID:** split-issue-107-date-functions-supported-deferred-by-verified-parity
**Plan:** add-date-arithmetic-pushdown
**Status:** Accepted

### Context

The project advertises a function only when its DataFusion result is confirmed to match Exasol. Live-Exasol parity runs found divergent renderings for most of the date functions in issue #107.

### Decision

The adapter advertises pushdown for `DAYS_BETWEEN`, `HOURS_BETWEEN`, `MINUTES_BETWEEN`, and `SECONDS_BETWEEN`, confirmed by parity against live Exasol. It defers `ADD_HOURS`, `ADD_MINUTES`, `ADD_DAYS`, `ADD_WEEKS`, `ADD_YEARS`, `ADD_SECONDS`, `ADD_MONTHS`, `MONTHS_BETWEEN`, `YEARS_BETWEEN`, `DAYOFWEEK`, and `CONVERT_TZ`, each for a named divergence. `LAST_DAY` is not an Exasol function, and `POSIX_TIME` stays out of scope.

### Options Considered

| Option | Verdict |
|--------|---------|
| Advertise all functions as a block | Rejected: violates the backing-path bar and risks silently wrong results |
| Defer everything until a full calendar-semantics layer exists | Rejected: leaves verified `*_BETWEEN` pushdowns unused |

### Consequences

`ADD_*` pushdown waits for a type-aware translator that varies rendering by argument type (DATE vs. TIMESTAMP). `CONVERT_TZ` stays unsupported and Exasol evaluates it.
