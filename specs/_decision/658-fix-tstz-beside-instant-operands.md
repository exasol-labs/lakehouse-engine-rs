# Decisions: fix-tstz-beside-instant-operands

## ADR: Fold clock values in the virtual schema zone, and in UTC beside instant operands

**ID:** fold-clock-values-in-zone-and-utc-beside-instant-operands
**Plan:** fix-tstz-beside-instant-operands
**Status:** Accepted
**Supersedes:** fold-clock-values-in-virtual-schema-zone

### Context

No adapter or UDF request carries `SESSIONTIMEZONE`, so the adapter cannot fold a clock function or a `TIMESTAMP WITH LOCAL TIME ZONE` value in the caller's session zone. An instant column is declared a plain `TIMESTAMP` that holds each instant's UTC date and time, and the virtual schema shows it in UTC, so a value folded to a zone-local wall clock meets it off by that zone's offset.

### Decision

Instant columns hold UTC. The adapter folds `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, and `TIMESTAMP WITH LOCAL TIME ZONE` values in the zone of the `SESSION_TIME_ZONE` property, or in `DBTIMEZONE` when it is unset, and `SYSDATE` and `SYSTIMESTAMP` in `DBTIMEZONE`. Beside a `DATE`, `TIMESTAMP`, or `TIMESTAMP WITH LOCAL TIME ZONE` operand that reads an instant column, every clock function and `TIMESTAMP WITH LOCAL TIME ZONE` value folds to its UTC date or date and time instead, anywhere in the operands of that predicate or function call outside a nested predicate.

### Options Considered

| Option | Verdict |
|--------|---------|
| Treat instant columns as UTC and fold every clock value beside a date or time operand over one to UTC | ✓ Chosen. The column, its date and time readers, plain literals, and clock values all use UTC, so a query over instant columns returns what native Exasol returns in a UTC session |
| Fold to UTC only a `TIMESTAMP WITH LOCAL TIME ZONE` value beside an operand that native Exasol types `TIMESTAMP WITH LOCAL TIME ZONE` | ✗ Rejected. `CURRENT_DATE`, `SYSTIMESTAMP`, and `CAST(tz AS DATE) = CURRENT_DATE` mix the UTC column with a zone-local clock |
| Declare instant columns `TIMESTAMP WITH LOCAL TIME ZONE` | ✗ Rejected. Exasol refuses that type as a UDF input, so `EXPORT ... INTO SCRIPT` of such a column fails, and a `REFRESH` would change every instant column's type and displayed value |
| Keep the clock functions unadvertised so Exasol evaluates them in the session zone | ✗ Rejected. A filter over a clock function or a TSTZ literal reads every file |

### Consequences

- `EXCLUDED_CAPABILITIES` restores native evaluation for a workload that needs the session zone, except for instant columns, which Exasol then reads as session-local.
- Exasol compares a plain `TIMESTAMP` with a `TIMESTAMP WITH LOCAL TIME ZONE` value as session-local wherever it evaluates the comparison itself, such as `tz + INTERVAL '1' HOUR > CURRENT_TIMESTAMP` or a filter above a `LIMIT` subquery. `docs/semantic-differences.md` lists these, and mixed operands such as `ADD_DAYS(ts, DAY(tz))`.
