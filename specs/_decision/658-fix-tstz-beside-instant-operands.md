# Decisions: fix-tstz-beside-instant-operands

## ADR: Fold clock values in the virtual schema zone, and in UTC beside instant operands

**ID:** fold-clock-values-in-zone-and-utc-beside-instant-operands
**Plan:** fix-tstz-beside-instant-operands
**Status:** Accepted
**Supersedes:** fold-clock-values-in-zone-and-utc-against-instant-columns

### Context

No adapter or UDF request carries `SESSIONTIMEZONE`, so the adapter cannot fold a clock function or a `TIMESTAMP WITH LOCAL TIME ZONE` value in the caller's session zone. An instant column is declared a plain `TIMESTAMP` that holds each instant's UTC date and time, so a value folded to a zone-local wall clock meets it, or an expression that keeps its instants such as `MAX(tz)` or `ADD_HOURS(tz, 1)`, off by that zone's offset.

### Decision

The adapter folds `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, and `TIMESTAMP WITH LOCAL TIME ZONE` values in the zone of the `SESSION_TIME_ZONE` property, or in `DBTIMEZONE` when it is unset, and `SYSDATE` and `SYSTIMESTAMP` in `DBTIMEZONE`. A `TIMESTAMP WITH LOCAL TIME ZONE` value folds to its UTC date and time instead when another operand of the same predicate or function call reads an instant column and native Exasol types it `TIMESTAMP WITH LOCAL TIME ZONE` over a table whose instant columns have that type.

### Options Considered

| Option | Verdict |
|--------|---------|
| Type each operand as native Exasol does and fold to UTC beside an instant operand | ✓ Chosen — covers aggregates, date additions, `CASE`, and sibling function arguments through the type rules the scan already checks against Exasol |
| Fold to UTC only beside a bare instant column | ✗ Rejected — `MAX(tz)`, `ADD_HOURS(tz, 1)`, and `HOURS_BETWEEN(CURRENT_TIMESTAMP, tz)` stay off by the zone offset |
| Fold to UTC anywhere in an expression that reads an instant column | ✗ Rejected — also folds a value beside a plain result such as `DAY(tz)` |
| Declare instant columns `TIMESTAMP WITH LOCAL TIME ZONE` | ✗ Rejected — Exasol refuses that type as an `EXPORT ... INTO SCRIPT` source column, and a `REFRESH` would change every instant column's type |
| Keep the clock functions unadvertised so Exasol evaluates them in the session zone | ✗ Rejected — a filter over a clock function or a TSTZ literal reads every file |

### Consequences

- `EXCLUDED_CAPABILITIES` restores native evaluation for a workload that needs the session zone, except for instant columns, which Exasol then reads as session-local.
- A plain `TIMESTAMP` or `DATE` value beside an instant column, and a function that reads an instant column's date or time of day, use its UTC date and time. An expression that native Exasol types plain, such as `CAST(tz AS TIMESTAMP)`, compares in the folding zone. `docs/semantic-differences.md` lists both.
