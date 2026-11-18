# Decisions: fix-instant-column-tstz-comparisons

## ADR: Fold clock values in the virtual schema zone, and in UTC against instant columns

**ID:** fold-clock-values-in-zone-and-utc-against-instant-columns
**Plan:** fix-instant-column-tstz-comparisons
**Status:** Accepted
**Supersedes:** fold-clock-values-in-virtual-schema-zone

### Context

No adapter or UDF request carries `SESSIONTIMEZONE`, so the adapter cannot fold a clock function or a compared `TIMESTAMP WITH LOCAL TIME ZONE` value in the caller's session zone. An instant column is declared a plain `TIMESTAMP` that holds each instant's UTC date and time, so a value folded to a zone-local wall clock compares with it off by that zone's offset.

### Decision

The adapter folds `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, and compared `TIMESTAMP WITH LOCAL TIME ZONE` values in the zone of the `SESSION_TIME_ZONE` property, or in `DBTIMEZONE` when it is unset, and `SYSDATE` and `SYSTIMESTAMP` in `DBTIMEZONE`. A `TIMESTAMP WITH LOCAL TIME ZONE` value that a comparison, `BETWEEN`, or `IN` sets directly against an instant column folds to its UTC date and time instead, so the comparison compares the instants in every session.

### Options Considered

| Option | Verdict |
|--------|---------|
| Keep instant columns plain `TIMESTAMP` in UTC and fold a compared TSTZ value to UTC | ✓ Chosen — native rows for TSTZ comparisons, no declared type changes, no `REFRESH` |
| Declare instant columns `TIMESTAMP WITH LOCAL TIME ZONE` | ✗ Rejected — Exasol refuses that type as an `EXPORT ... INTO SCRIPT` source column, and a `REFRESH` would change every instant column's type |
| Keep the clock functions unadvertised so Exasol evaluates them in the session zone | ✗ Rejected — a filter over a clock function or a TSTZ literal reads every file |

### Consequences

- `EXCLUDED_CAPABILITIES` restores native evaluation for a workload that needs the session zone, except for instant columns, which Exasol then reads as session-local.
- A plain `TIMESTAMP` or `DATE` value compares with an instant column's UTC date and time; `docs/semantic-differences.md` lists it.
