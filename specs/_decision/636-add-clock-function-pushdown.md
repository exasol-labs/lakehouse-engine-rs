# Decisions: add-clock-function-pushdown

## ADR: Fold clock values in a zone the virtual schema names

**ID:** fold-clock-values-in-virtual-schema-zone
**Plan:** add-clock-function-pushdown
**Status:** Accepted

### Context

No adapter or UDF request carries `SESSIONTIMEZONE`, and connect-back opens a different session, so the adapter cannot fold a clock function or a compared `TIMESTAMP WITH LOCAL TIME ZONE` literal in the caller's session zone.

### Decision

The adapter folds `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `LOCALTIMESTAMP`, and compared `TIMESTAMP WITH LOCAL TIME ZONE` literals in the zone of the `SESSION_TIME_ZONE` property, or in `DBTIMEZONE` when it is unset, and `SYSDATE` and `SYSTIMESTAMP` in `DBTIMEZONE`. A session whose zone differs gets the folding zone's values, as `docs/semantic-differences.md` documents.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fold in a zone the virtual schema names | ✓ Chosen — filters over clock values and TSTZ literals prune files and run in the scan |
| Keep the clock functions unadvertised so Exasol evaluates them in the session zone | ✗ Rejected — a filter over a clock function or a TSTZ literal reads every file |

### Consequences

- `EXCLUDED_CAPABILITIES` restores native evaluation for a workload that needs the session zone.
