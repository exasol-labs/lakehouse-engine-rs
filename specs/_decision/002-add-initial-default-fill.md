# Decisions: add-initial-default-fill

## ADR: Implement Iceberg Column-Projection Rule 3 for All Absent Fields

**ID:** implement-iceberg-column-projection-rule-3-for-all-absent-fields
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The Iceberg table spec returns a field's `initial-default` when the field is absent from a data file, whether the field is required or nullable. The scan must not NULL-fill such fields.

### Decision

The scan returns the defined `initial-default` for an absent field, whether the field is required or nullable. A nullable field with no default is NULL-filled, and a required field with no default fails with a clear error. `write-default` is never used on reads, because it governs writer-side backfill.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix only the required-column case | Rejected: leaves a spec deviation for nullable columns with a default |

### Consequences

- A nullable absent column is NULL-filled only when it defines no default.

