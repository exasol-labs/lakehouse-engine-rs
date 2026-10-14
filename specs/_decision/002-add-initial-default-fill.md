# Decisions: add-initial-default-fill

## ADR: Implement Iceberg Column-Projection Rule 3 for All Absent Fields

**ID:** implement-iceberg-column-projection-rule-3-for-all-absent-fields
**Plan:** `add-initial-default-fill`
**Status:** Accepted

### Context

The scan UDF NULL-filled every absent column and ignored the Iceberg `initial-default`. The Iceberg table spec returns the `initial-default` for any absent field, required or nullable.

### Decision

The scan returns the defined `initial-default` for an absent field, whether the field is required or nullable. A nullable field with no default is NULL-filled, and a required field with no default fails with a clear error. `write-default` is never used on reads, because it governs writer-side backfill.

### Options Considered

| Option | Verdict |
|--------|---------|
| Fix only the required-column case named in #27 | Rejected: leaves a known deviation for nullable columns with a default |

### Consequences

The scenario "nullable absent column is NULL-filled" applies only when the column defines no default.

---
