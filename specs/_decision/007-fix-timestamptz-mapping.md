# Decisions: fix-timestamptz-mapping

## ADR: Collapse only the Exasol-facing type string; keep the internal timezone-aware Arrow type

**ID:** collapse-exasol-facing-timestamp-string-keep-internal-tz-aware-arrow-type
**Plan:** `fix-timestamptz-mapping`
**Status:** Accepted

### Context

Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` type, so queries on an Iceberg `timestamptz` column failed. Internally the column is a UTC timezone-aware Arrow timestamp.

### Decision

The engine maps `timestamptz` to plain `TIMESTAMP` only in the Exasol-facing type strings. The internal Arrow type stays timezone-aware, and the emit cast converts it to a naive timestamp that keeps the UTC instant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop the internal timezone label | Rejected: needs changes at every site that uses the label and risks a mismatch between timezone-aware predicate literals and a naive column |

### Consequences

- Exasol no longer distinguishes `timestamptz` from `timestamp`, but no emitted value changes.
- DataFusion comparisons stay timezone-correct.
