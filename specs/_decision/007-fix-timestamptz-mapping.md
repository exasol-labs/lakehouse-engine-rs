# Decisions: fix-timestamptz-mapping

## ADR: Collapse only the Exasol-facing type string; keep the internal timezone-aware Arrow type

**ID:** collapse-exasol-facing-timestamp-string-keep-internal-tz-aware-arrow-type
**Plan:** `fix-timestamptz-mapping`
**Status:** Accepted

### Context

Exasol rejects `TIMESTAMP WITH LOCAL TIME ZONE` as a UDF `EMITS` type, so any query on an Iceberg `timestamptz` column failed. Internally, `timestamptz` is a timezone-aware Arrow `Timestamp` with UTC, and that label flows through the logical-schema tag round-trip and the `initial-default` reconstruction.

### Decision

The engine maps `timestamptz` to plain `TIMESTAMP` only in the Exasol-facing type strings. The internal Arrow type stays timezone-aware, and the emit-boundary cast already converts it to a naive timestamp while preserving the UTC instant.

### Options Considered

| Option | Verdict |
|--------|---------|
| Drop the internal timezone label | Rejected: requires changes at every site that uses the label and risks a mismatch in DataFusion between timezone-aware predicate literals and a naive column |

### Consequences

The Exasol column type no longer distinguishes `timestamptz` from `timestamp`, but no emitted value changes. DataFusion comparisons and predicate binding stay timezone-correct.
