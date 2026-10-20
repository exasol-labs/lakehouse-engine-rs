# Decisions: refactor-iceberg-delete-classification

## ADR: Unsupported Iceberg deletes are classified from the planned file tasks

**ID:** unsupported-deletes-classified-from-planned-tasks
**Plan:** refactor-iceberg-delete-classification
**Status:** Accepted
**Supersedes:** plan-time-fail-loud-at-the-manifest-datafile-level-is-the-authoritative-correctness-gate-for-unsupported-deletes

### Context

The engine applies only Parquet positional deletes over Parquet data files. iceberg-rust's planned file tasks now carry the format of each data file and each applied delete file.

### Decision

The adapter refuses a query at plan time when a planned data file, or a delete file iceberg-rust applies to it, is unsupported. Which delete files apply to which data file follows iceberg-rust's delete matching, not a matching of our own.

### Options Considered

| Option | Verdict |
|--------|---------|
| Read every manifest of the snapshot and refuse on any unsupported file | Rejected: reads the manifests twice per pushdown and refuses queries whose planned files no unsupported file touches |

### Consequences

- A query whose filter prunes every file an unsupported mechanism applies to returns rows.
- The refusal is as correct as iceberg-rust's delete matching. Inconsistent deletion-vector metadata fails with iceberg-rust's own planning error.
