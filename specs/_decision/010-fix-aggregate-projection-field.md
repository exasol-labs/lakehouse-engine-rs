# Decisions: fix-aggregate-projection-field

## ADR: Leave `projection` empty on aggregate scan specs rather than derive a precise list

**ID:** aggregate-scan-spec-empty-projection-field
**Plan:** `fix-aggregate-projection-field`
**Status:** Accepted

### Context

`EXPLAIN VIRTUAL` showed the full base-table column list in the `projection` field of aggregate and GROUP BY scan specs (#145). No aggregate execution path reads that field, and DataFusion projection pushdown already prunes the Parquet read.

### Decision

The adapter leaves `projection` empty on grouped and single-group aggregate scan specs. Referenced columns are carried only in the `aggregates` and `group_keys` fields.

### Options Considered

| Option | Verdict |
|--------|---------|
| Derive a precise projection list from aggregate and group-key columns | Rejected: duplicates `aggregates`/`group_keys` and is error-prone for expression arguments |

### Consequences

`EXPLAIN VIRTUAL` shows `"projection":[]` for aggregate scan specs. Row-scan and join projections are unaffected, because the single-group branch empties `projection` only when `aggregates` is set.
