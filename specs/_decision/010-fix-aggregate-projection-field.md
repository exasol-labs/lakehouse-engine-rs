# Decisions: fix-aggregate-projection-field

## ADR: Leave `projection` empty on aggregate scan specs rather than derive a precise list

**ID:** aggregate-scan-spec-empty-projection-field
**Plan:** `fix-aggregate-projection-field`
**Status:** Accepted

### Context

Issue #145 reported that `EXPLAIN VIRTUAL` shows the full base-table column list in the
`projection` field of an aggregate or GROUP BY `LAKEHOUSE_SCAN` scan spec. No aggregate
execution path ever reads `ScanSpec.projection`: `run_partial_aggregate` and
`run_grouped_partial_aggregate` register the full logical schema and build their
DataFusion query from the `aggregates`/`group_keys` fields, and DataFusion's own
projection pushdown then prunes the physical Parquet read. The bug is a misleading value
in a diagnostic field, not extra I/O. Deriving a precise partial-projection column list
from aggregate arguments was considered and rejected as duplicate, error-prone work.

### Decision

Set `ScanSpec.projection` to empty on both the grouped and single-group aggregate scan-spec
branches. Carry referenced-column information only in the `aggregates`/`group_keys` fields,
which are the fields the aggregate scan-dispatch path actually consults.

### Options Considered

| Option | Verdict |
|--------|---------|
| Empty `projection` on aggregate branches | ✓ Chosen — the aggregate path never reads `projection`; empty is minimal and unambiguous |
| Derive a precise projection list from aggregate/group-key columns | ✗ Rejected — duplicates data already in `aggregates`/`group_keys` and can be half-right for expression arguments |
| Bare-column-precise derivation with expression fallback | ✗ Rejected — same duplication risk, plus added complexity for no consulted benefit |

### Consequences

`EXPLAIN VIRTUAL` reports `"projection":[]` for aggregate and GROUP BY scan specs,
disambiguated on the wire by the co-present `aggregates`/`group_keys` fields. The
physical Parquet read stays column-pruned via DataFusion's projection pushdown,
confirmed empirically by a new physical-plan-introspection test. The row-scan and
join projection paths are unaffected: the single-group branch empties `projection`
only when `aggregates.is_some()`, preserving the shared `spec_template`'s row-scan
projection.
