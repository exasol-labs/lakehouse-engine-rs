# Decisions: add-direct-storage-statistics-pruning

## ADR: Plan-time Parquet statistics pruning uses the engine's partition predicate, not DataFusion's pruning predicate

**ID:** plan-time-parquet-statistics-pruning-uses-the-engines-partition-predicate
**Plan:** add-direct-storage-statistics-pruning
**Status:** Accepted

### Context

A partition value and a row group's footer bounds are both value ranges. The planner has no step that turns Exasol's filter JSON into a DataFusion expression.

### Decision

Direct-storage statistics pruning evaluates the pushed filter with the predicate that prunes partition values. DataFusion's pruning predicate is not used at plan time.

### Options Considered

| Option | Verdict |
|--------|---------|
| DataFusion's `PruningPredicate` | Rejected: its row-group statistics type is private, it ignores `ColumnOrder`, and it treats a missing null count as zero |
| A separate statistics-only walker | Rejected: it duplicates the node set and the JSON translation, and two translations of one filter can disagree about which files a node keeps |

### Consequences

- Partition pruning and statistics pruning cannot disagree about a filter.
