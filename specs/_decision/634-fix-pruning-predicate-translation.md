# Decisions: fix-pruning-predicate-translation

## ADR: Every pruning path converts literals through one exact converter

**ID:** pruning-literals-share-one-exact-converter
**Plan:** fix-pruning-predicate-translation
**Status:** Accepted

### Context

The Iceberg, Delta, and Hive partition pruning paths each converted Exasol literals by their own table, and the tables drifted (#634). Iceberg rounded a float literal and pruned files holding matching rows, and it pruned no decimal or `timestamptz` column.

### Decision

One converter turns a filter literal into a DataFusion `ScalarValue` of the column's Arrow type, or into nothing when that type cannot hold the literal exactly. Iceberg and Delta map their column type to an Arrow type and the converted value to their own literal type. The Hive partition predicate uses the value directly.

### Consequences

- An exactness rule is written once, and each path only maps types.
- A column type without an Arrow mapping in a path prunes nothing there. Iceberg `uuid` stays unmapped on purpose.
