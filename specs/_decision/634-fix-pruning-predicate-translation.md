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

## ADR: Iceberg and Delta pruning share one filter walk

**ID:** statistics-pruning-shares-one-filter-walk
**Plan:** fix-pruning-predicate-translation
**Status:** Accepted

### Context

The Iceberg and Delta translators walked the filter with two copies of the same dispatch, and each node shape was added to one copy before the other (#634).

### Decision

One walk translates every filter shape. Each format implements a trait that resolves a column, converts a literal, and builds a comparison, a null test, an `IN` list, a prefix test, and the junctions. The Hive partition predicate keeps its own three-valued evaluation, because it decides each file from its values instead of building a predicate.

### Consequences

- A filter shape added to the walk reaches Iceberg and Delta at once.
- A format that cannot express a node, such as Delta's missing prefix predicate, builds an equivalent one inside its trait method.
