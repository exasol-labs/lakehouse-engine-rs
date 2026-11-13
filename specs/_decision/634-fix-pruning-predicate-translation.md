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

## ADR: Every pruning path reads one parsed filter

**ID:** pruning-paths-share-one-parsed-filter
**Plan:** fix-pruning-predicate-translation
**Status:** Accepted

### Context

The Iceberg, Delta, and Hive partition paths each walked the filter JSON, and each node shape was added to one walk before the others (#634). A rewrite of a function over a column, such as `YEAR(d) = 2024` into a date range, would have needed one copy per walk.

### Decision

One parse turns the filter into a format-neutral pruning filter: comparisons, `IN` lists, null tests, and `LIKE` patterns over resolved columns, with every literal converted to the column's Arrow type, joined by `AND`, `OR`, and `NOT`, and an opaque node for anything else. Each path supplies only a column resolver. The Hive partition predicate evaluates the parsed filter under three-valued logic against each file's values. Iceberg and Delta lower it into their own predicate through a trait that builds a comparison, an `IN` list, a null test, a prefix test, and the junctions, and that tracks which nodes translated exactly.

### Consequences

- A filter shape or rewrite added to the parse reaches every path at once.
- A format that cannot express a node, such as Delta's missing prefix predicate, builds an equivalent one inside its trait method.
- Iceberg keeps its native `IN`, so a long `IN` list does not become a deep `OR` tree.
