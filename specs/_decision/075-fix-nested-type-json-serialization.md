# Decisions: fix-nested-type-json-serialization

## ADR: The logical Arrow type stays `Utf8`; the nested type never enters the tag vocabulary

**ID:** nested-logical-type-stays-utf8
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

Issue #350 proposed a recursive nested Arrow tag, which would make the column a real `Struct` or `Map` during DataFusion execution. DataFusion has no comparison, ordering, hashing, or aggregation for those types. That would force new decline gates at five pushdown decision sites, a re-sequenced `handle_pushdown`, and a wider shared `col_types` shape.

### Decision

The logical Arrow type of a nested column stays `Utf8`. The scan's physical-expression adapter, and the generated SQL on the legacy path, render the JSON, so the column is the JSON string in the DataFusion schema, the `ScanSpec` tag vocabulary, the pushdown planner, and Exasol's `VARCHAR(2000000)` declaration.

### Options Considered

| Option | Verdict |
|--------|---------|
| Recursive nested Arrow tag grammar | Rejected: the column becomes a real nested type, forcing five decline gates and a re-sequenced `handle_pushdown` |

### Consequences

No expression over such a column ever sees a nested type, so no new gate or error path is needed. The live check of the pushdown shapes stays as plan task 16.

