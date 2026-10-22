# Decisions: fix-nested-type-json-serialization

## ADR: The logical Arrow type stays `Utf8`; the nested type never enters the tag vocabulary

**ID:** nested-logical-type-stays-utf8
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

A recursive nested Arrow tag would make the column a real struct or map during DataFusion execution. DataFusion has no comparison, ordering, hashing, or aggregation for those types, so every pushdown decision site would need a new decline gate.

### Decision

The logical Arrow type of a nested column stays a string. The scan renders the JSON, so the column is a JSON string in the DataFusion schema, the scan spec, the pushdown planner, and Exasol's `VARCHAR(2000000)` declaration.

### Options Considered

| Option | Verdict |
|--------|---------|
| Recursive nested Arrow tag grammar | Rejected: the column becomes a real nested type and forces new decline gates at five pushdown sites |

### Consequences

- No expression over such a column sees a nested type, so no new gate or error path is needed.

