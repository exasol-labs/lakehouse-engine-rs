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

## ADR: The nested field descriptor is carried as data, not as a type

**ID:** nested-descriptor-carried-as-data-not-type
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

The only Delta struct fixture uses column mapping by name, so its inner fields have opaque physical names. Rendering physical names would emit those identifiers as JSON keys for the common Unity and Databricks table shape.

### Decision

`LogicalField` gains an optional, format-neutral nested descriptor. It holds each nested field's logical name and the one binding key its format's column mapping selects (field ID, physical name, or neither). Only the JSON renderer's name resolution reads it.

### Options Considered

| Option | Verdict |
|--------|---------|
| Render physical nested names and refuse column-mapped Delta tables | Rejected: leaves no working Delta struct coverage |
| Fold the nested structure into the `arrow_type` tag | Rejected: makes the column a real nested type |

### Consequences

Nested rename, reorder, add, and drop work through the binding-key mechanism that top-level columns use.

## ADR: The JSON shape diverges from the Iceberg spec's Appendix D, deliberately and on the record

**ID:** json-shape-diverges-from-appendix-d
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

Iceberg Appendix D prescribes a JSON object by field ID for a struct and key and value arrays for a map. It covers metadata single values, not query results, and the spec defines no JSON encoding for scan output rows.

### Decision

A struct renders as a JSON object keyed by field name, and a map as one JSON object keyed by its stringified key.

### Options Considered

| Option | Verdict |
|--------|---------|
| Adopt Appendix D shapes | Rejected: a field-ID-keyed object has no readable path expression, and parallel arrays cannot be read by key from Exasol SQL |

### Consequences

The divergence is recorded in the feature's Background with the scoping sentences quoted, so it is not a silent gap.

## ADR: Disable Parquet row-filter pushdown rather than decline the predicate to Exasol

**ID:** disable-parquet-row-filter-pushdown-for-nested-column
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

DataFusion approves filter pushdown against the table schema, where the column is `Utf8`, and removes the `FilterExec`. At file open, `build_row_filter` checks the physical nested schema and drops the conjunct, so queries return wrong rows without error. Live runs showed this for `=`, `<>`, `>`, `IN`, `LIKE`, `UPPER(col) =`, and `LENGTH(col) =` on both Iceberg and Delta. It already affects `list` columns today.

### Decision

When a table's registered schema has a JSON-rendered nested column, the scan disables Parquet row-filter pushdown for that table, so a `FilterExec` evaluates the predicate over the rendered `Utf8` column.

### Options Considered

| Option | Verdict |
|--------|---------|
| Decline the predicate in the VS adapter and apply it in the Exasol wrapper | Rejected: needs five decision sites, the blast radius the first ADR rejects |
| Accept the current behavior | Rejected: ships a known silent wrong-rows bug |

### Consequences

A query over a table with a nested column loses Parquet row-level filter pushdown for all its columns, so late materialization no longer skips rows within a row group. Statistics-based row-group and page pruning is unaffected. The change also fixes the existing `list` bug.

## ADR: The declared nested descriptor is the single signal for the diversion AND the pushdown withdrawal

**ID:** nested-descriptor-single-signal-diversion-and-withdrawal
**Plan:** `fix-nested-type-json-serialization`
**Status:** Accepted

### Context

The physical Arrow type is unavailable before file open, so the pushdown withdrawal can only read the declared descriptor. Keying the cast diversion on the physical type would render a descriptor-less spec over a nested column while pushdown stays enabled, and the scan would return every row.

### Decision

The cast diversion and the Parquet row-filter pushdown withdrawal both key on the logical field's declared nested descriptor, and additionally require the resolved column type to be one of the five nested variants. A physically nested column with no descriptor goes to the delegate, which fails loudly.

### Options Considered

| Option | Verdict |
|--------|---------|
| Key the diversion on the physical Arrow type | Rejected: reintroduces the silent wrong-rows bug |

### Consequences

A legacy spec without a descriptor still deserializes but no longer renders. The type check keeps a descriptor that contradicts the file's type from reaching an encoder that would quote it.
