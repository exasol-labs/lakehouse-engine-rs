# Decision Log: refactor-exasol-column-type

## Interview

- Q: Sequencing against #578 (open PR #580, current branch)? → A: plan on top of #580, implement after it merges
- Q: Base after #580 merged? → A: implement on top of PR #581 (#577), which adds the shared request-JSON test builders; it merged as `a4a2ca6d`
- Q: Is `LogicalField.arrow_type` (wire-format String tag) in scope? → A: out of scope, list under Out of scope; no wire change
- Q: One plan or split? → A: one plan, staged tasks
- Q: Shape of the owned Exasol type? → A: enum with parameters, one parser per input edge, one SQL renderer, one JSON renderer, byte-exact
- Issue #404 addenda fixed the rest: the owned type describes the column (Exasol type + what the scan needs, e.g. binary byte width), keyed by `ColumnRef`, passed as one named type; projections pair item with type (`ProjectedColumn`); done-checks: `BinaryColumns` and `ColumnTypes` removed as separate types

## Design Decisions

### [1] Build the per-column type map once, when the planner resolves a table

- **Decision:** `TableScanResolver::resolve` builds each table's `ColumnTypes` from the request's Exasol types and the declared `LogicalField`s, after it parses the column notes. It retypes the filter's binary literals with that map before format pruning and returns the map beside the `ResolvedScan`. `ResolvedJoinSide` carries it. Every consumer reads that map: the filter type checks, the binary comparison and literal retyping, the projection, and the join legs. The single-table path builds its projection after resolution. Format readers receive the retyped filter, and neither the map nor `binary_as_string`.
- **Rationale:** A binary column's byte width comes from the column notes, and `TableNotes::declared_schema` is their one parse. Before resolution, the single-table projection and the format-side literal retyping have no declared schema, so a map built there cannot carry the binary width. One construction point keeps one owner for the column-typing decision. Moving the projection preserves behavior, because `project_columns` fails only for a request without column metadata, which Exasol does not send. The map stays in the pushdown planner, which already depends on format-readers. A map field on the format-readers type `ResolvedScan` would add the reverse dependency and close a cycle.
- **Alternatives:** Keep the five request-only construction sites and add binary widths only where a declared schema exists. Rejected: the map would be wrong for binary columns at the other sites. Parse each column note a second time in `involved_columns`. Rejected: it adds a second parser of the column note format, which column-notes owns.
- **Promotes to ADR:** no

### [2] `arrow_to_exasol_type` stays as the Arrow input edge

- **Decision:** `arrow_to_exasol_type` returns the owned Exasol type and keeps its callers. Only `exasol_type_to_arrow` is deleted.
- **Rationale:** Issue #404 lists both functions as dead. On this branch `arrow_to_exasol_type` has two production callers: the Parquet arm of `column_source_type_to_exasol` and `render_join_sort_target` in `scan/join_scan.rs`. Deleting it would copy the Arrow mapping into both callers. The accepted ADR `single-authoritative-datafusion-to-exasol-type-mapping-with-json-fallback` requires one mapping.
- **Promotes to ADR:** no

## Review Findings

### [plan-review] The per-column type map stays out of format-readers

- **Finding:** Task 3.1 put the per-column type map into format-readers' `ResolvedScan`. The pushdown planner owns the map and already depends on format-readers, so the field closed a dependency cycle that the architecture delta does not record.
- **Direction change:** `TableScanResolver::resolve` builds the map, retypes the filter's binary literals with it, and returns it beside the `ResolvedScan`. `ResolvedJoinSide` carries it. `ResolvedScan::resolve` takes neither the map nor `binary_as_string`. Decision [1] states this.
- **Promotes to ADR:** no

### [plan-review] No separate rule for a DECIMAL without precision and scale

- **Finding:** The `pushdown/pushdown-planning` scenario for a `DECIMAL` `dataType` without precision and scale held on unchanged code, so its test could not prove Done check 4.
- **Direction change:** The scenario and its decision entry are removed. `ExasolType::Decimal` always carries precision and scale, and `exasol_type_from_json` keeps its 18 and 0 defaults. On Exasol 2025.1.16, `EXA_ALL_COLUMNS` reports a column declared `DECIMAL` or `NUMERIC` as `DECIMAL(18,0)`, and `TYPEOF(CAST(1.7 AS DECIMAL))` returns `DECIMAL(18,0)`, so Exasol never reports a bare `DECIMAL`.
- **Promotes to ADR:** no
