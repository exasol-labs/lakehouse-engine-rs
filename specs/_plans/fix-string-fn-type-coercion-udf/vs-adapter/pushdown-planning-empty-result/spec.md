# Feature: Pushdown Planning — Empty Result When All Files Are Pruned

Unchanged apart from the scenarios below. The recorded text stands as written in `specs/vs-adapter/pushdown-planning-empty-result/spec.md`.

## Background

* Unchanged by this plan. This section is unmarked, so `speq record` ignores it and the recorded Background survives intact. See `specs/vs-adapter/pushdown-planning-empty-result/spec.md`.

## Scenarios

<!-- DELTA:NEW -->
### Scenario: An ungrouped aggregate on the row-scan path with all files pruned returns one row

* *GIVEN* a request with no GROUP BY whose select list is aggregates, and which the shared request classifier routes to `RowScan`, for example `SELECT MAX(CAST(o_orderdate AS TIMESTAMP(4))), COUNT(*) FROM {vs_table} WHERE o_orderkey < 0` or `SELECT MAX(INSTR(c_name, 'c', 2)) FROM {vs_table} WHERE {prunes every file}`
* *AND* the WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces exactly one row, with one column per select-list item in order
* *AND* each `COUNT` family column SHALL be `0`, each other aggregate column SHALL be `NULL`, an expression over aggregates SHALL evaluate over those values, and every column SHALL be cast to its declared type from `selectListDataTypes`, so the row equals native Exasol over zero rows: `NULL, 0` for the first example
* *AND* a `RowScan` request without an aggregate SHALL keep the zero-row typed projection of the row-scan scenario, and the response MUST NOT invoke the scan SET UDF
<!-- /DELTA:NEW -->

<!-- DELTA:CHANGED -->
### Scenario: Empty-result shape matches the plan the non-empty path would commit to

* *GIVEN* any `pushdown` request whose filter prunes 100% of the table's data files during plan-time file pruning, whichever table format's reader resolved that list
* *WHEN* the adapter reaches the zero-files short-circuit
* *THEN* the adapter SHALL choose the empty-result shape using the SAME plan-detection priority the non-empty path uses — grouped aggregate first, then single-group aggregate, then row scan
* *AND* the single-group aggregate shape SHALL be chosen only when the aggregate column types pass the same numeric-type validation the non-empty path applies, and an aggregate the non-empty path demotes to a row scan produces the row-scan empty shape of the scenario "An ungrouped aggregate on the row-scan path with all files pruned returns one row"
* *AND* the empty and non-empty paths SHALL derive that priority and those validation gates from one shared request classifier, so the ROUTING decision is shared by construction rather than kept in lockstep by convention; each path then renders its own shape from that shared decision
* *AND* that shared classifier SHALL raise NO grouped-tier hard error, so a grouped request that does not decompose — for any reason, including a non-numeric aggregate column type or a HAVING the adapter cannot merge, whether or not a HAVING is present — SHALL yield the qualified single-table wrapper (`GroupByWrapper`) shape identically on the empty and non-empty paths, rather than the hard-error decline both paths previously surfaced (issue #195)
* *AND* the empty grouped-fallback (`GroupByWrapper`) shape SHALL type its columns from `selectListDataTypes` when present — a positional shape Exasol accepts against it, not a raw row projection — and when `selectListDataTypes` is absent or empty SHALL fall back to the full-row-projection empty shape, matching the pre-refactor empty-result behavior byte-for-byte (this refactor changes routing structure, not column-shape selection)
* *AND* the short-circuit SHALL be reached from the RESOLVED FILE LIST alone, so an Iceberg table whose manifests pruned to zero files and a Delta table whose `add` statistics pruned to zero files take the identical path and return the identical shape
<!-- /DELTA:CHANGED -->
