# Feature: Pushdown Planning, Empty Result When All Files Are Pruned

When plan-time file pruning eliminates every data file, the adapter returns a `pushdown` response whose column shape matches what the same query returns with data. The response is the empty instance of the plan the non-empty path would commit to: a row-scan projection, a single-group aggregate row, or a grouped-aggregate result.

<!-- DELTA:CHANGED -->
## Background

* The adapter resolves the data-file list once (`pushdown/pushdown-planning`), and the pushed filter can reduce it to zero files. With zero files the empty-result response invokes no scan fan-out and references no data file.
* Exasol validates each `pushdown` response's columns positionally against the request's `selectListDataTypes` and rejects a different column count or type (SQL code 04000, for example `Expected number of columns is 1 but pushdown query has 4`).
* The declared column types come from the sources the non-empty path uses: the row projection's declared types, `selectListDataTypes` for grouped aggregates, and Exasol's own typing of the request for an aggregate without GROUP BY. Exasol computes empty-aggregate semantics itself, over the zero-row relation the single-group empty shape selects from.
* A grouped request that does not decompose yields the qualified single-table wrapper shape on both.
* Credentials never appear in any empty-result SQL string or error message.
<!-- /DELTA:CHANGED -->

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Empty-result shape matches the plan the non-empty path would commit to

* *GIVEN* any `pushdown` request whose filter prunes 100% of the table's data files during plan-time file pruning, whichever table format's reader resolved that list
* *WHEN* the adapter reaches the zero-files short-circuit
* *THEN* the adapter SHALL choose the empty-result shape using the same plan-detection priority the non-empty path uses, grouped aggregate first, then single-group aggregate, then row scan
* *AND* a request Exasol marks `aggregationType: single_group` SHALL take the single-group empty shape whether or not the non-empty path decomposes it, and never the row-scan empty shape
* *AND* the empty and non-empty paths SHALL choose the same shape for the same request
* *AND* a grouped request that does not decompose, for any reason, including a non-numeric aggregate column type, a select-list item the scan cannot emit, or a HAVING the adapter cannot merge, SHALL yield the qualified single-table wrapper shape identically on the empty and non-empty paths
* *AND* the empty grouped-fallback shape SHALL type its columns from `selectListDataTypes`, a positional shape Exasol accepts, not a raw row projection
* *AND* the short-circuit SHALL be reached from the resolved file list alone, so an Iceberg table whose manifests pruned to zero files and a Delta table whose `add` statistics pruned to zero files take the identical path and return the identical shape
<!-- /DELTA:CHANGED -->
