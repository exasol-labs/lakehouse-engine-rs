# Feature: Pushdown Planning, Empty Result When All Files Are Pruned

When plan-time file pruning eliminates every data file, the adapter returns a `pushdown` response whose column shape matches what the same query returns with data. The response is the empty instance of the plan the non-empty path would commit to: a row-scan projection, a single-group aggregate row, or a grouped-aggregate result.

## Background

* The adapter resolves the data-file list once (`pushdown/pushdown-planning`), from the Iceberg or Delta reader, and the pushed filter can reduce that list to zero files.
* Exasol validates each `pushdown` response's columns positionally against the request's `selectListDataTypes`. A response with a different column count or declared types is rejected (SQL code 04000, for example `Expected number of columns is 1 but pushdown query has 4`).
* With zero files there is nothing to scan, so the empty-result response does not invoke the scan fan-out UDF and references no data file.
* The declared column types come from the same sources the non-empty path uses: the row projection's declared types, `selectListDataTypes` for grouped aggregates, and Exasol's own typing of the request for an aggregate without GROUP BY.
* When an aggregate request carries no `selectListDataTypes` (absent or empty), the empty path renders the full-row-projection shape while the non-empty path renders its select-list shape. The column counts can differ in that case, which affects only shapes the adapter declines to push down.
* Exasol computes empty-aggregate semantics itself, over the zero-row relation the single-group empty shape selects from.
* A single request classifier owns the routing decision for both paths. A grouped request that does not decompose yields the qualified single-table wrapper (`GroupByWrapper`) shape on both.
* Credentials never appear in any empty-result SQL string or error message.

## Scenarios

### Scenario: Row-scan query with all files pruned returns a typed empty projection

* *GIVEN* a virtual schema over an Iceberg table backed by SeaweedFS
* *AND* a plain row-scan query (no aggregate, no GROUP BY) whose WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that selects each projected item as `CAST(NULL AS <type>)` and produces zero rows (`WHERE 1=0`)
* *AND* the response's column count and per-column declared types SHALL equal those of the non-empty row-scan projection for the same request, aliasing each item with the SAME positional-unique naming rule (bare-column items keep their real column name; expression and literal items get a positional-unique synthetic alias), so a pruned query whose select list contains repeated literals, such as `SELECT 1, name, 1`, still yields a valid zero-row SELECT with unique column aliases and the correct arity
* *AND* the response MUST NOT invoke the scan SET UDF

### Scenario: Aggregate without GROUP BY with all files pruned returns the native empty-set result

* *GIVEN* a query with an aggregate and no GROUP BY, whatever its select list holds: decomposable aggregates, `COUNT(DISTINCT ...)`, constants, aggregates the adapter does not decompose, or no aggregate outside HAVING, e.g. `SELECT 'X', COUNT(*) FROM {vs_table} WHERE {prunes every file} HAVING COUNT(*) = 0`
* *AND* the WHERE predicate prunes 100% of the table's data files during plan-time file pruning, or one side of an inner join has zero files
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return the request's select list, HAVING, and LIMIT rendered over one zero-row relation per table, each typed like that table's columns, so Exasol evaluates the request over the empty set
* *AND* the result SHALL equal native Exasol: exactly one row when HAVING is absent or true (`COUNT` yields `0`, every other aggregate `NULL`, and constants and scalar expressions are evaluated over those values), and no row when HAVING is false
* *AND* the response MUST NOT invoke the scan fan-out UDF and MUST NOT reference any resolved data file

### Scenario: Grouped aggregate with all files pruned returns zero rows in grouped shape

* *GIVEN* a grouped-aggregate query, e.g. `SELECT k, COUNT(*), SUM(x) FROM {vs_table} WHERE id > {beyond every file's max} GROUP BY k`
* *AND* the WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces zero rows (`... FROM DUAL WHERE 1=0`) whose column count and per-column declared types equal the grouped output shape (group-key columns, merged-aggregate columns, and any constant select-list columns, in the user's select-list order)
* *AND* the response MUST NOT invoke the scan SET UDF, and MUST NOT need to render the request's `HAVING`, `ORDER BY`, or `LIMIT` (a zero-row result already satisfies every one of them)

### Scenario: Empty-result shape matches the plan the non-empty path would commit to

* *GIVEN* any `pushdown` request whose filter prunes 100% of the table's data files during plan-time file pruning, whichever table format's reader resolved that list
* *WHEN* the adapter reaches the zero-files short-circuit
* *THEN* the adapter SHALL choose the empty-result shape using the SAME plan-detection priority the non-empty path uses, grouped aggregate first, then single-group aggregate, then row scan
* *AND* a request Exasol marks `aggregationType: single_group` SHALL take the single-group empty shape whether or not the non-empty path decomposes it, and never the row-scan empty shape
* *AND* the empty and non-empty paths SHALL derive that priority and those validation gates from one shared request classifier, so the ROUTING decision is shared by construction rather than kept in lockstep by convention; each path then renders its own shape from that shared decision
* *AND* that shared classifier SHALL raise NO grouped-tier hard error, so a grouped request that does not decompose, for any reason, including a non-numeric aggregate column type or a HAVING the adapter cannot merge, whether or not a HAVING is present, SHALL yield the qualified single-table wrapper (`GroupByWrapper`) shape identically on the empty and non-empty paths
* *AND* the empty grouped-fallback (`GroupByWrapper`) shape SHALL type its columns from `selectListDataTypes` when present, a positional shape Exasol accepts against it, not a raw row projection, and when `selectListDataTypes` is absent or empty SHALL fall back to the full-row-projection empty shape, matching the pre-refactor empty-result behavior byte-for-byte (this refactor changes routing structure, not column-shape selection)
* *AND* the short-circuit SHALL be reached from the RESOLVED FILE LIST alone, so an Iceberg table whose manifests pruned to zero files and a Delta table whose `add` statistics pruned to zero files take the identical path and return the identical shape

### Scenario: An empty-result plan carries no storage block at all

* *GIVEN* a pushdown request this feature short-circuits to an empty result
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the returned SQL SHALL carry no scan-spec storage value, neither a credential nor a connection reference, so the credential-reference guarantee holds unconditionally
