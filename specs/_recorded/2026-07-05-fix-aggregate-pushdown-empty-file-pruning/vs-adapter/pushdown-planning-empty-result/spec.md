# Feature: Pushdown Planning — Empty Result When All Files Are Pruned

When Iceberg-level file pruning (driven by the pushed-down filter) eliminates
every data file for a query, the adapter still returns a `pushdown` response
whose output column shape matches what the same query would have produced with
matching data. The short-circuit is shape-aware: it emits the correct empty
instance of whichever plan the non-empty path would have committed to — a
row-scan projection, a single-group aggregate row, or a grouped-aggregate
result — so Exasol's positional pushdown validation always accepts the response
instead of rejecting it for a column-count/type mismatch.

## Background

* File pruning happens at the resolve-once seam (`vs-adapter/pushdown-planning`):
  the adapter resolves the Iceberg data-file list exactly once, and the pushed
  filter MAY reduce that list to zero files.
* Exasol validates every `pushdown` response's projected columns positionally
  against the request's `selectListDataTypes` at pushdown-validation time; a
  response whose column count or declared types disagree is rejected (SQL code
  04000, e.g. `Expected number of columns is 1 but pushdown query has 4`).
* The empty-result response MUST NOT invoke the scan SET UDF or the scalar
  distinct-merge UDF, and MUST NOT reference any resolved data file: with zero
  files there is nothing to scan or merge.
* The empty-result response's declared output column types come from the same
  type sources the non-empty path uses for that plan (the row projection's
  declared types; `selectListDataTypes` for aggregates), so the empty and
  non-empty shapes are identical for any given request.
* Credentials MUST NOT appear in any empty-result SQL string or error message.
* "Empty-aggregate semantics" follow single-node SQL over zero input rows: the
  COUNT family (`COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`) yields `0`;
  `SUM`, `MIN`, `MAX`, `AVG`, and the `STDDEV`/`VARIANCE` family yield `NULL`.

## Scenarios

### Scenario: Row-scan query with all files pruned returns a typed empty projection

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a plain row-scan query (no aggregate, no GROUP BY) whose WHERE predicate prunes 100% of the table's data files at the Iceberg level
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that selects each projected column as `CAST(NULL AS <declared-type>)` and produces zero rows (`... FROM DUAL WHERE 1=0`)
* *AND* the response's column count and per-column declared types SHALL equal those of the non-empty row-scan projection for the same request
* *AND* the response MUST NOT invoke the scan SET UDF

### Scenario: Single-group aggregate with all files pruned returns one shape-correct empty row

* *GIVEN* a query whose select list is one or more supported single-group aggregates (no GROUP BY) over the whole table, e.g. `SELECT COUNT(*), SUM(x), MIN(x), MAX(x), AVG(x) FROM {vs_table} WHERE id > {beyond every file's max}`
* *AND* the WHERE predicate prunes 100% of the table's data files at the Iceberg level
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces exactly one row whose column count equals the number of requested aggregates
* *AND* each `COUNT`/`COUNT(col)` output column SHALL be `0` and each `SUM`/`MIN`/`MAX`/`AVG`/`STDDEV*`/`VARIANCE*` output column SHALL be `NULL`, every column cast to the aggregate's declared result type from `selectListDataTypes`
* *AND* the merged empty result SHALL equal the same aggregate evaluated over zero rows on a single node (the response invokes no scan UDF, per Background)

### Scenario: Single-group COUNT(DISTINCT) with all files pruned returns zero

* *GIVEN* a query whose select list includes `COUNT(DISTINCT col)` over the whole table with no GROUP BY, e.g. `SELECT COUNT(DISTINCT id) FROM {vs_table} WHERE id > {beyond every file's max}`
* *AND* the WHERE predicate prunes 100% of the table's data files at the Iceberg level
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces exactly one row whose single `COUNT(DISTINCT col)` output column is `0`, cast to the aggregate's declared result type
* *AND* the response MUST NOT invoke the scalar distinct-merge UDF or emit any `LISTAGG` union over per-shard distinct sets

### Scenario: Grouped aggregate with all files pruned returns zero rows in grouped shape

* *GIVEN* a grouped-aggregate query, e.g. `SELECT k, COUNT(*), SUM(x) FROM {vs_table} WHERE id > {beyond every file's max} GROUP BY k`
* *AND* the WHERE predicate prunes 100% of the table's data files at the Iceberg level
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces zero rows (`... FROM DUAL WHERE 1=0`) whose column count and per-column declared types equal the grouped output shape (group-key columns, merged-aggregate columns, and any constant select-list columns, in the user's select-list order)
* *AND* the response MUST NOT invoke the scan SET UDF, and MUST NOT need to render the request's `HAVING`, `ORDER BY`, or `LIMIT` (a zero-row result already satisfies every one of them)

### Scenario: Empty-result shape matches the plan the non-empty path would commit to

* *GIVEN* any `pushdown` request whose filter prunes 100% of the table's data files at the Iceberg level
* *WHEN* the adapter reaches the zero-files short-circuit
* *THEN* the adapter SHALL choose the empty-result shape using the SAME plan-detection priority the non-empty path uses — grouped aggregate first, then single-group aggregate, then row scan
* *AND* the single-group aggregate shape SHALL be chosen only when the aggregate column types pass the same numeric-type validation the non-empty path applies (so an aggregate the non-empty path demotes to a row scan produces the row-scan empty shape, not an aggregate shape)
* *AND* the resulting response's positional column shape SHALL be one Exasol accepts against `selectListDataTypes`, never a raw row-projection shape returned for an aggregate request
