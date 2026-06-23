# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it
resolves the Iceberg data-file list once, captures the requested projection, filter,
LIMIT, and any supported single-group or grouped aggregate, and emits the SQL that
drives the DataFusion scan SET UDF — fanned out across G oversubscribed work-unit
shards via `GROUP BY shard_key` — over exactly those files.

## Background

* The adapter receives a `pushdown` request carrying the projection, filter, and
  aggregate specification from Exasol, and resolves the Iceberg file list once per query.
* Filter, select-list, group-key, and HAVING expressions are all rendered by the shared
  `crates/vs-expression` translator; an untranslatable expression is omitted/falls back
  rather than producing an incorrect result.
* An aggregate is pushed down only when it decomposes into a shard-associative
  partial/merge plan; otherwise the adapter falls back to row scanning.
* Credentials MUST NOT appear in any returned SQL or error message.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Adapter advertises aggregate pushdown for supported functions

* *GIVEN* an Exasol session that has installed the VS adapter script
* *WHEN* Exasol sends a `getCapabilities` request to the adapter
* *THEN* the capabilities list SHALL include single-group aggregate pushdown for `COUNT`/`COUNT(*)`/`SUM`/`MIN`/`MAX`/`AVG`, `AGGREGATE_GROUP_BY_COLUMN`/`AGGREGATE_GROUP_BY_EXPRESSION`/`AGGREGATE_HAVING`, the decomposable statistical aggregates `FN_AGG_STDDEV`/`FN_AGG_STDDEV_POP`/`FN_AGG_STDDEV_SAMP`/`FN_AGG_VARIANCE`/`FN_AGG_VAR_POP`/`FN_AGG_VAR_SAMP`, and (still) column projection, scalar select-list expressions, filter predicates, and LIMIT
* *AND* the capabilities list MUST NOT include `AGGREGATE_GROUP_BY_TUPLE`, `FN_AGG_COUNT_DISTINCT` (or any other `*_DISTINCT` aggregate), `FN_AGG_MEDIAN`, `FN_AGG_APPROXIMATE_COUNT_DISTINCT`, `FN_AGG_GROUP_CONCAT*`/`FN_AGG_LISTAGG`, or join pushdown
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: Scalar select-list expression is pushed into the scan-driving query

* *GIVEN* a query whose select list contains a scalar expression over table columns (e.g. `UPPER(name)`, `price * qty`, `EXTRACT(YEAR FROM order_date)`)
* *AND* the adapter advertises `SELECTLIST_EXPRESSIONS`
* *WHEN* Exasol sends the `pushdown` request carrying that select-list expression
* *THEN* the adapter SHALL render each select-list expression node to a DataFusion SQL fragment using the VS expression translator (raising mode) and carry the rendered fragments in the scan spec so the scan UDF projects exactly those expressions
* *AND* the UDF's declared EMITS column list SHALL match the rendered select-list expressions in order and result type
* *AND* a select-list item the adapter cannot translate SHALL cause the adapter to fall back to projecting the underlying columns and let Exasol evaluate the expression, rather than producing an incorrect result

### Scenario: HAVING predicate is pushed into the grouped scan plan

* *GIVEN* a grouped aggregate `pushdown` request carrying a `having` predicate over the grouped aggregates and/or group keys
* *AND* the adapter advertises `AGGREGATE_HAVING`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render the HAVING predicate to a DataFusion SQL fragment using the same VS expression translator path used for WHERE predicates
* *AND* the adapter SHALL apply the rendered HAVING predicate only in the OUTER wrapper SQL that merges the per-shard partial-aggregate rows, never inside the per-shard partial scan (a per-shard HAVING would discard groups that only meet the threshold after merge)
* *AND* a HAVING predicate the adapter cannot translate SHALL be omitted from the wrapper SQL and retained by Exasol as a correctness backstop rather than producing an incorrect result

### Scenario: Decomposable statistical aggregate is pushed down via sufficient statistics

* *GIVEN* a query selecting `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP` over a column, optionally with a GROUP BY clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL instruct the scan UDF to emit, per shard (and per group when grouped), the sufficient statistics `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` rather than a per-shard standard deviation or variance
* *AND* the outer wrapper SQL SHALL merge the per-shard sufficient statistics into the final variance as `(SUM(sum_sq) - SUM(sum)*SUM(sum)/SUM(cnt)) / d`, where `d` is `SUM(cnt)` for the population forms and `SUM(cnt) - 1` for the sample forms, and the final standard deviation as the square root of that variance
* *AND* the wrapper SHALL yield NULL (never divide by zero or take the square root of a negative rounding artifact) when the merged count is zero, or one for the sample forms
* *AND* the merged result SHALL equal the result of the same statistical aggregate evaluated over all rows on a single node within floating-point tolerance

### Scenario: Adapter falls back for non-decomposable aggregates

* *GIVEN* a `pushdown` request whose select list contains an aggregate the adapter does not advertise as decomposable (e.g. `MEDIAN`, `COUNT(DISTINCT ...)`, `APPROXIMATE_COUNT_DISTINCT`, `LISTAGG`, `GROUP_CONCAT`)
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field)
* *AND* Exasol SHALL compute the aggregate on the returned rows using its own engine
* *AND* the adapter MUST NOT emit a partial/merge plan for any aggregate it cannot decompose into shard-associative sufficient statistics, because doing so would yield an incorrect result
<!-- /DELTA:NEW -->
