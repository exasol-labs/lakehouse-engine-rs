# Feature: Pushdown Planning

Translates an Exasol query against the virtual schema into a pushdown plan: it resolves
the Iceberg data-file list once, captures the requested projection, filter, LIMIT, and
any supported aggregate, extracts the table's current Iceberg schema for field-id-based
projection, and emits the SQL that drives the DataFusion scan.

## Background

* This delta SUPERSEDES the preceding Background bullet "A predicate node the adapter cannot faithfully translate is OMITTED from the scan spec; Exasol keeps and evaluates the predicate itself as a correctness backstop." That claim is FALSE and is corrected, not merely superseded: Exasol decides what to delegate from the capabilities response alone, before the pushdown request exists, and never re-checks a predicate it delegated. A predicate the adapter cannot faithfully translate MUST be applied by the adapter's own returned SQL — see `vs-adapter/pushdown-declined-filter-self-apply`. Omitting it returns extra unfiltered rows, verified live.
* The single-table path distinguishes an ABSENT filter from a DECLINED one. An absent or trivially-true filter is omitted and the wrapper-free fast scan is unchanged; a declined filter routes the request to the qualified single-table wrapper, which applies the predicate in its own `WHERE`.
* The wrapper-free outer scalar scan select remains the shape for every request whose filter renders. The `SELECT * FROM (…)` boundary the wrapper introduces exists only on the decline path.

## Scenarios

<!-- DELTA:CHANGED -->
### Scenario: Filter predicate is pushed into the scan spec

* *GIVEN* a query with a WHERE predicate over a supported column and operator
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL translate the predicate into the shard-invariant common spec passed to the UDF, and the translation SHALL be ALL-OR-NOTHING over the whole top-level filter — REPLACING the recorded "omitting (never mistranslating) any node it cannot render", which sanctioned dropping one node while keeping the rest of the tree
* *AND* a filter the adapter cannot render for DataFusion SHALL be self-applied in the qualified wrapper's `WHERE` rather than omitted, per `vs-adapter/pushdown-declined-filter-self-apply`, because Exasol re-applies nothing it delegated
* *AND* before translating a `predicate_like` or `predicate_like_regexp` whose subject is a bare `column` node, the adapter SHALL apply the type-aware LIKE rule (see `vs-adapter/pushdown-planning-like-type-coercion`), because DataFusion performs no implicit non-string-to-VARCHAR coercion and would hard-fail the scan on a LIKE over a non-string column
* *AND* the adapter SHALL ALSO translate the soundly-translatable conjuncts into an `iceberg::expr::Predicate` applied to the Iceberg table scan as a file-pruning filter, dropping any node it cannot translate soundly rather than skipping a file that could match
* *AND* the DataFusion scan SHALL always apply the full common-spec filter, so the Iceberg pruning filter only narrows which files are opened and never changes the result set
<!-- /DELTA:CHANGED -->

<!-- DELTA:NEW -->
### Scenario: A declined WHERE filter routes the single-table request to the qualified wrapper

* *GIVEN* a single-table `pushdown` request carrying a non-null `filter` that the DataFusion-bound render declines, of any dispatch shape — row scan, top-N, single-group aggregate, grouped aggregate, or `COUNT(DISTINCT)`
* *WHEN* the pushdown dispatcher selects the SQL shape
* *THEN* the dispatcher SHALL route the request to the qualified single-table wrapper BEFORE the routing classifier runs, so one route serves every dispatch shape
* *AND* the request's ORIGINAL filter tree SHALL still be forwarded to Iceberg-level file pruning unchanged, because pruning reads the un-rewritten tree and only ever removes files that provably cannot match
* *AND* the wrapper's returned column count, order, and declared types SHALL equal what the request's `selectList` declares, and an absent, JSON-null, or empty `selectList` SHALL return the FULL base row rather than only the columns the declined predicate references, so the route never trips Exasol's positional `04000` validation
* *AND* a request whose filter renders, or which carries no filter, SHALL take its existing dispatch shape with the emitted SQL byte-identical to its pre-change output
<!-- /DELTA:NEW -->

