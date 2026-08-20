# Feature: Pushdown Planning — Empty Result When All Files Are Pruned

When plan-time file pruning (driven by the pushed-down filter) eliminates
every data file for a query, the adapter still returns a `pushdown` response
whose output column shape matches what the same query would have produced with
matching data. The short-circuit is shape-aware: it emits the correct empty
instance of whichever plan the non-empty path would have committed to — a
row-scan projection, a single-group aggregate row, or a grouped-aggregate
result — so Exasol's positional pushdown validation always accepts the response
instead of rejecting it for a column-count/type mismatch.

## Background

* File pruning happens at the resolve-once seam (`vs-adapter/pushdown-planning`):
  the adapter resolves the data-file list exactly once (from whichever format
  reader, Iceberg or Delta, the table uses), and the pushed filter MAY reduce
  that list to zero files.
* Exasol validates every `pushdown` response's projected columns positionally
  against the request's `selectListDataTypes` at pushdown-validation time; a
  response whose column count or declared types disagree is rejected (SQL code
  04000, e.g. `Expected number of columns is 1 but pushdown query has 4`).
* The empty-result response MUST NOT invoke the scan fan-out UDF, and MUST NOT
  reference any resolved data file: with zero files there is nothing to scan.
* The empty-result response's declared output column types come from the same
  type sources the non-empty path uses for that plan (the row projection's
  declared types; `selectListDataTypes` for aggregates), so the empty and
  non-empty shapes are identical for any given request — except the
  `GroupByWrapper` fall-through documented in the empty-result scenario: when an
  aggregate request carries no `selectListDataTypes` (absent or empty), the
  empty path renders the full-row-projection shape while the non-empty path
  renders its `selectList`-derived shape, so the two can differ in column count
  in that edge. A second such edge is the declined-ORDER-BY literal-only shape
  (`vs-adapter/pushdown-planning-literal-projection`): for a row-scan query
  projecting only literals with an ORDER BY on an unprojected column, the
  non-empty path falls back to the full base row (so the declined-ORDER-BY
  wrapper resolves), while the empty path keeps the narrow literal-only
  projection — so the two can differ in column count for that unsupported shape.
  Both edges only ever affect shapes the adapter declines to push down; neither
  returns wrong data.
* Credentials MUST NOT appear in any empty-result SQL string or error message.
* "Empty-aggregate semantics" follow single-node SQL over zero input rows: the
  COUNT family (`COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`) yields `0`;
  `SUM`, `MIN`, `MAX`, `AVG`, and the `STDDEV`/`VARIANCE` family yield `NULL`.
* This delta amends ONE clause of ONE scenario: the shared-classifier clause that
  currently requires the empty and non-empty paths to derive "the HAVING-present
  hard-error decline" from the shared request classifier. This plan deletes that
  decline from the classifier entirely (`vs-adapter/pushdown-module-structure`,
  `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback`, issue #195), so the clause must be
  amended or the recorded library would hold two contradictory normative statements
  about the same classifier.
* Nothing about the empty path's own behavior changes. The classifier is still the
  single owner of the routing decision, and each path still renders its own shape
  from it. Only the SET of outcomes that classifier can return narrows: the grouped
  tier no longer returns an error, so a grouped request that does not decompose
  yields the qualified single-table wrapper (`GroupByWrapper`) shape on both paths
  instead of failing on both.
* The empty path therefore gains no new branch: `empty_result_sql` re-invokes the
  same classifier and already renders the `GroupByWrapper` shape, which is exactly
  the shape a non-decomposable grouped request now classifies as.
* The `selectListDataTypes` typing rule for the empty `GroupByWrapper` shape is
  unchanged by this delta, including its documented absent/empty fall-back edge.

## Scenarios

### Scenario: Row-scan query with all files pruned returns a typed empty projection

* *GIVEN* a virtual schema over an Iceberg table backed by MinIO
* *AND* a plain row-scan query (no aggregate, no GROUP BY) whose WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that selects each projected item as `CAST(NULL AS <type>)` and produces zero rows (`WHERE 1=0`)
* *AND* the response's column count and per-column declared types SHALL equal those of the non-empty row-scan projection for the same request, aliasing each item with the SAME positional-unique naming rule (bare-column items keep their real column name; expression and literal items get a positional-unique synthetic alias), so a pruned query whose select list contains repeated literals — such as `SELECT 1, name, 1` — still yields a valid zero-row SELECT with unique column aliases and the correct arity
* *AND* the response MUST NOT invoke the scan SET UDF

### Scenario: Single-group aggregate with all files pruned returns one shape-correct empty row

* *GIVEN* a query whose select list is one or more supported single-group aggregates (no GROUP BY) over the whole table, e.g. `SELECT COUNT(*), SUM(x), MIN(x), MAX(x), AVG(x) FROM {vs_table} WHERE id > {beyond every file's max}`
* *AND* the WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces exactly one row whose column count equals the number of requested aggregates
* *AND* each `COUNT`/`COUNT(col)` output column SHALL be `0` and each `SUM`/`MIN`/`MAX`/`AVG`/`STDDEV*`/`VARIANCE*` output column SHALL be `NULL`, every column cast to the aggregate's declared result type from `selectListDataTypes`
* *AND* the merged empty result SHALL equal the same aggregate evaluated over zero rows on a single node (the response invokes no scan UDF, per Background)

### Scenario: Single-group COUNT(DISTINCT) with all files pruned returns zero

* *GIVEN* a query whose select list includes `COUNT(DISTINCT col)` over the whole table with no GROUP BY, e.g. `SELECT COUNT(DISTINCT id) FROM {vs_table} WHERE id > {beyond every file's max}`
* *AND* the WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response that produces exactly one row whose single `COUNT(DISTINCT col)` output column is `0`, cast to the aggregate's declared result type
* *AND* the response MUST NOT invoke the scan fan-out UDF and MUST NOT reference any resolved data file

### Scenario: Multi-distinct or mixed single-group request with all files pruned matches the non-empty aggregate shape

* *GIVEN* a single-group query whose select list carries more than one `COUNT(DISTINCT col)`, OR a `COUNT(DISTINCT col)` alongside one or more ordinary SUM/MIN/MAX/COUNT/AVG aggregates (Case 2/3), e.g. `SELECT COUNT(DISTINCT a), COUNT(DISTINCT b), SUM(x) FROM {vs_table} WHERE {prunes every file}`
* *AND* the WHERE predicate prunes 100% of the table's data files during plan-time file pruning
* *WHEN* Exasol sends the corresponding `pushdown` request
* *THEN* the adapter SHALL return a `pushdown` response producing exactly one row of N columns — one per select-list item, in order — each cast to its declared result type: every `COUNT`/`COUNT(DISTINCT)` column is `0` and every `SUM`/`MIN`/`MAX`/`AVG` column is `NULL`
* *AND* that N-column shape SHALL be identical to the qualified single-table wrapper the non-empty Case 2/3 path returns (`vs-adapter/pushdown-planning-count-distinct`), so the empty and non-empty column counts and types never diverge and Exasol's positional pushdown validation accepts both (never a `04000` mismatch)
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
* *THEN* the adapter SHALL choose the empty-result shape using the SAME plan-detection priority the non-empty path uses — grouped aggregate first, then single-group aggregate, then row scan
* *AND* the single-group aggregate shape SHALL be chosen only when the aggregate column types pass the same numeric-type validation the non-empty path applies (so an aggregate the non-empty path demotes to a row scan produces the row-scan empty shape, not an aggregate shape)
* *AND* the empty and non-empty paths SHALL derive that priority and those validation gates from one shared request classifier, so the ROUTING decision is shared by construction rather than kept in lockstep by convention; each path then renders its own shape from that shared decision
* *AND* that shared classifier SHALL raise NO grouped-tier hard error, so a grouped request that does not decompose — for any reason, including a non-numeric aggregate column type or a HAVING the adapter cannot merge, whether or not a HAVING is present — SHALL yield the qualified single-table wrapper (`GroupByWrapper`) shape identically on the empty and non-empty paths, rather than the hard-error decline both paths previously surfaced (issue #195)
* *AND* the empty grouped-fallback (`GroupByWrapper`) shape SHALL type its columns from `selectListDataTypes` when present — a positional shape Exasol accepts against it, not a raw row projection — and when `selectListDataTypes` is absent or empty SHALL fall back to the full-row-projection empty shape, matching the pre-refactor empty-result behavior byte-for-byte (this refactor changes routing structure, not column-shape selection)
* *AND* the short-circuit SHALL be reached from the RESOLVED FILE LIST alone, so an Iceberg table whose manifests pruned to zero files and a Delta table whose `add` statistics pruned to zero files take the identical path and return the identical shape
