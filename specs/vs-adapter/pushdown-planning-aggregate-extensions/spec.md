# Feature: Pushdown Planning — Aggregate Extensions (HAVING & Statistical Aggregates)

Extends pushdown planning (`vs-adapter/pushdown-planning`) with the newly advertised
aggregate-side capabilities: HAVING clause pushdown over the partial/merge decomposition,
and decomposable statistical aggregate pushdown via sufficient statistics. Also records the
boundary of aggregate decomposition — which aggregates fall back to row scanning rather than
being pushed as a partial/merge plan.

## Background

* An aggregate is pushed down only when it decomposes into a shard-associative
  partial/merge plan; otherwise the adapter falls back to row scanning.
* Credentials MUST NOT appear in any returned SQL or error message.
* This delta corrects one recorded claim, and only that claim: that a HAVING the adapter
  cannot render is omitted from the returned SQL and re-applied by Exasol as a correctness
  backstop. No such behavior exists, and relying on it would return wrong rows.
* No code path omits a HAVING. The adapter has exactly two HAVING renderers, and neither
  omits: `grouped_agg.rs::render_having_over_merge` (the partial/merge path) and
  `joins/sql_builders.rs::qualified_join_having` (the qualified-wrapper and N-scan join path,
  which raises `UdfError::User` on an unrenderable HAVING). The "omitted and retained by
  Exasol" clause therefore described behavior that was never implemented, so no code can rely
  on it regardless of how Exasol behaves.
* The adapter's own code asserts the corrected HAVING rule in six places — `request_shape.rs`
  lines 16, 70, and 85; `grouped_agg.rs` line 3394; `file_resolution.rs` line 1480; and
  `mod.rs` line 363 — all stating that Exasol will not re-apply a HAVING the adapter advertised
  `AGGREGATE_HAVING` for (`capabilities.rs` line 171). The recorded spec was the outlier.
* Exasol's re-apply behavior varies by pushed shape, which is itself a reason no unrenderable
  clause may depend on it. Live precedent under `add-topn-pushdown` B5/B6 (issues #225 / #189):
  an `orderBy` pushed TOGETHER with a `limit` is fully delegated — Exasol re-applies neither, so
  the withheld-limit fallback returned wrong, unsorted, unbounded rows and the adapter now
  renders a self-contained global `ORDER BY … LIMIT` (`topn.rs` lines 444-449, `mod.rs` lines
  690-694). An `orderBy` pushed WITHOUT a `limit` behaves differently: Exasol keeps its own
  top-level `ORDER BY` and re-sorts the returned rows (`tests/e2e_scan_test.rs` lines 1133-1138).
* The correct handling for a HAVING the adapter cannot render over the partial/merge
  decomposition is therefore neither omission nor an error: route the request to the qualified
  single-table wrapper, which renders the HAVING as ordinary Exasol SQL over materialized rows.
  See `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback` (issue #195).
* This delta SUPERSEDES the preceding Background bullet "This delta does NOT adjudicate the
  WHERE-filter backstop. An untranslatable WHERE predicate is genuinely omitted from the scan spec
  (`vs_expression::render_df_filter_safe` returns `None`), a distinct mechanism with its own
  capability story; the Background statement about filter and select-list expressions stands as
  recorded (see `vs-adapter/pushdown-planning-selectlist-expressions`). Only the HAVING claim is
  corrected." The WHERE-filter backstop is now adjudicated and the deferred claim was wrong: an
  untranslatable WHERE predicate omitted from the scan spec is applied by nobody and returns extra
  rows, verified live. The correct handling is the SAME one this delta already chose for HAVING —
  route the request to the qualified single-table wrapper and render the clause as ordinary Exasol
  SQL over materialized rows. See `vs-adapter/pushdown-declined-filter-self-apply`.
* The two clauses now share one destination, so the wrapper is the single place a clause the
  partial/merge decomposition cannot express is evaluated. A declined WHERE renders as the
  wrapper's `WHERE`, ahead of the aggregate; a declined HAVING renders as the wrapper's `HAVING`,
  after it.
* Iceberg spec compliance: checked, not engaged. This delta changes only HAVING routing and
  the statistical-aggregate merge SQL; it touches no manifest, schema-resolution, field-id, or
  type-mapping surface, so no normative Iceberg requirement applies and there is no deviation
  to fix or track.

## Scenarios

### Scenario: A declined WHERE filter under an aggregate request is applied ahead of the aggregate

* *GIVEN* an aggregate or `groupBy` `pushdown` request whose WHERE filter the DataFusion-bound render declines
* *WHEN* the adapter routes the request to the qualified single-table wrapper
* *THEN* the wrapper SHALL render the declined predicate as its `WHERE`, positioned between the raw sharded fan-out and the aggregate select list, GROUP BY, and HAVING it renders
* *AND* the per-shard fan-out SHALL carry no aggregate, no group keys, and no filter, so no shard aggregates unfiltered rows
* *AND* the returned aggregate value SHALL equal native Exasol evaluation of the same query, not the aggregate over the unfiltered row set the omission returned

### Scenario: HAVING predicate is pushed into the grouped scan plan

* *GIVEN* a grouped aggregate `pushdown` request carrying a `having` predicate over the grouped aggregates and group keys
* *AND* the adapter advertises `AGGREGATE_HAVING`
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL render the HAVING predicate to a DataFusion SQL fragment using the same VS expression translator path used for WHERE predicates
* *AND* the adapter SHALL apply the rendered HAVING predicate only in the OUTER wrapper SQL that merges the per-shard partial-aggregate rows, never inside the per-shard partial scan (a per-shard HAVING would discard groups that only meet the threshold after merge)
* *AND* the adapter MUST NOT omit a HAVING it cannot render from the returned SQL, because Exasol does not re-apply a HAVING whose `AGGREGATE_HAVING` capability the adapter advertises — omission returns wrong rows
* *AND* a HAVING the adapter cannot render over the partial/merge decomposition SHALL instead route the request to the qualified single-table wrapper, which renders the HAVING as ordinary Exasol SQL over materialized rows so the predicate is preserved rather than dropped (see `vs-adapter/pushdown-planning-grouped-agg-wrapper-fallback`, issue #195)

### Scenario: Decomposable statistical aggregate is pushed down via sufficient statistics

* *GIVEN* a query selecting `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP` over a column, optionally with a GROUP BY clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL instruct the scan UDF to emit, per shard (and per group when grouped), the sufficient statistics `COUNT(col)`, `SUM(col)`, and `SUM(col*col)` rather than a per-shard standard deviation or variance
* *AND* the outer wrapper SQL SHALL merge the per-shard sufficient statistics into the final variance as `(SUM(sum_sq) - SUM(sum)*SUM(sum)/SUM(cnt)) / d`, where `d` is `SUM(cnt)` for the population forms and `SUM(cnt) - 1` for the sample forms, and the final standard deviation as the square root of that variance
* *AND* the wrapper SHALL yield NULL (never divide by zero or take the square root of a negative rounding artifact) when the merged count is zero, or one for the sample forms
* *AND* both single-group and grouped aggregate merge expressions SHALL be wrapped in `CAST(<expr> AS <declared_type>)` to match the declared Exasol output column type, satisfying Exasol's strict pushdown output-type validation
* *AND* the merged result SHALL equal the result of the same statistical aggregate evaluated over all rows on a single node within floating-point tolerance

### Scenario: Adapter falls back for non-decomposable aggregates

* *GIVEN* a `pushdown` request whose select list contains an aggregate the adapter does not advertise as decomposable (e.g. `MEDIAN`, `APPROXIMATE_COUNT_DISTINCT`, `LISTAGG`, `GROUP_CONCAT`, or a `COUNT(DISTINCT ...)` that appears inside a GROUP BY request)
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field)
* *AND* Exasol SHALL compute the aggregate on the returned rows using its own engine
* *AND* the adapter MUST NOT emit a partial/merge plan for any aggregate it cannot decompose into a shard-associative partial/merge plan, because doing so would yield an incorrect result
* *AND* a single-group (no GROUP BY) `COUNT(DISTINCT col)` SHALL NOT fall back here — it is decomposed via `vs-adapter/pushdown-planning-count-distinct` — while a `COUNT(DISTINCT ...)` inside a GROUP BY request SHALL still fall back
