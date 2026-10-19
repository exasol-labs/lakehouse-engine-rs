# Feature: Pushdown Planning — Aggregate Extensions (HAVING & Statistical Aggregates)

Extends pushdown planning (`pushdown/pushdown-planning`) with the newly advertised
aggregate-side capabilities: HAVING clause pushdown over the partial/merge decomposition,
and decomposable statistical aggregate pushdown via sufficient statistics. Also records the
boundary of aggregate decomposition — which aggregates fall back to row scanning rather than
being pushed as a partial/merge plan.

## Background

* An aggregate is pushed down only when it decomposes into a shard-associative
  partial/merge plan; otherwise the adapter falls back to row scanning.
* The statistical family's bare-column-only argument rule was implicit and UNENFORCED, and this delta makes it explicit. `parse_agg_item` (`single_group_agg.rs:280-299`) resolves a statistical aggregate's argument with `column_from_first_arg`, which returns `None` for any node whose `type` is not `column` — but it assigns that `None` to `AggregatePlan::column` rather than declining, so `STDDEV(<expression>)` yielded a plan with NEITHER `column` NOR `arg_expr` set.
* What CODE INSPECTION establishes about such a plan: `validate_agg_col_types` admits it, because `col_type_for(None, None, col_types, None)` falls through to the numeric `DOUBLE PRECISION` default; `partial_emits_items` sizes its three `EMITS` columns from the same default; and `partial_select_items` renders `COUNT("")`, `SUM("")`, and `SUM("" * "")`, which DataFusion rejects. What a LIVE CAPTURE measured, per CLAUDE.md § Verification discipline (Docker Exasol container, plan `refactor-pushdown-agg-dedup` task 1.2, 2026-07-31): Exasol PUSHES this argument shape, and the query FAILS today, on all four reachable paths. `EXPLAIN VIRTUAL` returned status `ok` and rendered the wrapper SQL for `SELECT STDDEV(score + id)`, for `SELECT VARIANCE(score * 2)`, for `SELECT MOD(id, 4), STDDEV(score + id) … GROUP BY MOD(id, 4)`, and for `SELECT MOD(id, 4), SQRT(STDDEV(score + id)) … GROUP BY MOD(id, 4)`; each then failed at execution with `sqlCode 22002` and `Schema error: No field named .`, prefixed `partial aggregate SQL error:` on the two ungrouped paths and `grouped partial aggregate SQL error:` on the two grouped ones. The rejection surfaces as an EMPTY field name rather than as the `column "" not found` text the inspection chain predicted; the rendered `COUNT("")` itself is as inspected.
* The capability advertisement leaves the argument shape unconstrained, and the capture shows Exasol exercising it: `capabilities.rs:176-181` advertises `FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, and `FN_AGG_VAR_SAMP` alongside the scalar-function capabilities, and nothing between the advertisement and the scan tests the argument's node type. So the decline turns a measured failing query into a correct result through the row scan or the qualified single-table wrapper, on the ungrouped detection path and on both grouped paths alike. It is a bug fix over a reachable shape, not a structural guard over an unreachable one.
* Declining is the correct fix rather than a better error message, and the alternative was considered and rejected. Routing the scan's stat branch through `agg_arg_sql` (as `scan-aggregation/scan-partial-agg-column-contract` does for its own reasons) replaces the captured empty field name in `Schema error: No field named .` with an explicit `__MISSING_AGG_ARGUMENT__` placeholder in the same schema error — a clearer failure, still a failure. Declining at detection returns the right rows.
* Extending the family to expression arguments is explicitly NOT this delta's scope. The scan's sufficient statistics would have to render the mean and M2 over a fragment evaluated several times, and the partial `EMITS` types would have to come from the declared result type rather than a source column — both are new behavior with their own correctness story, not a limit made explicit.
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
  top-level `ORDER BY` and re-sorts the returned rows (`tests/e2e_local/scan.rs`, `order_by_without_limit_falls_back_correctly`).
* The correct handling for a HAVING the adapter cannot render over the partial/merge
  decomposition is therefore neither omission nor an error: route the request to the qualified
  single-table wrapper, which renders the HAVING as ordinary Exasol SQL over materialized rows.
  See `pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback` (issue #195).
* This delta SUPERSEDES the preceding Background bullet "This delta does NOT adjudicate the
  WHERE-filter backstop. An untranslatable WHERE predicate is genuinely omitted from the scan spec
  (`vs_expression::render_df_filter_safe` returns `None`), a distinct mechanism with its own
  capability story; the Background statement about filter and select-list expressions stands as
  recorded (see `pushdown/pushdown-planning-selectlist-expressions`). Only the HAVING claim is
  corrected." The WHERE-filter backstop is now adjudicated and the deferred claim was wrong: an
  untranslatable WHERE predicate omitted from the scan spec is applied by nobody and returns extra
  rows, verified live. The correct handling is the SAME one this delta already chose for HAVING —
  route the request to the qualified single-table wrapper and render the clause as ordinary Exasol
  SQL over materialized rows. See `pushdown/pushdown-declined-filter-self-apply`.
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
* *AND* a HAVING the adapter cannot render over the partial/merge decomposition SHALL instead route the request to the qualified single-table wrapper, which renders the HAVING as ordinary Exasol SQL over materialized rows so the predicate is preserved rather than dropped (see `pushdown-grouped-agg/pushdown-planning-grouped-agg-wrapper-fallback`, issue #195)

### Scenario: Decomposable statistical aggregate is pushed down via sufficient statistics

* *GIVEN* a query selecting `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP` over a column, optionally with a GROUP BY clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL instruct the scan UDF to emit, per shard (and per group when grouped), the sufficient statistics `cnt`, `mean`, and `m2` (the sum of squared deviations from the shard mean) rather than a per-shard standard deviation or variance
* *AND* the outer wrapper SQL SHALL compute each group's grand mean `g` over the partial rows with window functions, as one shard's mean plus `SUM(cnt * (mean - ref)) / SUM(cnt)`, so that equal shard means yield exactly that mean
* *AND* the wrapper SHALL merge the partials as `M2 = SUM(m2) + SUM(cnt * (mean - g) * (mean - g))` and `N = SUM(cnt)`, and MUST NOT expand the deviation term into `SUM(cnt * mean²) - N * g²`, because that expansion cancels
* *AND* `VAR_POP` SHALL be `M2 / N`, `VAR_SAMP` SHALL be `M2 / (N - 1)` (NULL when the denominator is not positive), `VARIANCE` SHALL be `0` when `N = 1` and `M2 / (N - 1)` otherwise, and each standard deviation SHALL be the square root of its matching variance, because Exasol returns `0` from `VARIANCE` and `STDDEV` for one non-NULL value where the sample forms return NULL
* *AND* both single-group and grouped aggregate merge expressions SHALL be wrapped in `CAST(<expr> AS <declared_type>)` to match the declared Exasol output column type, satisfying Exasol's strict pushdown output-type validation
* *AND* the merged result SHALL equal the result of the same statistical aggregate evaluated over all rows on a single node within floating-point tolerance

### Scenario: Statistical aggregates match native Exasol where the values are large, constant, or single

* *GIVEN* a table spread over several shards with a BIGINT column whose squares exceed the BIGINT range, an INT column whose squares exceed the INT range, a group of equal values near 1e9 in a BIGINT column and near 1e12 in a DECIMAL(18,2) column, a one-row group, an all-NULL group, and a group whose one value sits beside a shard that holds only NULL
* *WHEN* the statistical aggregates run with and without GROUP BY
* *THEN* each result SHALL match native Exasol within a relative tolerance of 1e-9 while the average is at most 1e6 times the standard deviation
* *AND* a group whose non-NULL values are all equal SHALL return exactly `0`, also when they come from several shards
* *AND* a `HAVING VARIANCE(col) = 0` that the adapter merges over the partials SHALL keep the same groups as native Exasol, with and without GROUP BY
* *AND* where the average is 1e8 times the standard deviation, each result SHALL match native Exasol within a relative tolerance of 1e-7

### Scenario: Adapter falls back for non-decomposable aggregates

* *GIVEN* a `pushdown` request whose select list contains an aggregate the adapter does not advertise as decomposable (e.g. `MEDIAN`, `APPROXIMATE_COUNT_DISTINCT`, `LISTAGG`, `GROUP_CONCAT`, or a `COUNT(DISTINCT ...)` that appears inside a GROUP BY request)
* *WHEN* Exasol sends the request
* *THEN* the adapter SHALL fall back to row scanning (emitting a row-scan ScanSpec with no aggregates field)
* *AND* Exasol SHALL compute the aggregate on the returned rows using its own engine
* *AND* the adapter MUST NOT emit a partial/merge plan for any aggregate it cannot decompose into a shard-associative partial/merge plan, because doing so would yield an incorrect result
* *AND* a single-group (no GROUP BY) `COUNT(DISTINCT col)` SHALL NOT fall back here — it is decomposed via `pushdown-aggregates/pushdown-planning-count-distinct` — while a `COUNT(DISTINCT ...)` inside a GROUP BY request SHALL still fall back

### Scenario: Statistical aggregate over an expression argument declines the partial/merge pushdown

* *GIVEN* a `pushdown` request whose select list contains a statistical aggregate — `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP` — whose single argument is NOT a bare `column` node (for example `STDDEV(a + b)` or `STDDEV(LENGTH(name))`), with or without a GROUP BY clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL decline the aggregate pushdown for that item, returning no `AggregatePlan` for it, so the request routes to row scanning or to the qualified single-table wrapper and Exasol computes the statistic natively over the returned rows
* *AND* the adapter MUST NOT emit an `AggregatePlan` carrying neither a `column` nor an `arg_expr`, because such a plan passes `validate_agg_col_types` on the `DOUBLE PRECISION` default and then renders `COUNT("")`, `SUM("")`, and `SUM("" * "")` in the scan, failing the whole query where a decline returns the correct result
* *AND* a statistical aggregate over a BARE COLUMN SHALL continue to push down unchanged through the sufficient-statistics decomposition, with a byte-identical scan spec, `EMITS` clause, and merge expression
* *AND* the decline SHALL apply at ALL FIVE `parse_agg_item` call sites, not only the two detection entry points, so no path can emit or consume the malformed plan: `detect_aggregates` (`single_group_agg.rs:78`) SHALL decline the whole single-group select list, routing the request to the row scan; `detect_group_by_aggregates` (`grouped_agg.rs:215`) SHALL decline the whole grouped detection, routing it to the qualified single-table wrapper; `classify_scalar_over_aggregate` (`grouped_agg.rs:399`) SHALL decline a select item that WRAPS such an aggregate (for example `SQRT(STDDEV(a + b))` over a GROUP BY), so the grouped detection declines to that same wrapper instead of sizing three `EMITS` columns from the `DOUBLE PRECISION` default; and `render_scalar_over_merge` (`grouped_agg.rs:427`) and `render_having_over_merge` (`grouped_agg.rs:988`) SHALL return `None`, routing a HAVING or a merge ORDER BY over such an aggregate to the qualified wrapper — which is the outcome those two already produce through their `AggregatePlan` lookup against the detected plans, so their observable behavior is UNCHANGED and only the decline's reason moves to parse time
* *AND* the statistical family SHALL keep its bare-column-only argument rule; the adapter MUST NOT resolve the argument through `arg_column_or_expr` and MUST NOT populate `arg_expr` for a statistical aggregate, because rendering `SUM(<expr> * <expr>)` over a doubly-evaluated fragment and sizing the partial `EMITS` columns without a source column are new behavior outside this delta's scope

### Scenario: Generated SQL carries a credential reference, not a credential

* *GIVEN* a pushdown request through this feature's path
* *WHEN* the adapter generates the pushdown SQL
* *THEN* the credential-reference guarantee of `storage-access/scan-spec-credential-reference` SHALL apply, so no credential value appears in generated SQL or error messages
