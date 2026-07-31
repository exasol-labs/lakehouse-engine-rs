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

<!-- DELTA:NEW -->
* The statistical family's bare-column-only argument rule was implicit and UNENFORCED, and this delta makes it explicit. `parse_agg_item` (`single_group_agg.rs:280-299`) resolves a statistical aggregate's argument with `column_from_first_arg`, which returns `None` for any node whose `type` is not `column` — but it assigns that `None` to `AggregatePlan::column` rather than declining, so `STDDEV(<expression>)` yielded a plan with NEITHER `column` NOR `arg_expr` set.
* What CODE INSPECTION establishes about such a plan, stated without inferring the query-level outcome from it: `validate_agg_col_types` admits it, because `col_type_for(None, None, col_types, None)` falls through to the numeric `DOUBLE PRECISION` default; `partial_emits_items` sizes its three `EMITS` columns from the same default; and `partial_select_items` renders `COUNT("")`, `SUM("")`, and `SUM("" * "")`, which DataFusion rejects. **TASK 1.2 CAPTURE PENDING** — whether Exasol pushes this argument shape at all, and therefore whether any query ever reached that rejection, is recorded here from a live Docker-container capture (plan `refactor-pushdown-agg-dedup`, task 1.2) before this delta is recorded. CLAUDE.md § Verification discipline forbids settling it from the three-function inference chain above, so this bullet MUST NOT be recorded while the pending marker still stands.
* The capability advertisement leaves the argument shape unconstrained, which is why the decline is warranted whichever way the capture falls: `capabilities.rs:176-181` advertises `FN_AGG_STDDEV`, `FN_AGG_STDDEV_POP`, `FN_AGG_STDDEV_SAMP`, `FN_AGG_VARIANCE`, `FN_AGG_VAR_POP`, and `FN_AGG_VAR_SAMP` alongside the scalar-function capabilities, and nothing between the advertisement and the scan tests the argument's node type. If the capture shows an error, the decline turns a failing query into a correct result through the qualified single-table wrapper. If it shows the query already succeeds, Exasol never pushed the shape, no user-visible behavior changes, and the decline is a structural guard that keeps this unenforced limit distinguishable from a regression.
* Declining is the correct fix rather than a better error message, and the alternative was considered and rejected. Routing the scan's stat branch through `agg_arg_sql` (as `datafusion-scan/scan-partial-agg-column-contract` does for its own reasons) replaces `column "" not found` with `__MISSING_AGG_ARGUMENT__ not found` — a clearer failure, still a failure. Declining at detection returns the right rows.
* Extending the family to expression arguments is explicitly NOT this delta's scope. The scan's sufficient statistics would have to render `SUM(<expr> * <expr>)` over a fragment evaluated twice, and the partial `EMITS` types would have to come from the declared result type rather than a source column — both are new behavior with their own correctness story, not a limit made explicit.
* Iceberg spec compliance: checked, not engaged. This delta changes one argument-shape test in the adapter's aggregate detection. It touches no manifest read, snapshot resolution, field-id projection, delete-file application, or type-mapping surface, so no normative requirement of the Apache Iceberg table spec applies and there is no deviation to fix or track.
<!-- /DELTA:NEW -->

## Scenarios

<!-- DELTA:NEW -->
### Scenario: Statistical aggregate over an expression argument declines the partial/merge pushdown

* *GIVEN* a `pushdown` request whose select list contains a statistical aggregate — `STDDEV`, `STDDEV_POP`, `STDDEV_SAMP`, `VARIANCE`, `VAR_POP`, or `VAR_SAMP` — whose single argument is NOT a bare `column` node (for example `STDDEV(a + b)` or `STDDEV(LENGTH(name))`), with or without a GROUP BY clause
* *WHEN* Exasol sends the `pushdown` request
* *THEN* the adapter SHALL decline the aggregate pushdown for that item, returning no `AggregatePlan` for it, so the request routes to row scanning or to the qualified single-table wrapper and Exasol computes the statistic natively over the returned rows
* *AND* the adapter MUST NOT emit an `AggregatePlan` carrying neither a `column` nor an `arg_expr`, because such a plan passes `validate_agg_col_types` on the `DOUBLE PRECISION` default and then renders `COUNT("")`, `SUM("")`, and `SUM("" * "")` in the scan, failing the whole query where a decline returns the correct result
* *AND* a statistical aggregate over a BARE COLUMN SHALL continue to push down unchanged through the sufficient-statistics decomposition, with a byte-identical scan spec, `EMITS` clause, and merge expression
* *AND* the decline SHALL apply at ALL FIVE `parse_agg_item` call sites, not only the two detection entry points, so no path can emit or consume the malformed plan: `detect_aggregates` (`single_group_agg.rs:78`) SHALL decline the whole single-group select list, routing the request to the row scan; `detect_group_by_aggregates` (`grouped_agg.rs:215`) SHALL decline the whole grouped detection, routing it to the qualified single-table wrapper; `classify_scalar_over_aggregate` (`grouped_agg.rs:399`) SHALL decline a select item that WRAPS such an aggregate (for example `SQRT(STDDEV(a + b))` over a GROUP BY), so the grouped detection declines to that same wrapper instead of sizing three `EMITS` columns from the `DOUBLE PRECISION` default; and `render_scalar_over_merge` (`grouped_agg.rs:427`) and `render_having_over_merge` (`grouped_agg.rs:988`) SHALL return `None`, routing a HAVING or a merge ORDER BY over such an aggregate to the qualified wrapper — which is the outcome those two already produce through their `AggregatePlan` lookup against the detected plans, so their observable behavior is UNCHANGED and only the decline's reason moves to parse time
* *AND* the statistical family SHALL keep its bare-column-only argument rule; the adapter MUST NOT resolve the argument through `arg_column_or_expr` and MUST NOT populate `arg_expr` for a statistical aggregate, because rendering `SUM(<expr> * <expr>)` over a doubly-evaluated fragment and sizing the partial `EMITS` columns without a source column are new behavior outside this delta's scope
<!-- /DELTA:NEW -->
