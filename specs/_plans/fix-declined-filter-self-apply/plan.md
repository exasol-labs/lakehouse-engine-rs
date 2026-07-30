# Plan: fix-declined-filter-self-apply

`Closes #279`

## Summary

Make the adapter apply, in its own returned SQL, any WHERE predicate it cannot push into the
DataFusion scan — at all three render sites — because Exasol re-applies nothing it delegated. Each
site today conflates "no filter" with "filter declined" into one `None` and omits the predicate,
returning extra unfiltered rows.

## Design

### Context

Exasol's Virtual Schema pushdown protocol is all-or-nothing per capability. The pushdown response
carries exactly two fields, `type` and `sql`; there is no residual field, no partial-pushdown
acknowledgment, and no per-query capability qualification. Exasol splits the query from the
capabilities response ALONE, before the pushdown request exists, and post-processes only what the
adapter did not advertise. Once a predicate arrives in the request, the adapter's returned SQL is
the only place it can be evaluated.

The codebase assumed the opposite. Three sites render the DataFusion-bound WHERE filter, each
returning `Option<String>`, and each reads `None` as "safe to omit — Exasol keeps it":

| Site | Symbol | What it omits on decline |
|---|---|---|
| Single-table WHERE | `handle_pushdown` (`pushdown/mod.rs`) | the whole scan-spec `filter` |
| Broadcast join | `render_broadcast_join` (`joins/sql_builders.rs`) | the whole scan-spec `filter`; the broadcast SQL has no outer `WHERE` at all |
| N-scan per-leg | `build_side_fan_out_sql` (`joins/sql_builders.rs`) | one side-local conjunct, which the outer wrapper's residual set excludes by construction |

`None` collapses three outcomes: no filter in the request (omit is correct), a filter rendering
trivially `TRUE`/`NULL` (omit is correct), and a filter present and non-trivial but unrenderable
(omit is WRONG). That collapse is why the defect survived review.

Verified live against the Docker Exasol stack (`exasol/docker-db:2025.2.1`), three shapes, all
returning all 12 rows of `TYPED_DISTINCT_PROBE` instead of the correct subset, with `EXPLAIN
VIRTUAL` confirming the emitted SQL carries no `"filter"` in the scan spec and no `WHERE` anywhere:

| Query | Correct | Actual | Decline source |
|---|---|---|---|
| `WHERE SECOND(c_ts, 3) > 1` | 0 rows | 12 rows | DataFusion-dialect arity refusal (`FN_SECOND` advertised) |
| `WHERE c_decimal_a LIKE '1%'` | 3 rows | 12 rows | `like_subject_type_guard` DECIMAL decline |
| `WHERE INSTR(c_varchar,'a',2) = 0` | 7 rows | 12 rows | `string_function_arg_type_guard` >2-arg decline |

The control case `WHERE SECOND(c_ts) > 1` (1 argument) emits `"filter":"(1 < date_part('SECOND',
…))"` and returns 0 rows correctly, so the difference is the decline, not the query shape.

- **Goals** — one guarantee at all three sites: a predicate the adapter accepted is always
  evaluated. Each site distinguishes absent from declined. The declined predicate is rendered by the
  adapter in Exasol dialect, positioned so it restricts the rows every other clause consumes. The
  correcting fact is recorded in `CLAUDE.md` and every spec and doc comment asserting the disproven
  backstop is corrected, so the library cannot re-seed the defect.
- **Non-Goals** — no new abstraction, module, or translator entry point; `crates/vs-expression` code
  is unchanged (doc comments only). No change to the wrapper-free fast path for any request whose
  filter renders. No wiring of the type-rewrite guards into the join paths (issue #215). No
  adjudication or closure of issue #228 — the structural fix reaches its surface, which is noted,
  but that issue is re-verified on its own. No change to file pruning, select-list widening, HAVING
  routing, or ORDER BY/LIMIT self-rendering; each is a separate and already-correct mechanism.

### Decision

Reuse three mechanisms that already exist rather than build a fourth. No new module and no new
translator API.

```
                              ┌─ filter absent / trivially true ──▶ unchanged (omit, no wrapper)
single-table  handle_pushdown ┤
                              └─ filter DECLINED ──▶ qualified_single_table_fallback_pushdown
                                                     (already the destination of 3 other declines)
                                                       SELECT <exasol select list>
                                                       FROM (raw fan-out, filter=None) AS "LHS_T0"
                                                       WHERE <declined predicate, qualified>   ◀ NEW
                                                       [GROUP BY] [HAVING] [ORDER BY] [LIMIT]

broadcast  render_broadcast_join ── filter DECLINED ──▶ Ok(None) ──▶ N-scan fallback (below)

N-scan  render-site screen ── side-local AND df-renderable ──▶ that side's fan-out leg
                          └─ everything else ──────────────▶ outer wrapper WHERE (existing clause)
        (the pruning-facing partition input is NOT screened — it keeps every side-local conjunct)
```

**Why the qualified wrapper, not an outer `WHERE` around the emitted SQL.** The single-table path
serves five request shapes (row scan, top-N, single-group aggregate, grouped aggregate,
`COUNT(DISTINCT)`). Wrapping the emitted SQL would filter AFTER aggregation and AFTER truncation —
wrong for four of the five. The qualified wrapper is the one shape that positions the `WHERE`
between the raw fan-out (aggregate-free, sort-free, LIMIT-free by construction) and every other
clause. It already exists, already builds the `LHS_T0` alias map, already renders the select list,
GROUP BY, HAVING, ORDER BY, and LIMIT in Exasol dialect, and is already the destination of three
other decline guards in `build_dispatch_sql`.

**Why broadcast declines instead of gaining a wrapper.** `pushdown-planning-join`'s recorded
broadcast contract already reads "a condition/filter/projection the `crates/vs-expression`
translator can render; any deviation is served by the unified unaccelerated fallback". The code
never enforced the filter half. This is a bug against the recorded spec, not an open design fork.
The alternative also collapses on inspection: the broadcast projection is narrowed to select-list
items, so a filter-only column is not in scope for an outer `WHERE`, and widening the projection to
put it in scope trips the existing widened-projection decline anyway.

**Why the N-scan screen sits at the render sites, not inside the partition.** `side_local_filter` has
two consumers: `plan_join` passes its result to Iceberg manifest pruning, and `build_n_scan_join_sql`
passes it to a leg's DataFusion filter. Only the second can decline. Screening inside the function
would strip a declined conjunct from pruning as well — more files opened, correct rows, no failing
test. So the partition stays purely structural (attribution by `tableName` alone) and ONE
renderability screen, `renderable_only` / `declined_only` as exact complements, is applied at the two
render call sites in `build_n_scan_join_sql`: the leg keeps the conjuncts it can apply, the outer
wrapper's `WHERE` carries the rest. The emitted SQL changes only for a conjunct that today vanishes,
so no golden-SQL fixture for a rendering filter changes.

**Why no new translator API.** Declined and trivially-true are already distinguishable:
`render_expression_safe` does not suppress a trivially-true result, so it returns `None` for exactly
the declined case. One three-line `pub(super)` predicate in `pushdown/support.rs` wraps it, shared by
all three sites. The trivially-true rule stays owned by `crates/vs-expression` and is never
re-tested against the literal strings `TRUE`/`NULL` at a call site.

**Why the both-dialects-unrenderable case errors.** A predicate no dialect renders can be applied
nowhere. Returning rows without it is wrong; erroring is correct. This adds a route to an existing
outcome, not a new failure mode — `pushdown-planning-selectlist-expressions` already records the
same terminal for a select-list node untranslatable under both dialects, reaching the same wrapper
refusal site. The error is decided by the NON-suppressing `render_expression_qualified`, never by
`render_df_filter_qualified` alone: that renderer suppresses a trivially-true result to `None`
exactly as its DataFusion twin does, so reading its `None` as "unrenderable" would re-create this
plan's own root-cause conflation one dialect over and hard-fail a correct no-op predicate.

#### Patterns

| Pattern | Where | Why |
|---|---|---|
| Distinguish absent from declined at the call site | all three render sites | the two require opposite handling and today share one `None` |
| Route a decline to an existing fallback | `build_dispatch_sql`, `render_broadcast_join` | both fallbacks already apply clauses in Exasol dialect; a fourth and a fifth route cost nothing |
| One renderability screen, applied only where rendering happens | `renderable_only` / `declined_only` at `build_n_scan_join_sql`'s two render sites | the pruning-facing partition stays unscreened, and the two screened halves are exact complements by construction |
| Three outcomes where a renderer returns two | both wrappers' residual render | `Some`, trivially-true, and unrenderable need three different actions; only the third errors |
| Correctness over availability at the terminal | both wrappers | a predicate applicable nowhere must not return rows |

### Consequences

| Decision | Alternatives Considered | Rationale |
|---|---|---|
| Self-apply the declined predicate in the adapter's own Exasol SQL | Fail the whole query on any decline — rejected, loses availability for every shape the adapter CAN apply. Use a native partial-pushdown acknowledgment — ruled out: `PushDownResponse` carries one field, `sql`; no residual mechanism exists in the protocol | the protocol gives the adapter no way to hand a predicate back, so the adapter owns it |
| Single-table decline routes to the qualified single-table wrapper | Wrap the emitted SQL in `SELECT * FROM (…) WHERE …` — rejected, filters after aggregation and after truncation, wrong for four of five request shapes | one shape is correct for all five, and it already exists |
| Broadcast decline falls through to N-scan | Give broadcast its own outer wrapper — rejected: contradicts the recorded broadcast contract, needs a wrapper that does not exist, and requires a projection widening that trips the existing widened decline | the recorded spec already prescribes the fall-through |
| Declined side-local conjuncts become residual, screened at the two render sites | Add the condition inside `side_local_filter` / `cross_side_residual_filter` — rejected, `side_local_filter` also feeds Iceberg manifest pruning, which must keep every side-local conjunct. Render the FULL filter into the outer `WHERE` unconditionally — rejected, double-applies every side-local conjunct and rewrites the emitted SQL for every join query | surgical; only a conjunct that today vanishes changes position, and pruning input is untouched |
| The residual render errors only on the non-suppressing render's `None` | Error whenever `render_df_filter_qualified` returns `None` — rejected, that renderer also returns `None` for a trivially-true residual, so a correct no-op predicate would hard-fail | the suppressing renderer cannot distinguish trivially-true from unrenderable; the non-suppressing one can |
| No new `vs-expression` entry point | Add a three-way `FilterRender` outcome enum — rejected as unnecessary: `render_expression_safe` already isolates the declined case | fewer public entry points; the trivially-true rule keeps one owner |
| Both-dialects-unrenderable returns an error | Omit and return rows — rejected, that is the defect | correctness first; a new route to an existing outcome |

## Features

| Feature | Status | Spec |
|---|---|---|
| pushdown-declined-filter-self-apply | NEW | `vs-adapter/pushdown-declined-filter-self-apply/spec.md` |
| pushdown-planning | CHANGED | `vs-adapter/pushdown-planning/spec.md` |
| pushdown-planning-like-type-coercion | CHANGED | `vs-adapter/pushdown-planning-like-type-coercion/spec.md` |
| pushdown-planning-string-fn-type-coercion | CHANGED | `vs-adapter/pushdown-planning-string-fn-type-coercion/spec.md` |
| pushdown-planning-aggregate-extensions | CHANGED | `vs-adapter/pushdown-planning-aggregate-extensions/spec.md` |
| pushdown-planning-join | CHANGED | `vs-adapter/pushdown-planning-join/spec.md` |
| pushdown-planning-join-fallback | CHANGED | `vs-adapter/pushdown-planning-join-fallback/spec.md` |
| pushdown-file-pruning | CHANGED | `vs-adapter/pushdown-file-pruning/spec.md` |
| vs-expression-translator | CHANGED | `sql-comprehension/vs-expression-translator/spec.md` |
| vs-expression-translator-cast | CHANGED | `sql-comprehension/vs-expression-translator-cast/spec.md` |
| vs-expression-translator-date-fns | CHANGED | `sql-comprehension/vs-expression-translator-date-fns/spec.md` |

Every CHANGED delta corrects recorded prose that asserts the disproven backstop. Six of them also
revise an existing scenario — `pushdown-planning-like-type-coercion`,
`pushdown-planning-string-fn-type-coercion`, `pushdown-planning-join`,
`pushdown-planning-join-fallback`, `pushdown-file-pruning`, and `vs-expression-translator-cast`; the
other four add a scenario pinning the corrected contract.

## Impact

A query whose WHERE predicate the adapter cannot push now returns the correct rows. It previously
returned every row the predicate would have excluded, silently, at all three pushdown sites.

Four behavior changes an operator will observe:

- **A predicate unrenderable under BOTH dialects now fails the query.** Today it returns wrong rows.
  Reachable via an advertised `FN_CAST` to `INTERVAL`, `GEOMETRY`, `HASHTYPE`, or `TIMESTAMP WITH
  LOCAL TIME ZONE`. This trades availability for correctness deliberately.
- **A declined single-table predicate loses the wrapper-free fast scan for that query**, gaining one
  `SELECT … FROM (…) AS "LHS_T0" WHERE …` boundary. Every request whose filter renders is
  byte-identical to today.
- **A broadcast-eligible join whose filter declines loses broadcast acceleration** and runs the
  N-scan fallback, which applies the predicate.
- **On the decline path the shard's UNFILTERED output crosses the UDF boundary** and Exasol filters
  it. That query's temp-DB RAM therefore scales with scanned rows, not result rows. This is the same
  resource profile as the three wrapper routes already in production (widened projection, declined
  select-list expression, declined `COUNT(DISTINCT)`), not a new class.

No interface, VS property, DDL, or wire-format change. No migration. Existing virtual schemas need
no redeployment beyond the new `.so`.

## Dependencies

None external. Unblocks issue #215 (`fix-join-perleg-like-type-guard`), whose decline arm rested on
the disproven backstop. Issue #228 shares the corrected seam but is neither verified nor closed here.

## Implementation Tasks

1. Add `pub(super) fn datafusion_renderable(expr: &Json) -> bool` to
   `crates/lakehouse-engine/src/adapter/pushdown/support.rs`, answering "can the DataFusion dialect
   express this predicate" via `render_expression_safe`, with a doc comment stating WHY it exists (a
   predicate it rejects must be self-applied, never omitted) and that a trivially-true predicate
   answers `true`. Add unit tests: a rendering predicate, a `SECOND(ts, 3)` arity decline, a
   trivially-true `TRUE` literal, and a test proving `strip_table_alias` does not change the answer
   (the N-scan leg renders a stripped tree the partition screened un-stripped). Also add
   `second_with_precision_declines_for_datafusion_renders_for_exasol` to `crates/vs-expression`'s own
   `mod tests`, pinning the dialect asymmetry every other test in this plan relies on: `SECOND(ts, 3)`
   returns `None` from `render_expression_safe` and `Some("SECOND(…, 3)")` from
   `render_expression_exasol_safe`. No `crates/vs-expression` production code changes.
2. Add the `CLAUDE.md` subsection stating the corrected protocol fact: once the adapter's
   capabilities response advertises a predicate or function shape, Exasol delegates it fully and
   never independently re-checks or re-applies it; there is no Exasol-side fallback once a capability
   is advertised, so the adapter owns generating the equivalent SQL itself for anything it cannot
   faithfully push to DataFusion. Place it as a new subsection near `## Exasol / tooling`. State it
   as a plain general fact — no discovery narrative, and NO issue number anywhere in the text.
3. In `render_broadcast_join` (`joins/sql_builders.rs`), distinguish an absent or trivially-true
   filter from a declined one: when `pushdown_req["filter"]` is present, non-null, and
   `!datafusion_renderable(filter)`, return `Ok(None)` so the caller falls through to the N-scan
   fallback; otherwise keep today's `and_then(render_df_filter_safe)` result. Update the function's
   doc comment to list the filter decline alongside the disjoint-schema, unrenderable-condition, and
   widened-projection declines. Add the unit test
   `broadcast_declines_on_unrenderable_filter_stays_eligible_when_absent` to `joins/sql_builders.rs`'s
   `mod tests` in the same unit of work, written failing first.
4. Reclassify declined side-local conjuncts as residual, screening the RENDER path only. [expert]
   Leave `side_local_filter` and `cross_side_residual_filter` UNCHANGED. `side_local_filter` has a
   second production consumer the screen must not reach: `plan_join`
   (`joins/mod.rs:122`) passes its result to `resolve_one_join_side` as that side's Iceberg
   manifest-pruning predicate, so a screen inside the function would drop declined conjuncts from
   pruning and silently open more files — correct rows, no failing test. Add instead to
   `joins/rendering.rs` two `pub(super)` screens over the existing `partition_conjuncts`, exact
   complements over one filter's top-level conjuncts and called ONLY from `build_n_scan_join_sql`:
   `renderable_only` (keep a conjunct when `datafusion_renderable`) and `declined_only` (keep it when
   not). In `build_n_scan_join_sql` (`joins/sql_builders.rs`) screen once —
   `let leg_eligible = where_filter.and_then(renderable_only);` — then at the per-leg site (`:419`)
   pass `side_local_filter(&leg_eligible, &side.table_name)`, and at the residual site (`:396`) build
   ONE combined residual tree by AND-ing `cross_side_residual_filter(&leg_eligible)` with
   `declined_only(where_filter)`: two provably disjoint conjunct sets, so the AND duplicates nothing
   and a small `rendering.rs` helper returns the present side when only one is. Render that combined
   tree with `render_df_filter_qualified` and distinguish THREE outcomes, not two: `Some(sql)` becomes
   the outer `WHERE`; `None` while the NON-suppressing `render_expression_qualified` over the same
   tree returns `Some` is a trivially-true residual, which emits NO outer `WHERE` and MUST NOT error;
   `None` from BOTH is unrenderable and returns `join_render_decline`, because the predicate can then
   be applied nowhere. Do NOT gate that error on `render_df_filter_qualified` alone — it suppresses a
   trivially-true render to `None` exactly as its DataFusion twin does, and a column-free conjunct is
   residual by construction, so gating on it would hard-fail a query that correctly emits no clause
   today. Rewrite `build_side_fan_out_sql`'s stale doc claim that the outer query "still applies the
   FULL `WHERE`" to state that it applies exactly the residual set and that `side_filter` arrives
   pre-screened as DataFusion-renderable, so the leg's own render cannot decline. The screen also
   falsifies both partition functions' own doc comments, so update them in this same unit of work even
   though their bodies do not change: `side_local_filter`'s doc MUST state that consumer (a) Iceberg
   manifest pruning receives the RAW filter and consumer (b) that side's fan-out `ScanSpec.filter`
   receives a pre-screened one, and that the function itself makes no renderability decision;
   `cross_side_residual_filter`'s doc MUST state that the complement it forms is over whatever tree it
   is given, and that on the render path the outer `WHERE` additionally carries the declined
   conjuncts, so the total partition is `renderable_only`/`declined_only` composed with these two.
   Add these unit tests
   in the same unit of work, each written failing first — in `joins/rendering.rs`:
   `declined_side_local_conjunct_partitions_to_residual` and its complement (a rendering side-local
   conjunct still reaches its leg), and `join_side_pruning_input_unchanged_when_df_render_declines`,
   asserting `side_local_filter` still returns the declined conjunct for `plan_join`'s pruning input
   while the screened leg filter omits it; in `joins/sql_builders.rs`:
   `trivially_true_residual_emits_no_outer_where_and_does_not_error`.
5. Self-apply a declined single-table filter through the qualified wrapper. [expert] Compute the
   decline EXACTLY ONCE, in `handle_pushdown` alongside the existing `filter` computation
   (`pushdown/mod.rs:190-194`, which already reads the same `filter_json_raw` and `col_types`): the
   filter is declined when it is present and non-null AND (`apply_type_rewrites` returned `None` OR
   `!datafusion_renderable(rewritten)`). Thread the result into `build_dispatch_sql` as ONE new
   parameter carrying the ORIGINAL (un-type-rewritten) filter tree on decline and nothing otherwise.
   `build_dispatch_sql` MUST NOT recompute renderability — one owner for the classification, as
   `crates/vs-expression` is the one owner of the trivially-true rule. `build_dispatch_sql` routes a
   declined request to `qualified_single_table_fallback_pushdown` ahead of the routing classifier, so
   one route serves the row-scan, top-N, single-group-aggregate, grouped-aggregate, and
   `COUNT(DISTINCT)` shapes alike. Give `build_qualified_single_table_fallback_sql` and
   `qualified_single_table_fallback_pushdown` (`joins/sql_builders.rs`) a declined-predicate
   parameter: render it through `render_df_filter_qualified` against the `LHS_T0` alias map the
   wrapper already builds and emit it as the wrapper's `WHERE` between the fan-out and `trailing`. Use
   the same three-outcome gate as task 4 — error only when the NON-suppressing
   `render_expression_qualified` ALSO returns `None`, never on `render_df_filter_qualified`'s `None`
   alone. The fan-out spec's `filter` MUST be `None` on the decline route, so the predicate is applied
   exactly once. Update both doc comments (the current text asserts no outer `WHERE` is ever needed).
   Leave `resolve_file_list`'s pruning input and the no-decline path untouched.

   The decline route MUST also fix the wrapper's inner projection for a `SELECT *` request.
   `qualified_single_table_fallback_pushdown` derives that projection from
   `referenced_column_projection`, which collects only the columns the rendered clauses NAME. For an
   absent, JSON-null, or empty-array `selectList` — a genuine `SELECT *`, a shape ONLY the new route
   can reach — the narrowed set is the filter's columns alone. `n_scan_join_select_items`'
   absent-`selectList` arm then enumerates that narrowed `cols_per_side` through
   `n_full_row_qualified_items`, returning one column where Exasol validates the base row
   positionally: `04000` "Expected number of columns is N but pushdown query has M", per
   `build_dispatch_sql`'s own comment at `pushdown/mod.rs:517-540`. Give
   `qualified_single_table_fallback_pushdown` a SECOND new parameter — a pre-computed projection
   override, `Option<(Vec<ProjectionItem>, Vec<String>)>` — and on the decline route pass the FULL
   base row: every `col_types` entry, in order, with its Exasol type (`build_dispatch_sql` already
   holds `col_types`). `None` keeps today's `referenced_column_projection` result for every other
   caller. The guard MUST be added at the new decline route and MUST NOT be folded into
   `referenced_column_projection`: that function is shared with the join wrapper's narrowing, and both
   `referenced_clause_values`' doc comment and the recorded
   `vs-adapter/pushdown-joins-module-structure` scenario "One clause walk feeds both wrapper
   column-narrowing routines" forbid it. Both `SELECT *` shapes take the override, and they diverge
   from each other today: absent or JSON-null `selectList` (`project_columns` → full base row,
   `projection_widened == false`, so the request stays on the bare scan path and never reaches this
   wrapper) and empty-array `selectList` (`project_columns` → the first column only, arity 1, while
   the N-scan wrapper's `referenced_side_columns` empty-narrowing fallback already emits its whole
   column set for the same shape). The empty-array arity is the one point not settled from code —
   task 7's `SELECT *` e2e case and its § Manual Testing row confirm the positional outcome against
   the live Docker Exasol container rather than assuming it.

   Exact call-site census — production AND test callers of BOTH
   `qualified_single_table_fallback_pushdown` and `build_qualified_single_table_fallback_sql`. The
   declined-predicate parameter lands on both symbols; the projection override lands on
   `qualified_single_table_fallback_pushdown` alone. Every site below passes `None` for each new
   parameter it receives:

   | Call site | Argument |
   |---|---|
   | `pushdown/mod.rs:239` (`handle_pushdown` → `build_dispatch_sql`, production) | the computed declined tree |
   | `pushdown/mod.rs:430` (`qualified_single_table_fallback_pushdown`, widened projection) | `None` |
   | `pushdown/mod.rs:500` (`qualified_single_table_fallback_pushdown`) | `None` |
   | `pushdown/mod.rs:543` (`qualified_single_table_fallback_pushdown`) | `None` |
   | `pushdown/dispatch_golden.rs:193` (`build_dispatch_sql`, test) | `None` |
   | `pushdown/mod.rs:1554` (`build_dispatch_sql`, test) | `None` |
   | `joins/sql_builders.rs:1997` (`build_qualified_single_table_fallback_sql`, test) | `None` |
   | `joins/sql_builders.rs:2269` (same, test) | `None` |
   | `joins/sql_builders.rs:2310` (same, test) | `None` |
   | `joins/sql_builders.rs:2478` (`qualified_single_table_fallback_pushdown`, test) | `None` |
   | `pushdown/grouped_agg.rs:3129` (same, test) | `None` |
   | `pushdown/support.rs:3017` (same, test) | `None` |

   Every hand-written mirror of the production filter pipeline MUST either reproduce the new decline
   branch or carry a doc comment stating it deliberately mirrors only the no-decline path: the
   `apply_type_rewrites` → `render_df_filter_safe` mirrors at `pushdown/mod.rs:871`, `:907`, `:941`,
   `:972`, `:1002`, `:1043`, and `plan_scan_sql`'s dispatch-decision mirror at
   `pushdown/topn.rs:495-505`. A mirror that silently stops mirroring production hides exactly the
   regression class this plan fixes.

   Add these unit tests in the same unit of work, each written failing first:
   `single_table_wrapper_renders_declined_predicate_in_exasol_dialect` (`joins/sql_builders.rs`);
   `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper` covering row scan, grouped
   aggregate, and top-N, `trivially_true_filter_omitted_without_wrapper`,
   `declined_filter_with_absent_select_list_projects_full_row` (asserting that a `SELECT *` request
   with a declined filter projects EVERY `col_types` column in order, not just the filter's columns),
   and `iceberg_pruning_input_unchanged_when_df_render_declines` (`pushdown/mod.rs`);
   `nested_like_decline_routes_to_wrapper_where`,
   `declined_like_on_integer_column_routes_to_wrapper_where`, and
   `declined_like_on_unresolvable_column_routes_to_wrapper_where` (`pushdown/support.rs`).
6. Add the two no-change golden fixtures against `dispatch_golden`, which assert across all three
   render sites at once: `filterless_request_emits_unchanged_sql_at_all_three_sites` and
   `rendering_filter_emits_unchanged_wrapper_free_scan`.
7. Add live e2e coverage for the single-table surface to
   `crates/lakehouse-engine/tests/e2e_capability_test.rs`, against `TYPED_DISTINCT_PROBE`: a
   `WHERE SECOND(c_ts, 3) > 1` row scan (0 rows, not 12), a `WHERE c_decimal_a LIKE '1%'` row scan
   (3 rows: ids 1, 5, 7), a `WHERE INSTR(c_varchar,'a',2) = 0` shape, an aggregate over a declined
   filter proving the `WHERE` precedes aggregation, an `ORDER BY … LIMIT` over a declined filter
   proving it precedes truncation, and — as
   `e2e_declined_filter_select_star_returns_full_row_shape` — `SELECT * FROM
   MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE SECOND(C_TS, 3) > 1`, asserting the wrapper returns the
   FULL base row (every column, in order) with no `04000` positional error and the correct 0 rows.
   Assert the emitted `EXPLAIN VIRTUAL` SQL carries the wrapper `WHERE`, not a scan-spec `"filter"`.
8. Add live e2e coverage for the broadcast surface to `crates/lakehouse-engine/tests/e2e_join_test.rs`:
   a two-table inner equi-join under the broadcast threshold with no postprocessing and a declined
   side-local conjunct (`SECOND(<ts col>, 3)`), asserting correct rows AND — via the existing
   `has_broadcast_join_block` / `has_n_scan_wrapper` helpers — that the plan is the N-scan wrapper,
   not a broadcast block.
9. Add live e2e coverage for the N-scan per-leg surface to
   `crates/lakehouse-engine/tests/e2e_join_test.rs`: an above-threshold or three-table join carrying
   both a rendering side-local conjunct and a declined side-local conjunct, asserting correct rows,
   that the declined conjunct appears in the outer wrapper `WHERE`, and that the rendering conjunct
   still appears in its leg's scan-spec filter.
10. Add live e2e coverage for the terminal case: a WHERE predicate carrying an advertised `FN_CAST`
    to a target refused in both dialects returns a clean adapter error naming the unrenderable
    predicate, with no rows and no credential leakage. If no such predicate proves reachable through
    Exasol's parser, record that finding in `verification-report.md` and cover the arm with a unit
    test on the wrapper's error path instead — do not silently drop the case.
11. Correct every code doc comment asserting the disproven backstop. Census to re-resolve by symbol,
    not by line number: `pushdown/mod.rs` module header ("correctness backstop: Exasol keeps the
    predicate at its own level"); `pushdown/support.rs` `like_subject_type_guard` ("`None`-means-omit
    contract, so Exasol evaluates the…") and `string_function_arg_type_guard`;
    `joins/rendering.rs` `render_df_filter_qualified` ("a dropped predicate is Exasol's own backstop
    responsibility exactly as elsewhere"); `joins/rendering.rs` `side_local_filter` and
    `cross_side_residual_filter`, verifying task 4 rewrote both to match the render-site screen (the
    stale text claims consumers (a) and (b) receive the same tree, and that the two slices are the
    total partition); `pushdown/support.rs` the rewrite-primitive doc whose "A
    decline propagates to the root" section reads "the whole filter is dropped so Exasol evaluates it
    natively"; `joins/sql_builders.rs` the `RenderedJoinPushdown::filter` field doc — task 3 changes
    that field's semantics, so its replacement text MUST state the post-change meaning: absent or
    trivially true only, never declined, because a declined filter forfeits the broadcast plan;
    `adapter/capabilities.rs` the `FN_CAST` comment claiming
    the adapter "falls back"; `crates/vs-expression/src/lib.rs` `render_df_filter_safe` and
    `render_df_filter_exasol_safe` ("lets Exasol keep it as a correctness backstop" — state instead
    that `None` has two distinguishable causes and what a `None` means belongs to the caller). Change
    no behavior in this task.
12. Run the gate: `make cross-musl-udf-build`, `cargo test`, `cargo clippy --all-targets`,
    `cargo fmt`, then `make test-e2e` against a manually started `docker compose` stack. Verify
    `git diff` shows no change to any golden-SQL fixture for a request whose filter renders.

## Parallelization

| Parallel Group | Tasks |
|---|---|
| Group A | 1, 2 |
| Group B | 3, then 4, then 5 |
| Group C | 6 |
| Group D | 7, 8, 9, 10 |
| Group E | 11 |
| Group F | 12 |

Sequential dependencies:

- Group A → Group B (tasks 3-5 all call `datafusion_renderable`)
- Group B internally sequential: tasks 3, 4, and 5 all edit
  `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs`
- Group B → Group C, Group B → Group D
- Group C, Group D → Group E (task 11 edits files tasks 3-5 touch; docs last avoids conflicts)
- Group E → Group F

## Dead Code Removal

| Type | Location | Reason |
|---|---|---|
| None | — | The fix adds one predicate and routes to existing paths; no symbol becomes unreachable. `render_df_filter_qualified` gains a second caller rather than losing its only one. |

## Verification

### Scenario Coverage

| Scenario | Test Type | Test Location | Test Name |
|---|---|---|---|
| A declined single-table WHERE filter is applied in the adapter's own outer WHERE | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_second_arity_returns_filtered_rows` |
| A declined single-table WHERE filter is applied in the adapter's own outer WHERE | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_like_on_decimal_returns_filtered_rows` |
| A declined filter is applied before aggregation, grouping, and truncation | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_under_aggregate_filters_before_aggregating` |
| A declined filter is applied before aggregation, grouping, and truncation | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_under_order_by_limit_filters_before_truncating` |
| A filter that renders keeps the wrapper-free fast path unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `rendering_filter_emits_unchanged_wrapper_free_scan` |
| A trivially-true filter is still omitted with no wrapper | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `trivially_true_filter_omitted_without_wrapper` |
| A broadcast-eligible join whose filter declines takes the N-scan fallback | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` |
| A broadcast-eligible join whose filter declines takes the N-scan fallback | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `broadcast_declines_on_unrenderable_filter_stays_eligible_when_absent` |
| An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where` |
| An N-scan side-local conjunct whose DataFusion render declines becomes a residual conjunct | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `declined_side_local_conjunct_partitions_to_residual` |
| A render decline leaves each side's Iceberg manifest-pruning predicate unchanged | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/rendering.rs` | `join_side_pruning_input_unchanged_when_df_render_declines` |
| A residual set that renders trivially true emits no outer WHERE and does not error | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `trivially_true_residual_emits_no_outer_where_and_does_not_error` |
| A predicate unrenderable under both dialects returns a clean error | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_both_dialects_unrenderable_predicate_errors_without_rows` |
| An absent filter is distinguished from a declined filter at every site | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `datafusion_renderable_separates_absent_declined_and_trivially_true` |
| An absent filter is distinguished from a declined filter at every site | Unit | `crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | `filterless_request_emits_unchanged_sql_at_all_three_sites` |
| LIKE on a DECIMAL column declines the whole filter (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_like_on_decimal_returns_filtered_rows` |
| LIKE on an integer column declines the whole filter (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `declined_like_on_integer_column_routes_to_wrapper_where` |
| LIKE on a bare column whose type cannot be resolved declines the whole filter (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `declined_like_on_unresolvable_column_routes_to_wrapper_where` |
| A nested non-string LIKE declines the entire enclosing filter (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/support.rs` | `nested_like_decline_routes_to_wrapper_where` |
| A non-coercible resolvable column type in a WHERE-clause string function declines the whole filter (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_instr_three_arg_returns_filtered_rows` |
| Broadcast join projection and filter are rendered per involved table (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_broadcast_declined_filter_falls_back_to_n_scan_and_filters` |
| Join conditions attach greedily by table-name set and side-local filters push into each leg (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_join_test.rs` | `e2e_n_scan_declined_side_local_conjunct_applied_in_outer_where` |
| Untranslatable conjunct disables pruning for that conjunct only (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `iceberg_pruning_input_unchanged_when_df_render_declines` |
| Filter predicate is pushed into the scan spec (CHANGED) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper` |
| CAST renders the mapped target type per dialect (CHANGED) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_both_dialects_unrenderable_predicate_errors_without_rows` |
| A declined WHERE filter routes the single-table request to the qualified wrapper (NEW) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_filter_routes_every_dispatch_shape_to_qualified_wrapper` |
| A declined WHERE filter routes the single-table request to the qualified wrapper (NEW) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/mod.rs` | `declined_filter_with_absent_select_list_projects_full_row` |
| A declined WHERE filter routes the single-table request to the qualified wrapper (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_select_star_returns_full_row_shape` |
| A declined WHERE filter under an aggregate request is applied ahead of the aggregate (NEW) | Integration | `crates/lakehouse-engine/tests/e2e_capability_test.rs` | `e2e_declined_filter_under_aggregate_filters_before_aggregating` |
| A declined single-table WHERE predicate is an Exasol-dialect wrapper position (NEW) | Unit | `crates/lakehouse-engine/src/adapter/pushdown/joins/sql_builders.rs` | `single_table_wrapper_renders_declined_predicate_in_exasol_dialect` |
| A refused argument count declines for DataFusion and renders for Exasol (NEW) | Unit | `crates/vs-expression/src/lib.rs` | `second_with_precision_declines_for_datafusion_renders_for_exasol` |

### Manual Testing

| Feature | Command | Expected Output |
|---|---|---|
| pushdown-declined-filter-self-apply (single-table) | `exapump sql --profile docker "SELECT COUNT(*) FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE SECOND(C_TS, 3) > 1"` | `0` — was `12` before the fix |
| pushdown-declined-filter-self-apply (single-table) | `exapump sql --profile docker "SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE C_DECIMAL_A LIKE '1%' ORDER BY ID"` | ids `1, 5, 7` — was all 12 ids |
| pushdown-declined-filter-self-apply (emitted SQL) | `exapump sql --profile docker "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE SECOND(C_TS, 3) > 1"` | `PUSHDOWN_SQL` contains `AS "LHS_T0" WHERE` and no `"filter"` inside the scan spec |
| pushdown-declined-filter-self-apply (`SELECT *` column shape) | `exapump sql --profile docker "SELECT * FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE SECOND(C_TS, 3) > 1"` | 0 rows and the FULL base-row column shape (every column, in order) — no `04000` "Expected number of columns" error and no truncated single-column result; confirms Exasol's positional validation against the live Docker container, including the empty-`selectList` arity |
| pushdown-planning (fast path unchanged) | `exapump sql --profile docker "EXPLAIN VIRTUAL SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE SECOND(C_TS) > 1"` | `PUSHDOWN_SQL` contains `"filter":"(1 < date_part('SECOND'` and no `LHS_T0` wrapper |
| pushdown-planning-join (broadcast decline) | `exapump sql --profile docker "EXPLAIN VIRTUAL SELECT … FROM <fact> f JOIN <dim> d ON … WHERE SECOND(f.C_TS, 3) > 1"` | `PUSHDOWN_SQL` shows the N-scan wrapper (`"LHS_T0"` + `"LHS_T1"`), no `join` block in the common spec |
| pushdown-planning-join-fallback (residual routing) | `exapump sql --profile docker "EXPLAIN VIRTUAL SELECT … FROM a JOIN b JOIN c … WHERE a.C_VARCHAR = 'x' AND SECOND(a.C_TS, 3) > 1"` | outer `WHERE` carries the `SECOND(…, 3)` conjunct; leg `a`'s scan-spec `"filter"` carries only the `C_VARCHAR` conjunct |
| vs-expression-translator-cast (terminal error) | `exapump sql --profile docker "SELECT ID FROM MY_LAKEHOUSE.TYPED_DISTINCT_PROBE WHERE CAST(C_VARCHAR AS HASHTYPE) = CAST('x' AS HASHTYPE)"` | a clean adapter error naming the unrenderable predicate; no rows, no credentials in the message |
| pushdown-declined-filter-self-apply (decline-path memory cost) | run both `SELECT ID … WHERE SECOND(C_TS, 3) > 1` (declined) and `SELECT ID … WHERE SECOND(C_TS) > 1` (renders), then `FLUSH STATISTICS` and read `TEMP_DB_RAM_PEAK` per statement from `EXA_DBA_AUDIT_SQL` | the declined statement's peak scales with SCANNED rows, the rendering one with RESULT rows; record both figures in `verification-report.md` |
| CLAUDE.md fact recorded | `grep -n "never independently re-checks" /home/crusty/code/lakehouse-engine-rs/CLAUDE.md` | one match; `grep -c '#' ` over that subsection shows no issue number |

### Checklist

| Step | Command | Expected |
|---|---|---|
| Build | `make cross-musl-udf-build` | Exit 0 |
| Test | `cargo test` | 0 failures |
| Test (e2e) | `make test-e2e` (requires a manually started `docker compose` stack) | 0 failures, 0 skips |
| Lint | `cargo clippy --all-targets` | 0 errors/warnings |
| Format | `cargo fmt` | No changes |
| Golden-SQL stability | `git diff -- crates/lakehouse-engine/src/adapter/pushdown/dispatch_golden.rs` | no change to any fixture for a request whose filter renders |
